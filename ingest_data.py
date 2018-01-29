import os
import pickle
import time
from argparse import ArgumentParser
from multiprocessing import Pool, Value
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement


def split_list(alist, wanted_parts=1):
    length = len(alist)
    return [alist[i * length // wanted_parts: (i + 1) * length // wanted_parts]
            for i in range(wanted_parts)]


class QueryManager(object):
    # chosen to match the default in execute_concurrent_with_args
    concurrency = 100
    counter = Value("d", 0)

    def __init__(self, cluster, keyspace, process_count=1):
        self.processes = process_count
        self.pool = Pool(processes=process_count,
                         initializer=self._setup,
                         initargs=(cluster, keyspace))

    @classmethod
    def _setup(cls, cluster, keyspace):
        cls.cluster = Cluster([cluster])
        cls.session = cls.cluster.connect()
        cls.session.default_timeout = 60
        cls.session.set_keyspace(keyspace)
        cql_stmt = """INSERT INTO block
                      (height, block_hash, timestamp, block_version, size, txs)
                      VALUES (?, ?, ?, ?, ?, ?);"""
        cls.insert_block_stmt = cls.session.prepare(cql_stmt)

        cql_stmt = """INSERT INTO transaction
                      (block_group, tx_number, tx_hash,
                       height, timestamp, size, coinbase, vin, vout)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);"""
        cls.insert_transaction_stmt = cls.session.prepare(cql_stmt)

    def insert(self, files):
        params = list(files)
        self.pool.map(_multiprocess_insert, split_list(params, self.processes))

    @classmethod
    def insertBlocks(cls, params):
        print("ingesting files", params)
        for filename in params:
            pickle_input = open(filename, "rb")

            while True:
                try:
                    block = pickle.load(pickle_input)
                except EOFError:
                    break
                if (cls.counter.value % 1000) == 0:
                    print("Read block %d" % (cls.counter.value), end="\r")
                with cls.counter.get_lock():
                    cls.counter.value += 1

                transactions = block[6]
                block.pop(6)

                batchStmt = BatchStatement()
                batchStmt.add(cls.insert_block_stmt, block)
                block_group = block[0] // 10000
                tx_number = 0
                for transaction in transactions:
                    batchStmt.add(cls.insert_transaction_stmt,
                                  [block_group, tx_number] + transaction)
                    tx_number += 1

                while True:
                    try:
                        cls.session.execute(batchStmt)
                    except Exception as err:
                        print("Exception ", err, " retrying...", end="\r")
                        continue
                    break


def _multiprocess_insert(params):
    return QueryManager.insertBlocks(params)


def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--cassandra", dest="cassandra",
                        help="cassandra node",
                        default="localhost")
    parser.add_argument("-k", "--keyspace", dest="keyspace",
                        help="keyspace to import data to",
                        default="graphsense_raw")
    parser.add_argument("-d", "--dir", dest="directory",
                        help="source directory for raw json bitcoin dump")
    parser.add_argument("-p", "--processes", dest="processes",
                        type=int, default=1,
                        help="number of processes")

    args = parser.parse_args()
    if args.directory is None:
        parser.error("Directory not given.")

    files = [os.path.join(args.directory, f)
             for f in os.listdir(args.directory)
             if os.path.isfile(os.path.join(args.directory, f))]

    qm = QueryManager(args.cassandra, args.keyspace, args.processes)
    start = time.time()
    qm.insert(files)
    delta = time.time() - start
    print("\n%.1fs" % delta)


if __name__ == "__main__":
    main()
