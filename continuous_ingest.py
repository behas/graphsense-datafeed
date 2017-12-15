#!/usr/bin/env python3
import argparse
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import blockutil

LOG_LEVEL = logging.INFO  # Could be e.g. "DEBUG" or "WARNING"
BLOCK_0 = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"


# class to capture stdout and sterr in the log
class MyLogger(object):
    def __init__(self, logger, level):
        """Needs a logger and a logger level."""
        self.logger = logger
        self.level = level

    def write(self, message):
        # only log if there is a message (not just a new line)
        if message.rstrip() != "":
            self.logger.log(self.level, message.rstrip())


class FakeRS(object):
    def __init__(self, block_hash, height):
        self.block_hash = block_hash
        self.height = height


class BlockchainIngest:

    def __init__(self, session):
        self.__session = session
        cql_stmt = """INSERT INTO block
                      (height, block_hash, timestamp, block_version, size, txs)
                      VALUES (?, ?, ?, ?, ?, ?);"""
        self.__insert_block_stmt = session.prepare(cql_stmt)
        cql_stmt = """INSERT INTO transaction
                      (block_group, tx_number, tx_hash, height,
                       timestamp, coinbase, vin, vout)
                      VALUES (?, ?, ?, ?, ?, ?, ?, ?);"""
        self.__insert_transaction_stmt = session.prepare(cql_stmt)

    def write_next_blocks(self, start_block):
        next_block = blockutil.hash_str(start_block)
        while next_block:
            block_json = blockutil.fetch_block_json(next_block)
            if "nextblockhash" in block_json.keys():
                next_block, block, txs = blockutil.transform_json(block_json)
                batchStmt = BatchStatement()
                batchStmt.add(self.__insert_block_stmt, block)
                block_group = block[0] // 10000
                tx_number = 0
                for transaction in txs:
                    batchStmt.add(self.__insert_transaction_stmt,
                                  [block_group, tx_number] + transaction)
                    tx_number += 1
                while True:
                    try:
                        self.__session.execute(batchStmt)
                    except Exception as err:
                        print("Exception ", err, " retrying...", end="\r")
                        continue
                    break
                print("Wrote block %d" % (block[0]), end="\r")

    def get_last_block(self, keyspace):
        select_stmt = "SELECT height, block_hash FROM " + keyspace + \
                      ".block WHERE height = ?;"
        block_max = 0
        block_inc = 100000
        last_rs = None
        rs = None
        while True:
            last_rs = rs
            rs = self.__session.execute(self.__session.prepare(select_stmt),
                                        [block_max])
            if not rs:
                if block_max == 0:
                    return [FakeRS(bytearray.fromhex(BLOCK_0), 0)]
                if block_inc == 1:
                    return last_rs
                else:
                    block_max -= block_inc
                    block_inc //= 10
            else:
                block_max += block_inc


def main():
    parser = argparse.ArgumentParser(description="Bitcoin ingest service")
    parser.add_argument("-b", "--bitcoin", dest="bitcoin",
                        default="localhost", metavar="BITCOIN_HOST",
                        help="address or name of bitcoin REST interface")
    parser.add_argument("-c", "--cassandra", dest="cassandra",
                        default="localhost", metavar="CASSANDRA_NODE",
                        help="address or name of cassandra database")
    parser.add_argument("-k", "--keyspace", dest="keyspace",
                        help="keyspace to import data to",
                        default="graphsense_raw")
    parser.add_argument("-s", "--sleep", dest="sleep",
                        type=int, default=600,
                        help="numbers of seconds to sleep " +
                             "before checking for new blocks.")
    parser.add_argument("-l", "--log", dest="log",
                        help="Location of log file")
    args = parser.parse_args()

    if args.log:
        logger = logging.getLogger(__name__)
        logger.setLevel(LOG_LEVEL)
        # handler that writes to a file, creates a new file at midnight
        # and keeps 3 backups
        handler = TimedRotatingFileHandler(args.log, when="midnight",
                                           backupCount=3)
        log_fmt = "%(asctime)s %(levelname)-8s %(message)s"
        formatter = logging.Formatter(log_fmt)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        # log stdout to file at INFO level
        sys.stdout = MyLogger(logger, logging.INFO)
        # log stderr to file at ERROR level
        sys.stderr = MyLogger(logger, logging.ERROR)

    cluster = Cluster([args.cassandra])
    session = cluster.connect()
    session.default_timeout = 60
    session.set_keyspace(args.keyspace)
    bc_ingest = BlockchainIngest(session)

    blockutil.set_blockchain_api("http://%s:8332/rest/block/" % args.bitcoin)

    while True:
        last_rs = bc_ingest.get_last_block(args.keyspace)
        if last_rs:
            hash_val = last_rs[0].block_hash
            print("Found last block:")
            print("\tHeight:\t%d" % last_rs[0].height)
            print("\tHash:\t%s" % blockutil.hash_str(hash_val))
            if hash_val:
                bc_ingest.write_next_blocks(hash_val)
        else:
            print("Could not get last block. Exiting...")
            break
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()
