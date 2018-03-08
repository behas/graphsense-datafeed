from argparse import ArgumentParser
import collections
import json
import os
import pickle
import blockutil

BLOCK_0 = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"


def write_blocks_to_file(directory, file_prefix, start_block, num_blocks):

    next_block = start_block
    filesize = 1024 * 1024 * 32
    counter = 0
    out_file = None
    tx_type_counter = collections.Counter()

    while True:
        block_json = json.loads(blockutil.fetch_block_text(next_block))
        next_block, block, txs = blockutil.transform_json(block_json,
                                                          tx_type_counter)
        height = block[0]
        block_hash = blockutil.hash_str(block[1])

        if not out_file or (out_file and (out_file.tell() > filesize)):
            if out_file:
                out_file.close()
            filename = "{:s}_{:09d}_{:s}.bin".format(file_prefix,
                                                     height,
                                                     block_hash)
            filename = os.path.join(directory, filename)
            print("Writing to file " + filename)
            out_file = open(filename, "wb")

        block.append(txs)
        pickle.dump(block, out_file, -1)
        print("Wrote block %s (%s)" % (height, block_hash), end="\r")

        if not next_block:
            break
        if num_blocks != 0:
            if counter >= num_blocks:
                break
        counter += 1

    out_file.close()
    print(tx_type_counter)


def main():
    parser = ArgumentParser(add_help=False)
    parser.add_argument('--help', action='help',
                        help='show this help message and exit')
    parser.add_argument("-d", "--directory", dest="directory", required=True,
                        help="directory containing exported block files")
    parser.add_argument("-f", "--filename", dest="file_prefix",
                        default="blocks",
                        help="file prefix of exported block files")
    parser.add_argument("-h", "--host", dest="host", required=True,
                        default="localhost", metavar="RPC_HOST",
                        help="host running bitcoin RPC interface")
    parser.add_argument("-p", "--port", dest="port",
                        type=int, default=8332,
                        help="port number of RPC interface")
    parser.add_argument("-s", "--startblock", dest="startblock",
                        default=BLOCK_0,
                        help="hash of first block to export")
    parser.add_argument("-n", "--numblocks", dest="numblocks",
                        type=int, default=0,
                        help="number of blocks to write " +
                             "(default value 0 exports all blocks)")

    args = parser.parse_args()

    blockutil.set_blockchain_api("http://%s:%d/rest/block/" %
                                 (args.host, args.port))
    write_blocks_to_file(args.directory, args.file_prefix,
                         args.startblock, args.numblocks)


if __name__ == "__main__":
    main()
