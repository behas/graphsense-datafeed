from argparse import ArgumentParser
import json
import pickle
import blockutil
from pathlib import Path

BLOCK_0 = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"


def write_blocks_to_file(directory, file_prefix, start_block, no_blocks):

    next_block = start_block
    counter = 0
    out = None
    filesize = 1024 * 1024 * 32
    while True:
        if not out or (out and (out.tell() > filesize)):
            if out:
                out.close()
            filename = Path(directory, "%s_%s.bin" % (file_prefix, counter))
            print("Writing to file " + str(filename))
            out = open(filename, "wb")
        block_json = json.loads(blockutil.fetch_block_text(next_block))

        next_block, block, txs = blockutil.transform_json(block_json)
        block.append(txs)
        pickle.dump(block, out, -1)

        print("Wrote block %s" % counter, end="\r")
        if not next_block:
            break
        if no_blocks != 0:
            if counter >= no_blocks:
                break
        counter += 1
    out.close()


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
