from argparse import ArgumentParser
import json
import pickle
import blockutil

BLOCK_0 = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"


def write_blocks_to_file(prefix, start_block, no_blocks):

    next_block = start_block
    counter = 0
    out = None
    filesize = 1024 * 1024 * 32
    while True:
        if not out or (out and (out.tell() > filesize)):
            if out:
                out.close()
            filename = "%s_%s.bin" % (prefix, counter)
            print("Writing to file " + filename)
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
    parser = ArgumentParser()
    parser.add_argument("-p", "--prefix", dest="prefix",
                        help="prefix of exported files containing blocks")
    parser.add_argument("-b", "--bitcoin", dest="bitcoin",
                        default="localhost", metavar="BITCOIN_HOST",
                        help="host running bitcoin REST interface")
    parser.add_argument("-s", "--startblock", dest="startblock",
                        default=BLOCK_0,
                        help="hash of first block to export")
    parser.add_argument("-n", "--numblocks", dest="numblocks",
                        type=int, default=0,
                        help="number of blocks to write " +
                             "(default value 0 exports everything)")

    args = parser.parse_args()
    if args.prefix is None:
        parser.error("Filename not given.")
    blockutil.set_blockchain_api("http://%s:8332/rest/block/" % args.bitcoin)
    write_blocks_to_file(args.prefix, args.startblock, args.numblocks)


if __name__ == "__main__":
    main()
