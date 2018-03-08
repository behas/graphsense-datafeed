import requests
import sys


BLOCKCHAIN_API = ''


def hash_str(bytebuffer):
    return "".join(("%02x" % a) for a in bytebuffer)


def transform_json(raw_block, tx_type_counter):
    transaction_ids = []
    transactions = []
    for raw_tx in raw_block["tx"]:
        tx = []
        tx.append(bytearray.fromhex(raw_tx["txid"]))
        tx.append(raw_block["height"])
        tx.append(raw_block["time"])
        tx.append(raw_block["size"])

        coinbase = False
        vins = []
        for raw_vin in raw_tx["vin"]:
            if "coinbase" in raw_vin.keys():
                coinbase = True
            else:
                vin = []
                if "txid" in raw_vin.keys():
                    vin.append(bytearray.fromhex(raw_vin["txid"]))
                if "vout" in raw_vin.keys():
                    vin.append(raw_vin["vout"])
                vins.append(vin)
        tx.append(coinbase)
        tx.append(vins)
        vouts = []
        for raw_vout in raw_tx["vout"]:
            vout = []
            if "value" in raw_vout.keys():
                vout.append(int(raw_vout["value"] * 1e8 + 0.1))
            if "n" in raw_vout.keys():
                vout.append(raw_vout["n"])
            if "scriptPubKey" in raw_vout.keys():
                script_pubkey = raw_vout["scriptPubKey"]
                tx_type_counter[script_pubkey["type"]] += 1
                addr = []
                if "addresses" in script_pubkey.keys():
                    addr = script_pubkey["addresses"]
                else:
                    tx_type_counter["tx_not_captured"] += 1
            vout.append(addr)
            vouts.append(vout)
        tx.append(vouts)
        transactions.append(tx)
        transaction_ids.append(bytearray.fromhex(raw_tx["hash"]))
    block = []
    block.append(raw_block["height"])
    block.append(bytearray.fromhex(raw_block["hash"]))
    block.append(raw_block["time"])
    block.append(raw_block["version"])
    block.append(raw_block["size"])
    block.append(transaction_ids)

    if "nextblockhash" in raw_block.keys():
        next_block = raw_block["nextblockhash"]
    else:
        next_block = None
    return (next_block, block, transactions)


def fetch_block(block_hash):
    global BLOCKCHAIN_API
    sz_req = BLOCKCHAIN_API + block_hash + ".json"
    while True:
        try:
            r = requests.get(sz_req)
            if r.status_code == requests.codes.ok:
                return r
        except KeyboardInterrupt:
            print("Ctrl-c pressed ...")
            sys.exit(1)
        except:
            print("Request failed. Retrying ...", end="\r")


def fetch_block_json(block_hash):
    return fetch_block(block_hash).json()


def fetch_block_text(block_hash):
    return fetch_block(block_hash).text


def set_blockchain_api(endpoint):
    global BLOCKCHAIN_API
    BLOCKCHAIN_API = endpoint
