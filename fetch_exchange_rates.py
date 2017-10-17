import csv
import requests
from argparse import ArgumentParser
from datetime import datetime
from cassandra.cluster import Cluster

BTC_EUR = (111697700, 163)
BTC_USD = (111720171, 167)

CSV_URL = "http://www.ariva.de/quote/historic/" + \
          "historic.csv?secu={}&boerse_id={}"


def ingest_exchange_rates(session, currency="eur",
                          security_no=111697700, boerse_id=163):

    fetch_url = CSV_URL.format(security_no, boerse_id)
    print("Fetching exchange rates from {}\n".format(fetch_url))

    insert_stmt = """INSERT INTO graphsense_raw.exchange_rates
                     (timestamp, {}) VALUES (?, ?)""".format(currency)
    prep_stmt = session.prepare(insert_stmt)

    print("Ingesting exchange rates into Cassandra.\n")
    with requests.Session() as s:
        download = s.get(fetch_url)
        decoded_content = download.content.decode("utf-8")

        cr = csv.reader(decoded_content.splitlines(), delimiter=";")
        my_list = list(cr)
        for index, row in enumerate(my_list):
            if index > 0 and len(row) != 0:
                timestamp = datetime.strptime(row[0], "%Y-%m-%d").timestamp()
                value = float(row[1].replace(".", "").replace(",", "."))
                session.execute(prep_stmt, (int(timestamp), value))

    print("Finished ingest for currency {}.".format(currency))


def main():
    parser = ArgumentParser()
    parser.add_argument("-c", "--cassandra", dest="cassandra",
                        metavar="CASSANDRA_NODE", default="localhost",
                        help="cassandra node")

    args = parser.parse_args()

    cluster = Cluster([args.cassandra])
    session = cluster.connect()

    ingest_exchange_rates(session, "eur", BTC_EUR[0], BTC_EUR[1])
    ingest_exchange_rates(session, "usd", BTC_USD[0], BTC_USD[1])


if __name__ == "__main__":
    main()
