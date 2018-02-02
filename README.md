# GraphSense Datafeed

A service for ingesting raw data into Apache Cassandra.

At the moment, the following sources are supported:

* Bitcoin transactions extracted from the [Bitcoin Client][bitcoin-client]
* currency conversion rates from [ariva.de][ariva.de]

## Prerequisites

### Python

Make sure Python 3 is installed

    python --version

Install dependencies (`requests` and `cassandra-driver`), e.g. via `pip`

    pip install -r requirements.txt

### Apache Cassandra

Download and install [Apache Cassandra][apache-cassandra] >= 3.11
in `$CASSANDRA_HOME`.

Start Cassandra (in the foreground for development purposes):

    $CASSANDRA_HOME/bin/cassandra -f

Connect to Cassandra via CQL

    $CASSANDRA_HOME/bin/cqlsh

and test if it is running

    cqlsh> SELECT cluster_name, listen_address FROM system.local;

    cluster_name | listen_address
    --------------+----------------
    Test Cluster |      127.0.0.1

    (1 rows)

## Ingest Bitcoin transactions

Create raw data keyspace in Cassandra

    $CASSANDRA_HOME/bin/cqlsh -f schema_raw.cql

Use the following script to retrieve larger transaction files from a running
Bitcoin client. The following retrieves 50000 blocks from the Bitcoin client
and stores them in binary format to `block_*` files within the sample folder
(replace `BITCOIN_CLIENT` with the hostname or IP address of the Bitcoin REST
interface)

    mkdir data
    python fetch_blocks.py -d ./data -h BITCOIN_CLIENT -n 50000

The -f option specifies the prefix of the files generated (saves 32Mb files
named into the `data` folder) and the -h option specifies the server running
the Bitcoin server.

Ingest the blocks from the data directory into Cassandra

    python ingest_data.py -d ./data/ -c localhost -p 2

The -c option specifies that your Cassandra server is running on the
localhost and the -p options specifies the number of worker processes to
be used by the ingest.

If necessary the `continuous_ingest.py` script can be started after the
`ingest_raw.py` script in order to retrieve and ingest the blocks
present in the blockchain but not ingested into Cassandra. This script
checks periodically (specified via the -s option) for newly available
blocks and ingests them.

    python continuous_ingest.py -h BITCOIN_CLIENT -c localhost -s 10

## Ingest exchange rates

    python fetch_exchange_rates.py -c localhost

The -c option specifies that the Cassandra server is running on the
localhost.

[bitcoin-client]: https://github.com/graphsense/bitcoin-client
[ariva.de]: http://www.ariva.de
[apache-cassandra]: http://cassandra.apache.org/download/
