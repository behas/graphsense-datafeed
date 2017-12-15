DROP KEYSPACE IF EXISTS graphsense_raw_dev_new;
CREATE KEYSPACE graphsense_raw_dev_new WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3 };

USE graphsense_raw_dev_new;

// BITCOIN blockchain data

CREATE TYPE input (
    txid blob,
    vout int
);

CREATE TYPE output (
    value bigint,
    n int,
    addresses list<text>
);

CREATE TABLE block (
    height int PRIMARY KEY,
    block_hash blob,
    timestamp int,
    block_version int,
    size int,
    txs list<blob>
);

CREATE TABLE transaction (
    block_group int,
    height int,
    tx_hash blob,
    tx_number int,
    timestamp int,
    coinbase boolean,
    vin list<FROZEN<input>>,
    vout list<FROZEN<output>>,
    PRIMARY KEY (block_group, height, tx_hash)
);

CREATE TABLE exchange_rates (
    timestamp int PRIMARY KEY,
    eur double,
    usd double
);

CREATE TABLE tag (
    address text,
    tag text,
    tag_uri text,
    description text,
    actor_category text,
    source text,
    source_uri text,
    timestamp int,
    PRIMARY KEY (address, tag, source)
);

