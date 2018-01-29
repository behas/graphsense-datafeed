DROP KEYSPACE IF EXISTS graphsense_raw;
CREATE KEYSPACE graphsense_raw WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };

USE graphsense_raw;

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
    size smallint,
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

