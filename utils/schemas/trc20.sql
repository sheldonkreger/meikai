CREATE TABLE main.trc20_events_kafka (
                       timestamp UInt64,
                       level String,
                       message String
) ENGINE = Kafka('localhost:9092', 'tron.tether.edges0', 'trc20events', 'JSONEachRow');

CREATE TABLE main.trc20_events ON CLUSTER 'nonprod'
(
    'contract_address' String CODEC(ZSTD(2)),
    'sender' String CODEC(ZSTD(2)),
    'recipient' String CODEC(ZSTD(2)),
    'block_number' UInt64,
    'block_timestamp' UInt64,
    'converted_value' UInt256,
)
ENGINE = ReplicatedMergeTree
PARTITION BY toYYYMM(fromUnixTimestamp64Milli(block_timestamp))
ORDER BY (contract_address, recipient)

CREATE MATERIALIZED VIEW main.trc20_events_mv TO main.trc20_events AS
SELECT
    JSONExtractString(message, 'contract_address') AS contract_address,
    JSONExtractString(message, 'sender') AS sender,
    JSONExtractString(message, 'recipient') AS recipient,
    toUInt64(JSONExtractString(message, 'block_number')) AS block_number,
    toUInt64(JSONExtractString(message, 'block_timestamp')) AS block_timestamp,
    toUInt256(JSONExtractString(message, 'converted_value')) AS converted_value
FROM main.trc20_events_kafka;