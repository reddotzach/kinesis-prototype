ALTER SYSTEM SET wal_level = logical;
alter system set max_replication_slots = '64';
-- SELECT * FROM pg_create_logical_replication_slot('test_slot_wal2json', 'wal2json');

CREATE TABLE outbox (
    id SERIAL PRIMARY KEY,
    aggregate_id TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    payload JSONB NOT NULL
);
