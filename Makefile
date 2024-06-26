setup:
	docker exec -it kinesis-prototype-db-1 apt-get update
	docker exec -it kinesis-prototype-db-1 apt-get install postgresql-16-wal2json
	docker exec -it -e PGPASSWORD=postgres kinesis-prototype-db-1  psql -U postgres -d postgres -c "SELECT * FROM pg_create_logical_replication_slot('test_slot_wal2json', 'wal2json');"
