CREATE KEYSPACE IF NOT EXISTS hashes WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE hashes;
CREATE TABLE IF NOT EXISTS hashes.blob_hash_files (blob_hash ascii, repo text, file_path text, PRIMARY KEY (blob_hash, repo));

DESCRIBE KEYSPACE hashes;
