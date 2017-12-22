CREATE KEYSPACE IF NOT EXISTS __KEYSPACE__ WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE __KEYSPACE__;
CREATE TABLE IF NOT EXISTS __KEYSPACE__.blob_hash_files (blob_hash ascii, repo text, ref_hash ascii, file_path text, PRIMARY KEY (blob_hash, repo, ref_hash, file_path));
