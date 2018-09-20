CREATE KEYSPACE IF NOT EXISTS __KEYSPACE__ WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE __KEYSPACE__;
CREATE TABLE IF NOT EXISTS __KEYSPACE__.meta (sha1 ascii, repo text, commit ascii, path text, PRIMARY KEY (sha1, repo, commit, path));
CREATE TABLE IF NOT EXISTS __KEYSPACE__.hashtables (sha1 text, hashtable tinyint, value blob, PRIMARY KEY (hashtable, value, sha1));
CREATE TABLE IF NOT EXISTS __KEYSPACE__.docfreq (id text, docs int, df map<text, int>, PRIMARY KEY (id));
