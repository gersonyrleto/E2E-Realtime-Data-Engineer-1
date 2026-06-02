#!/bin/bash

# Interactive Cassandra Query Script
# Usage: docker exec cassandra bash /tmp/cass_query.sh

cqlsh cassandra 9042 << 'EOF'
-- Show keyspaces
DESCRIBE KEYSPACES;

-- Use ETL keyspace
USE etl_keyspace;

-- Show tables
DESCRIBE TABLES;

-- Show users table schema
DESCRIBE TABLE users;

-- Query all users
SELECT first_name, last_name, email, gender, phone FROM users;

-- Count total users
SELECT COUNT(*) FROM users;
EOF
