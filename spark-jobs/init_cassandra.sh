#!/bin/bash

# Wait for Cassandra to be ready
sleep 30

# Create keyspace and table
cqlsh cassandra 9042 << EOF
CREATE KEYSPACE IF NOT EXISTS etl_keyspace 
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE etl_keyspace;

CREATE TABLE IF NOT EXISTS users (
  id UUID PRIMARY KEY,
  first_name TEXT,
  last_name TEXT,
  gender TEXT,
  address TEXT,
  postcode TEXT,
  email TEXT,
  username TEXT,
  dob TEXT,
  registered_date TEXT,
  phone TEXT,
  picture TEXT,
  created_at TIMESTAMP
);

EXIT;
EOF

echo "Cassandra keyspace and table created successfully"
