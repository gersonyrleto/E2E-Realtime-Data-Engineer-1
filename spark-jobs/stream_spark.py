#!/usr/bin/env python3
"""
Kafka Consumer that reads user data and writes to Cassandra
Run: python stream_spark.py
"""

import json
import time
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.util import uuid_from_time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection
CASSANDRA_HOST = 'cassandra'
CASSANDRA_PORT = 9042
CASSANDRA_KEYSPACE = 'etl_keyspace'
CASSANDRA_TABLE = 'users'

# Kafka connection
KAFKA_BROKER = 'broker:29092'
KAFKA_TOPIC = 'user_created'

def init_cassandra():
    """Initialize Cassandra connection and create keyspace/table"""
    try:
        cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
        session = cluster.connect()
        logger.info("Connected to Cassandra")
        
        # Create keyspace
        session.execute(f"""
            CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
        """)
        
        session.set_keyspace(CASSANDRA_KEYSPACE)
        
        # Create table
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {CASSANDRA_TABLE} (
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
            )
        """)
        
        logger.info(f"Cassandra keyspace '{CASSANDRA_KEYSPACE}' and table '{CASSANDRA_TABLE}' initialized")
        return cluster, session
        
    except Exception as e:
        logger.error(f"Failed to initialize Cassandra: {e}")
        raise

def insert_user(session, user_data):
    """Insert user data into Cassandra"""
    try:
        query = f"""
            INSERT INTO {CASSANDRA_TABLE} 
            (id, first_name, last_name, gender, address, postcode, email, username, dob, registered_date, phone, picture, created_at)
            VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, dateof(now()))
        """
        
        session.execute(query, [
            user_data.get('first_name'),
            user_data.get('last_name'),
            user_data.get('gender'),
            user_data.get('address'),
            str(user_data.get('postcode')),
            user_data.get('email'),
            user_data.get('username'),
            user_data.get('dob'),
            user_data.get('registered_date'),
            user_data.get('phone'),
            user_data.get('picture')
        ])
        
        logger.info(f"Inserted user: {user_data.get('first_name')} {user_data.get('last_name')}")
        
    except Exception as e:
        logger.error(f"Failed to insert user: {e}")

def consume_kafka():
    """Consume messages from Kafka and write to Cassandra"""
    cluster, session = init_cassandra()
    
    try:
        # Wait for Cassandra to be fully ready
        time.sleep(5)
        
        # Initialize Kafka consumer
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='cassandra_consumer',
            enable_auto_commit=True,
            session_timeout_ms=30000
        )
        
        logger.info(f"Connected to Kafka topic '{KAFKA_TOPIC}'")
        logger.info("Starting to consume messages...")
        
        for message in consumer:
            user_data = message.value
            logger.info(f"Received message: {user_data}")
            insert_user(session, user_data)
            
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        consumer.close()
        session.shutdown()
        cluster.shutdown()
        logger.info("Kafka consumer and Cassandra connection closed")

if __name__ == '__main__':
    consume_kafka()
