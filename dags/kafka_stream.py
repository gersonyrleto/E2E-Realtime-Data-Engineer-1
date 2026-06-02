from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

default_args = {
    'owner': 'gersonleto',
    'start_date': datetime(2026, 6, 1)
}

def get_data():
    import json
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']} {location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    
    logger = logging.getLogger(__name__)
    logger.info("Starting Kafka producer task")
    
    try:
        logger.info("Connecting to Kafka broker at broker:29092")
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],
            max_block_ms=5000,
            request_timeout_ms=10000
        )
        logger.info("Connected to Kafka successfully")
        
        for i in range(3):
            try:
                logger.info(f"Fetching data iteration {i+1}")
                res = get_data()
                res = format_data(res)
                
                msg = json.dumps(res).encode('utf-8')
                logger.info(f"Sending message to user_created topic: {msg[:100]}")
                
                future = producer.send('user_created', msg)
                record_metadata = future.get(timeout=5)
                
                logger.info(f"Message sent successfully to partition {record_metadata.partition}, offset {record_metadata.offset}")
                time.sleep(2)
            except Exception as e:
                logger.error(f"Error sending message iteration {i+1}: {str(e)}", exc_info=True)
        
        producer.flush()
        producer.close()
        logger.info("Producer closed successfully")
        
    except Exception as e:
        logger.error(f"Fatal error in stream_data: {str(e)}", exc_info=True)
        raise

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )