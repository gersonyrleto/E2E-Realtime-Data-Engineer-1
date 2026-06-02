# E2E-Realtime-Data-Engineer-1
E2E Realtime Data Engineering Project | With Airflow, Potsgree, Kafka, Spark &amp; Cassandra With Process Into Docker Compose

## Architecture System

![Figure](evidence-step-by-step/Flow-System-Architecture.png)

## Introduction
<p>This project serves as a comprehensive guide to building an end-to-end data engineering pipeline and ETL Coseption. It covers each stage from data ingestion to processing and finally to storage, utilizing a robust tech stack that includes Apache Airflow, Python, Apache Kafka, Apache Zookeeper, Apache Spark, and Cassandra. Everything is containerized using Docker for ease of deployment and scalability.
</p>

## Deep Learn
- Setting up a data pipeline with Apache Airflow
  1. Conf_inisialize_put_data_from_airflow_to_kafka
   ![Figure](evidence-step-by-step/inisialize_put_data_from_airflow_to_kafka_1.png)

  2. Inisialize_put_data_from_airflow_to_kafka_DAG_airflow
   ![Figure](evidence-step-by-step/inisialize_put_data_from_airflow_to_kafka_DAG_airflow_2.png)
- Real-time data streaming with Apache Kafka
  1. 
- Distributed synchronization with Apache Zookeeper
  1. Sync_zookeeper_control-center_&_schema_registry_on_Kafka
  ![Figure](evidence-step-by-step/sync_zookeeper_control-center_&_schema_registry_on_Kafka.png)

  2. Result_zookeeper_control-center__schema_registry_on_Kafka
   ![Figure](evidence-step-by-step/sync_zookeeper_control-center_&_schema_registry_on_Kafka_Result.png)

- Data processing techniques with Apache Spark
  1. Inisialize_spark_worker-spark_master_&_cassandra
   ![Figure](evidence-step-by-step/inisialize_spark_worker-spark_master_&_cassandra.png)

- Data storage solutions with Cassandra and PostgreSQL
  1. Set_config_in_spark_job_to_create_key_spaces&table&insert_data_from_arflow_to_cassandra
   ![Figure](evidence-step-by-step/set_config_in_spark_job_to_create_key_spaces&table&insert_data_from_arflow_to_cassandra.png)

  2. Postgree
   ![Figure](evidence-step-by-step/Sync_Postgrees.png)

- Containerizing your entire data engineering setup with Docker
  1. Docker Images
   ![Figure](evidence-step-by-step/docker-images.png)

  2. Docker Container
   ![Figure](evidence-step-by-step/docker-container.png)

  3. Docker Compose Set-Up
   ![Figure](evidence-step-by-step/docker-compose.png)
   
- Result
  1. Success_create_key_spaces&table
   ![Figure](evidence-step-by-step/Success_create_key_spaces&table_1.png)
   ![Figure](evidence-step-by-step/Success_create_key_spaces&table_2.png)

  2. Data_success_load_from_airflow_kafka&spark_to_cassandra
   ![Figure](evidence-step-by-step/Data_success_load_from_airflow_kafka&spark_to_cassandra.png)


### Thanks