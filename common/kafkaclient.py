import os
import json
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import logging

logger = logging.getLogger(__name__)

class KafkaClient:
    def __init__(self):
        self.bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
        self.sasl_username = os.getenv('KAFKA_SASL_USERNAME')
        self.sasl_password = os.getenv('KAFKA_SASL_PASSWORD')
        
        # SASL settings for Confluent Cloud
        self.security_config = {
            'bootstrap_servers': self.bootstrap_servers,
            'security_protocol': 'SASL_SSL',
            'sasl_mechanism': 'PLAIN',
            'sasl_plain_username': self.sasl_username,
            'sasl_plain_password': self.sasl_password,
        }
        
        self.producer = None
        self.consumer = None
    
    def create_producer(self):
        """Create a Kafka producer"""
        try:
            self.producer = KafkaProducer(
                **self.security_config,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Producer connected to Kafka: {self.bootstrap_servers}")
            return self.producer
        except Exception as e:
            logger.error(f"Error creating producer: {e}")
            raise
    
    def create_consumer(self, topic, group_id='weather-consumer-group'):
        """Create a Kafka consumer"""
        try:
            self.consumer = KafkaConsumer(
                topic,
                **self.security_config,
                group_id=group_id,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            logger.info(f"Consumer connected to the topic '{topic}' in Kafka: {self.bootstrap_servers}")
            return self.consumer
        except Exception as e:
            logger.error(f"Error creating consumer: {e}")
            raise
    
    def send_message(self, topic, message):
        """Send a message to the topic"""
        if not self.producer:
            self.create_producer()
        
        try:
            future = self.producer.send(topic, value=message)
            record_metadata = future.get(timeout=10)
            logger.debug(f"Message sent to {record_metadata.topic}[{record_metadata.partition}] @ offset {record_metadata.offset}")
            return True
        except KafkaError as e:
            logger.error(f"Error sending message: {e}")
            return False
    
    def close(self):
        """Close connections"""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Producer closed")
        
        if self.consumer:
            self.consumer.close()
            logger.info("Consumer closed")