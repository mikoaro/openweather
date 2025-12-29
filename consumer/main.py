import os
import sys
import logging
from dotenv import load_dotenv

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.supabaseclient import SupabaseClient  # ✅ NEW!
from common.kafkaclient import KafkaClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)  

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'raw_data')
KAFKA_GROUP_ID = os.getenv('KAFKA_CONSUMER_GROUP', 'weather-consumer-group')


def validate_environment():
    required_kafka = [KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC]
    required_supabase = [os.getenv('SUPABASE_URL'), os.getenv('SUPABASE_KEY')]
    
    if not all(required_kafka):
        logger.error("❌ Kafka variables missing")
        return False
    
    if not all(required_supabase):
        logger.error("❌ Supabase variables missing")
        return False
    
    return True


def main():
    logger.info("🚀 Starting Weather Consumer...")
    
    if not validate_environment():
        logger.error("❌ Shutting down due to missing configuration settings")
        return
    
    try:
        # Create BOTH clients
        kafka_client = KafkaClient()
        consumer = kafka_client.create_consumer(
            topic=KAFKA_TOPIC,
            group_id=KAFKA_GROUP_ID
        )
        
        db_client = SupabaseClient()  # ✅ NEW!
        
        logger.info("✅ Consumer and Supabase successfully connected")
        logger.info(f"📡 Waiting for messages from the topic '{KAFKA_TOPIC}'...")
        
    except Exception as e:
        logger.error(f"Error initializing clients: {e}")
        return
    
    try:
        message_count = 0
        
        for message in consumer:
            message_count += 1
            data = message.value
            
            try:
                logger.info(f"📩 Message #{message_count}: {data['city']} - {data['temperature_celsius']}°C")
                
                db_client.insert_data(data)
                logger.info(f"✅ Data saved in Supabase: {data['city']}")
                
            except KeyError as e:
                logger.error(f"Invalid message structure: {e}")
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        logger.info("\n⏹️ Consumer interrupted by the user (Ctrl+C)")
    except Exception as e:
        logger.error(f"Fatal error in the consumer: {e}")
    finally:
        kafka_client.close()
        db_client.close()
        logger.info("Consumer finalized")


if __name__ == "__main__":
    main()
    