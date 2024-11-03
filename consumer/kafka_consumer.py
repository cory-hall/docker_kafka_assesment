import time
import sys
import os
from kafka.errors import KafkaError
import logging
from jsonschema import validate, ValidationError

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from processors.data_processors import process_data, analyze_data
from utils.data_cleaner import clean_data
from utils.run_consumer import run_consumer
from utils.run_producer import run_producer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka Consumer Setup
consumer = run_consumer('user-login', 'user-login-group')

# Kafka Producer Setup
producer = run_producer()

# Define schema for validation
schema = {
    "type": "object",
    "properties": {
        "user_id": {"type": "string", "pattern": "^[a-f0-9-]"},  # UUID pattern
        "app_version": {"type": "string", "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"},  # Semantic versioning
        "ip": {"type": "string", "format": "ipv4"},  # IPv4 address format
        "locale": {"type": "string", "minLength": 2, "maxLength": 2},  # Locale (assuming 2-character code)
        "device_id": {"type": "string", "pattern": "^[a-f0-9-]"},  # UUID pattern
        "timestamp": {"type": "integer", "minimum": 0},  # UNIX timestamp, should be non-negative
        "device_type": {"type": "string", "enum": ["android", "iOS"]}  # Accepts "android" or "iOS" only
    },
    "required": ["user_id", "app_version", "ip", "locale", "device_id", "timestamp", "device_type"]
}

# Helper function for retries
def retry_on_failure(func, retries=5, delay=1):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"All retries failed: {e}")
                raise
    
try:
    logger.info("Starting consumer for user-login...")

    for message in consumer:
        try:
            data = message.value
            logger.info(f"Raw data received: {data}")

            # Schema validation
            try:
                validate(instance=data, schema=schema)
            except ValidationError as ve:
                logger.error(f"Validation error for message {data}: {ve}")
                producer.send('user-login-dlq', value=data)
                continue  # Skip to the next message

            # Data cleaning
            cleaned_data = clean_data([data])
            logger.info(f"Cleaned data: {cleaned_data}")

            # Check if cleaned data is valid
            if cleaned_data is not None and not cleaned_data.empty:
                for item in cleaned_data.to_dict(orient='records'):
                    # Send cleaned data to the output topic with retry logic
                    def send_message():
                        producer.send('cleaned-user-login', value=item)
                        logger.info(f"Sent cleaned data to cleaned-user-login: {item}")
                    
                    retry_on_failure(send_message)

            else:
                logger.warning("No cleaned data to send.")
        
        except KafkaError as ke:
            logger.error(f"Kafka error while processing message {message}: {ke}")
            # Send the raw data to the DLQ in case of Kafka-specific errors
            producer.send('user-login-dlq', value=message.value)
        
        except Exception as e:
            logger.error(f"Unexpected error for message {message}: {e}")
            # Send the problematic message to DLQ
            producer.send('user-login-dlq', value=message.value)

except Exception as e:
    logger.critical(f"Critical error in Kafka consumer: {e}")

finally:
    logger.info("Closing consumer and producer connections.")
    consumer.close()
    producer.close()
