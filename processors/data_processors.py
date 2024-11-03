import time
import sys
import os
import logging
import pandas as pd
from kafka.errors import KafkaError

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.run_consumer import run_consumer
from utils.run_producer import run_producer

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def is_valid_ip(ip):
    parts = ip.split('.')
    return len(parts) == 4 and all(part.isdigit() and 0 <= int(part) < 256 for part in parts)

# Retry decorator for transient errors
def retry_on_failure(func, retries=3, delay=1):
    for attempt in range(retries):
        try:
            return func()
        except Exception as e:
            if attempt < retries - 1:
                logger.warning(f"Retry {attempt + 1}/{retries} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"All retries failed: {e}")
                raise

def process_data():
    consumer = run_consumer('cleaned-user-login', 'process-data-group')
    producer = run_producer()

    try:
        logger.info("Starting data processor...")
        for message in consumer:
            data = message.value  # Get the cleaned data

            try:
                # Validate and process fields
                if 'timestamp' in data:
                    data['timestamp'] = pd.to_datetime(data['timestamp']).strftime('%Y-%m-%d %H:%M:%S')

                if 'locale' in data:
                    data['locale'] = data['locale'].upper()

                if 'ip' in data and not is_valid_ip(data['ip']):
                    data['ip'] = 'Invalid IP'

                if 'device_type' in data:
                    device_mapping = {
                        "android": "Android Device",
                        "iOS": "iOS Device"
                    }
                    data['device_type'] = device_mapping.get(data['device_type'], data['device_type'])

                # Send processed data to the next topic
                def send_message():
                    producer.send('processed-user-login', value=data)
                    logger.info(f"Sent processed data to 'processed-user-login': {data}")

                retry_on_failure(send_message)

            except (KafkaError, ValueError) as e:
                logger.error(f"Error processing message {message}: {e}")
                # Send to DLQ in case of processing errors
                producer.send('processed-user-login-dlq', value=message.value)

    except Exception as e:
        logger.critical(f"Critical error in process_data function: {e}")
    finally:
        consumer.close()
        producer.close()

def analyze_data():
    consumer = run_consumer('cleaned-user-login', 'analyze-data-group')
    data_storage = []

    try:
        logger.info("Starting data analyzer...")
        for message in consumer:
            try:
                processed_data = message.value  # Get the cleaned data
                data_storage.append(processed_data)

                # Convert data to DataFrame
                df = pd.DataFrame(data_storage)

                # Analyze data
                unique_users = df['user_id'].nunique()
                device_counts = df['device_type'].value_counts().reset_index()
                locale_counts = df['locale'].value_counts().reset_index()
                df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                hour_counts = df['timestamp'].dt.hour.value_counts().sort_index().reset_index()
                unique_devices = df['device_id'].nunique()

                logger.info(f"Unique users: {unique_users}")
                logger.info(f"Device usage:\n{device_counts}")
                logger.info(f"Locale distribution:\n{locale_counts}")
                logger.info(f"User activity by hour:\n{hour_counts}")
                logger.info(f"Unique devices: {unique_devices}")
                logger.info('\n')
                # time.sleep(30)

            except (ValueError, KeyError) as e:
                logger.error(f"Error analyzing message {message}: {e}")
                # Optionally send problematic messages to a DLQ for review
                producer = run_producer()  # Re-create producer if needed for DLQ
                producer.send('data-analysis-dlq', value=message.value)
                producer.close()

    except Exception as e:
        logger.critical(f"Critical error in analyze_data function: {e}")
    finally:
        consumer.close()

