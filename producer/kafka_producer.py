import sys
import os
import json

# Add the root directory of the project to the Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from utils.run_consumer import run_consumer
from utils.run_producer import run_producer

# Initialize consumer and producer
consumer = run_consumer('processed-user-login', 'processed-user-login-group')
producer = run_producer()  # Make sure to call the function

try:
    print("Starting consumer for processed-user-login...")
    for message in consumer:
        data = message.value
        print(f"Processed data received: {data}")

        # If data exists, send each record to the producer
        if data:
            # Convert the message to a dictionary, if needed
            try:
                data_dict = json.loads(data) if isinstance(data, str) else data
            except json.JSONDecodeError:
                print("Error decoding JSON data")
                continue

            # Send each item as a message to the 'processed-user-login-storage' topic
            if isinstance(data_dict, dict):  # If data is already in dict format
                producer.send('processed-user-login-storage', value=data_dict)
                print(f"Sent cleaned data to processed-user-login-storage: {data_dict}")
            elif isinstance(data_dict, list):  # If data is a list of records
                for item in data_dict:
                    producer.send('processed-user-login-storage', value=item)
                    print(f"Sent cleaned data to processed-user-login-storage: {item}")
            else:
                print("Data format is not supported for sending.")
        else:
            print("No data to send.")

except Exception as e:
    print(f"Something happened with the Kafka producer!: {e}")
finally:
    consumer.close()
    producer.close()
