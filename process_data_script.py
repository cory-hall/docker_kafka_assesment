import logging
from processors.data_processors import process_data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        process_data()
    except KeyboardInterrupt:
        logging.info("Data processing interrupted.")
