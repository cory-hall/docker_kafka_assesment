import logging
from processors.data_processors import process_data

if __name__ == "__main__":
    # Script to run process_data function
    logging.basicConfig(level=logging.INFO)
    try:
        process_data()
    except KeyboardInterrupt:
        logging.info("Data processing interrupted.")
