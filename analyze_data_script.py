import logging
from processors.data_processors import analyze_data

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        analyze_data()
    except KeyboardInterrupt:
        logging.info("Data analysis interrupted.")
