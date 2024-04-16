import logging

def setup_logger(log_filename):
    # Set up logging configuration
    logging.basicConfig(filename=log_filename, level=logging.INFO, format='%(asctime)s - %(levelname)8s - %(name)10s > %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    return logging.getLogger()

def log_multiline_message(logger, message):
    # Log the entire multiline message as a single entry with proper indentation and formatting
    logger.info("Message logged:\n" + '\n'.join([' ' * 4 + line.strip() for line in message.split('\n')]))
