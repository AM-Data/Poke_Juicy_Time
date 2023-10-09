import logging

class CustomLogger:
    def __init__(self, name, log_file='poker_metrics.log', file_log_level=logging.WARNING, console_log_level=logging.INFO):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        # Create a file handler and set level to debug
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(file_log_level)  # Set level of file handler

        # Create a console handler with a higher log level
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_log_level)  # Set level of console handler

        # Create formatter and add it to the handlers
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Add the handlers to the logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

    def get_logger(self):
        return self.logger