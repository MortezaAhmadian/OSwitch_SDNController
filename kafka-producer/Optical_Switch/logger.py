import logging
import os

class Logger:
    _instance = None  # Singleton instance

    def __new__(cls, log_file="OS_Controller.log", level=logging.DEBUG):
        """Ensures only one instance of Logger is created (Singleton)."""
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)

            # Create a logger object
            cls._instance.logger = logging.getLogger("OS_ControllerLogger")
            cls._instance.logger.setLevel(level)

            # Prevent adding duplicate handlers
            if not cls._instance.logger.hasHandlers():
                cls._instance._configure_logger(log_file, level)

        return cls._instance

    @classmethod
    def _configure_logger(cls, log_file, level):
        """Configures the logger with console and file handlers."""
        log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(log_format)
        cls._instance.logger.addHandler(console_handler)

        # File handler (creates a log file)
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.INFO)  # Log INFO and above to file
        file_handler.setFormatter(log_format)
        cls._instance.logger.addHandler(file_handler)

    def get_logger(self):
        """Returns the logger instance."""
        return self.logger
