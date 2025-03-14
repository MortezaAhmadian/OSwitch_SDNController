import functools
from logging import exception
import traceback

class decorators:
    def __init__(self, logger):
        self.logger = logger

    def __call__(self, func):
        self.func = func
        print(f"Calling function: {self.func.__name__}")
        return self.func

    def retrieve_metrics(self, func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                self.logger.info(f"Start Retrieving: {func.__name__} from {args[0]._config['device']}")
                retrieved_metrics = func(*args, **kwargs)
                self.logger.info(f"Finish Retrieving: {func.__name__} from {args[0]._config['device']}")
                return retrieved_metrics
            except exception as e:
                self.logger.info(f"Error in {args[0]._config['device']}:{self.func.__name__}:{e}")
                self.logger.info(traceback.format_exc()) # Print full error stack trace
        return wrapper
