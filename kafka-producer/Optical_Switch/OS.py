from Optical_Switch.wrapper.decorators import decorators
from .logger import Logger


class OS:
    def __init__(self):
        self.ip_address = None

    def set_ip_address(self):
        return

    @decorators(Logger().get_logger())
    def get_existing_connections(self):
        pass

