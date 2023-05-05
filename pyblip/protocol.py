##

import logging
from .frame import BLIPMessenger


class BLIPProtocol(object):

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.messenger = BLIPMessenger()

    def get_checkpoint(self, client: str = "pyblip"):
        flags = 0
        properties = {
            "Profile": "setCheckpoint",
            "client": client
        }

        return self.messenger.compose(flags, properties)
