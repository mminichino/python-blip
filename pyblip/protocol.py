##

import logging
from .frame import BLIPMessenger, BLIPMessage, MessageType, MPAtomicIncrement

logger = logging.getLogger('pyblip.protocol')
logger.addHandler(logging.NullHandler())


class BLIPProtocol(object):

    def __init__(self):
        self.messenger = BLIPMessenger()

    def send_message(self, m_type: int,
                     properties: dict,
                     body: str = "",
                     urgent: bool = False,
                     compress: bool = False,
                     no_reply: bool = False,
                     partial: bool = False):
        m = BLIPMessage.construct()

        m.next_number()
        m.urgent = urgent
        m.compressed = compress
        m.no_reply = no_reply
        m.more_coming = partial
        m.compute_flag(m_type)
        m.properties = properties

        if len(body) > 0:
            m.body_import(body.encode('utf-8'))

        return self.messenger.compose(m)

    def receive_message(self, data: bytearray):
        m: BLIPMessage = self.messenger.receive(data)

        logger.debug(f"Message #{m.number}")
        logger.debug(f"Type: {MessageType(m.type).name}")
        logger.debug(f"Properties: {m.properties}")
        logger.debug(f"Body: {m.body_as_string()}")
