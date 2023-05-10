##

import logging
import asyncio
from threading import Thread
import time
from .frame import BLIPMessenger, BLIPMessage, MessageType
from .exceptions import BLIPError
from .client import BLIPClient

logger = logging.getLogger('pyblip.protocol')
logger.addHandler(logging.NullHandler())


class BLIPProtocol(BLIPClient):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.messenger = BLIPMessenger()
        self.run_thread = Thread(target=self.start)
        self.run_thread.start()

    def start(self):
        connections = [self.loop.create_task(self.connect())]
        results = self.loop.run_until_complete(asyncio.gather(*connections, return_exceptions=True))
        for result in results:
            if isinstance(result, Exception):
                raise result

    def stop(self):
        logger.debug(f"Received protocol stop request")
        self.loop.create_task(self.disconnect())
        self.run_thread.join()

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

        message = self.messenger.compose(m)
        self.write_queue.put(message)

    def receive_message(self):
        data = self.read_queue.get()
        m: BLIPMessage = self.messenger.receive(data)

        logger.debug(f"Message #{m.number}")
        logger.debug(f"Type: {MessageType(m.type).name}")
        logger.debug(f"Properties: {m.properties}")
        logger.debug(f"Body: {m.body_as_string()}")

        if m.type == 2:
            raise BLIPError(m.number, m.properties, m.body_as_string())

        return m
