##

import logging
import asyncio
import json
from typing import Any
from threading import Thread
from queue import Empty
from .frame import BLIPMessenger, BLIPMessage, MessageType
from .exceptions import BLIPError, ClientError
from .client import BLIPClient

logger = logging.getLogger('pythonblip.protocol')
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
                logger.error(f"protocol exception: {result}")

    def stop(self):
        logger.debug(f"Received protocol stop request")
        self.loop.create_task(self.disconnect())
        self.run_thread.join()

    def send_message(self, m_type: int,
                     properties: dict,
                     body: str = "",
                     body_json: Any = None,
                     reply: int = None,
                     ack_bytes: int = 0,
                     urgent: bool = False,
                     compress: bool = False,
                     no_reply: bool = False,
                     partial: bool = False):
        m = BLIPMessage.construct()

        if body_json:
            body = json.dumps(body_json, separators=(',', ':'))

        if reply:
            m.set_number(reply)
        else:
            m.next_number()
        m.urgent = urgent
        m.compressed = compress
        m.no_reply = no_reply
        m.more_coming = partial
        m.compute_flag(m_type)
        m.properties = properties

        if ack_bytes > 0:
            m.set_ack_bytes(ack_bytes)

        if len(body) > 0:
            m.body_import(body.encode('utf-8'))

        message = self.messenger.compose(m)
        self.write_queue.put(message)
        return m

    def receive_message(self, p: BLIPMessage = None):
        send_ack = False
        try:
            data = self.read_queue.get(timeout=15)
        except Empty:
            raise ClientError(408, "Receive Timeout")

        if data == 0:
            raise ClientError(self.run_status.value, self.run_message.value.decode('utf-8'))

        if p:
            old_received = p.frame_total
            m: BLIPMessage = self.messenger.receive(data, continuation=True)
            logger.debug(f"Received {m.frame_total} bytes")
            m = p.extend(m)
            new_received = m.frame_total
            logger.debug(f"Received {new_received} bytes of multipart message")
            if int(old_received / BLIPMessenger.kAckInterval) < int(new_received / BLIPMessenger.kAckInterval):
                send_ack = True
        else:
            m: BLIPMessage = self.messenger.receive(data)

        if m.type == 2:
            raise BLIPError(m.number, m.properties, m.body_as_string())

        if m.more_coming:
            if send_ack:
                logger.debug(f"Sending ACK for message {m.number} bytes {m.frame_total}")
                self.send_message(5, {}, reply=m.number, urgent=True, no_reply=True, ack_bytes=m.frame_total)
            return self.receive_message(m)

        logger.debug(f"Message #{m.number}")
        logger.debug(f"Type: {MessageType(m.type).name}")
        logger.debug(f"Properties: {m.properties}")
        try:
            logger.debug(f"Body: {m.body_as_string()}")
        except UnicodeDecodeError:
            logger.debug("Body: .... [binary data]")

        return m
