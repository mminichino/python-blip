##
##

import time
import logging
import asyncio
import websockets
from websockets.legacy.client import WebSocketClientProtocol
from websockets.exceptions import InvalidStatusCode, ConnectionClosed
import multiprocessing
from multiprocessing import Lock
from queue import Empty
from pyblip.frame import MPAtomicIncrement
from .exceptions import WebSocketError, NotAuthorized, HTTPNotImplemented, InternalServerError

logger = logging.getLogger('pyblip.client')
logger.addHandler(logging.NullHandler())
sent_counter = MPAtomicIncrement()


class BLIPClient(object):

    def __init__(self, uri: str, headers: dict):
        lock = Lock()
        self.uri = uri
        self.headers = headers
        self.run_loop = True
        self.websocket = WebSocketClientProtocol()
        self.loop = asyncio.get_event_loop()
        self.read_queue = multiprocessing.Queue()
        self.write_queue = multiprocessing.Queue()
        self.run_status = multiprocessing.Value('i', 0)
        self.run_message = multiprocessing.Array('c', 256, lock=lock)

    async def connect(self):
        tasks = []

        try:
            connection = websockets.connect(self.uri,
                                            extra_headers=self.headers,
                                            subprotocols=['BLIP_3+CBMobile_3'],
                                            logger=logger)
            async with connection as self.websocket:
                while self.websocket.open:
                    tasks.append(self.loop.create_task(self.reader()))
                    tasks.append(self.loop.create_task(self.writer()))
                    results = await asyncio.gather(*tasks, return_exceptions=True)
                    for result in results:
                        if isinstance(result, Exception):
                            logger.error(f"connection: {result}")
                            raise result
        except ConnectionClosed:
            return
        except InvalidStatusCode as err:
            self.handle_exception(err.status_code, str(err))
        except Exception as err:
            self.handle_exception(500, str(err))

    async def disconnect(self):
        logger.debug(f"Received disconnect request")
        await self.websocket.close()

    def handle_exception(self, code: int, message: str):
        text = (message[:256] + '..') if len(message) > 256 else message
        self.run_status.value = code
        self.run_message.value = text.encode('utf-8')
        self.read_queue.put(0)

    async def reader(self):
        try:
            data = await asyncio.wait_for(self.websocket.recv(), timeout=1.0)
            if data:
                logger.debug(f"received data frame")
                self.read_queue.put(data)
        except asyncio.TimeoutError:
            pass
        except Exception as err:
            logger.debug(f"Reader error: {err}")
            raise

    async def writer(self):
        try:
            data = self.write_queue.get(block=False)
            await self.websocket.send(data)
            logger.debug(f"sent data frame")
        except Empty:
            time.sleep(1.0)
        except Exception as err:
            logger.debug(f"Writer error: {err}")
            raise
