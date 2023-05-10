##
##

import time
import logging
import asyncio
import websockets
from websockets.legacy.client import WebSocketClientProtocol
import multiprocessing
from queue import Empty
from .exceptions import WebSocketError

logger = logging.getLogger('pyblip.client')
logger.addHandler(logging.NullHandler())


class BLIPClient(object):

    def __init__(self, uri: str, headers: dict):
        self.uri = uri
        self.headers = headers
        self.run_loop = True
        self.websocket: WebSocketClientProtocol = None
        self.loop = asyncio.get_event_loop()
        self.read_queue = multiprocessing.Queue()
        self.write_queue = multiprocessing.Queue()

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
                            logger.error(f"run loop exception: {result}")
                            raise result
        except websockets.ConnectionClosed:
            return
        except Exception as err:
            raise WebSocketError(f"Websocket error: {err}")

    async def disconnect(self):
        logger.debug(f"Received disconnect request")
        await self.websocket.close()

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
