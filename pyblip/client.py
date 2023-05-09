##
##

import struct
import zlib
from io import BytesIO

from websocket import create_connection


class BlipClient:

    def __init__(self, uri: str, auth_header: dict):
        self.ws = create_connection(uri,
                                    header=auth_header,
                                    subprotocols=['BLIP_3+CBMobile_3']
                                    )

    def get_message(self):
        while True:
            data = self.ws.recv()
            if data:
                return data

    def send_message(self, data: bytearray):
        self.ws.send(data)
