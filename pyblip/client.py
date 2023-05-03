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
        result = self.ws.recv()
        return self.decode_frame(result)

    def read_uvarint(self, stream):
        shift = 0
        result = 0
        while True:
            i = stream.read(1)
            result |= (i & 0x7f) << shift
            shift += 7
            if not (i & 0x80):
                break
        return result

    def write_uvarint(self, data):
        buf = b''
        while True:
            towrite = data & 0x7f
            data >>= 7
            if data:
                buf += bytes(towrite | 0x80)
            else:
                buf += bytes(towrite)
                break
        return buf

    def decompress(self, frame: bytes):
        data = bytearray(frame)
        data.append(b'\x00\x00\xff\xff')
        return zlib.decompress(data)

    def decode_frame(self, data: bytes):
        buffer = self.decompress(data)
        stream = BytesIO(buffer)
        request_number = self.read_uvarint(stream)
        msg_type = self.read_uvarint(stream)

        print(request_number)
        print(msg_type)

    def encode_frame(self, body):
        pass

