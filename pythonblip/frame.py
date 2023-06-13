##

from __future__ import annotations
import attr
import logging
from attr.validators import instance_of as io
from enum import Enum
import multiprocessing
import pythonblip.varint as binary
from .exceptions import CRCMismatch
import zlib
import struct
import json
from io import BytesIO

logger = logging.getLogger('pythonblip.frame')
logger.addHandler(logging.NullHandler())


class FrameDump:
    def __init__(self, buffer):
        self.buffer = buffer

    def __iter__(self):
        for i in range(0, len(self.buffer), 16):
            block = bytearray(self.buffer[i: i + 16])
            line = "{:08x}  {:23}  {:23}  |{:16}|".format(
                i,
                " ".join(("{:02x}".format(x) for x in block[:8])),
                " ".join(("{:02x}".format(x) for x in block[8:])),
                "".join((chr(x) if 32 <= x < 127 else "." for x in block)),
            )
            yield line
        yield "{:08x}".format(len(self.buffer))

    def __str__(self):
        return "\n".join(self)

    def __repr__(self):
        return "\n".join(self)


class MessageType(Enum):
    RequestType = 0
    ResponseType = 1
    ErrorType = 2
    AckRequestType = 4
    AckResponseType = 5


class FrameFlags(Enum):
    kTypeMask = 0x07
    kCompressed = 0x08
    kUrgent = 0x10
    kNoReply = 0x20
    kMoreComing = 0x40


class MPAtomicIncrement(object):

    def __init__(self, i=1, s=1):
        self.count = multiprocessing.Value('i', i)
        self._set_size = s
        self.set_count = multiprocessing.Value('i', s)

    def reset(self, i=1):
        with self.count.get_lock():
            self.count.value = i

    def set_size(self, n):
        self._set_size = n
        with self.set_count.get_lock():
            self.set_count.value = self._set_size

    @property
    def do_increment(self):
        with self.set_count.get_lock():
            if self.set_count.value == 1:
                self.set_count.value = self._set_size
                return True
            else:
                self.set_count.value -= 1
                return False

    @property
    def next(self):
        if self.do_increment:
            with self.count.get_lock():
                current = self.count.value
                self.count.value += 1
            return current
        else:
            return self.count.value


message_number = MPAtomicIncrement()


@attr.s
class BLIPMessage(object):
    number = attr.ib(validator=io(int))
    type = attr.ib(validator=io(int))
    compressed = attr.ib(validator=io(bool))
    urgent = attr.ib(validator=io(bool))
    no_reply = attr.ib(validator=io(bool))
    more_coming = attr.ib(validator=io(bool))
    properties = attr.ib(validator=io(dict))
    body = attr.ib(validator=io(bytearray))
    frame_size = attr.ib(validator=io(int))
    ack_bytes = attr.ib(validator=io(int))

    @classmethod
    def construct(cls):
        return cls(
            0,
            0,
            False,
            False,
            False,
            False,
            {},
            bytearray(),
            0,
            0
        )

    def set_number(self, n: int):
        self.number = n

    def next_number(self):
        self.number = message_number.next

    def set_type(self, n: int):
        self.type = MessageType(n & FrameFlags.kTypeMask.value).value

    def set_flags(self, n: int):
        if n & FrameFlags.kUrgent.value != 0:
            self.urgent = True
        if n & FrameFlags.kCompressed.value != 0:
            self.compressed = True
        if n & FrameFlags.kNoReply.value != 0:
            self.no_reply = True
        if n & FrameFlags.kMoreComing.value != 0:
            self.more_coming = True

    def compute_flag(self, n: int):
        self.type = self.type | n
        if self.urgent:
            self.type = self.type | FrameFlags.kUrgent.value
        if self.compressed:
            self.type = self.type | FrameFlags.kCompressed.value
        if self.no_reply:
            self.type = self.type | FrameFlags.kNoReply.value
        if self.more_coming:
            self.type = self.type | FrameFlags.kMoreComing.value

    def set_ack_bytes(self, n: int):
        self.ack_bytes = n

    def body_as_string(self):
        return self.body.decode('utf-8')

    def body_as_bytes(self) -> bytes:
        return bytes(self.body)

    def body_import(self, data: bytes):
        self.body.extend(data)
        self.frame_size += len(data) + 4

    def has_body(self) -> bool:
        return len(self.body) > 0

    def prop_string(self):
        prop_string = ""
        for key in self.properties:
            begin = '\0' if len(prop_string) > 0 else ""
            prop_string = f"{prop_string}{begin}{key}\0{self.properties[key]}"
        prop_string = f"{prop_string}\0"
        return prop_string.encode('utf-8'), len(prop_string)

    def prop_encode(self):
        s = json.dumps(self.properties, separators=('\0', '\0')).encode('utf-8')
        s = s.replace(b'\x22', b'')
        s = s.replace(b'\x7b', b'')
        s = s.replace(b'\x7d', b'')
        s = s + b'\x00'
        return s, len(s)

    def prop_import(self, data: bytes):
        data = data.rstrip(b'\0')
        prop_list = data.split(b'\0')
        for k, v in zip(*[iter(prop_list)]*2):
            self.properties[k.decode('utf-8')] = v.decode('utf-8')

    def extend(self, m: BLIPMessage):
        self.type = m.type
        self.urgent = m.urgent
        self.compressed = m.compressed
        self.more_coming = m.more_coming
        self.no_reply = m.no_reply
        self.body_import(m.body)
        return self

    def frame_extend(self, n):
        self.frame_size += n

    @property
    def frame_total(self):
        return self.frame_size

    @property
    def as_dict(self):
        return self.__dict__


class BLIPMessenger(object):
    DEFLATE_TRAILER = b"\x00\x00\xff\xff"
    kAckInterval = 50000
    kMaxUnackedBytes = 128000

    def __init__(self):
        self.messages_number = MPAtomicIncrement()
        self.buffer = bytearray()
        self.unzip = zlib.decompressobj(-zlib.MAX_WBITS)
        self.zip = zlib.compressobj(wbits=-zlib.MAX_WBITS)
        self.s_crc = 0
        self.r_crc = 0

    def compose(self, m: BLIPMessage):
        header = 0
        message = bytearray()

        buffer, n = binary.put_uvarint(binary.uint64(m.number))
        message.extend(buffer)
        header += n
        buffer, n = binary.put_uvarint(binary.uint64(m.type))
        message.extend(buffer)
        header += n

        if (m.type & 0x07) == MessageType.AckResponseType.value:
            buffer, n = binary.put_uvarint(binary.uint64(m.ack_bytes))
            message.extend(buffer)
            return message

        prop_string, prop_length = m.prop_encode()
        buffer, _ = binary.put_uvarint(binary.uint64(prop_length))
        message.extend(buffer)
        message.extend(prop_string)

        if m.has_body():
            message.extend(m.body)

        self.s_crc = zlib.crc32(message[header:], self.s_crc)

        if m.compressed:
            deflated_full = self.zip.compress(message[header:])
            deflated = deflated_full[:len(deflated_full) - 4]
            message[header:] = deflated

        message.extend(struct.pack('>I', self.s_crc))

        for line in FrameDump(message):
            logger.debug(line)

        return message

    def error_frame(self, code: int, e_type: str, message: str):
        m = BLIPMessage.construct()

        m.next_number()
        m.set_type(2)
        m.set_flags(0)
        m.properties = {
            "Error-Domain": e_type,
            "Error-Code": code
        }
        m.body_import(message.encode('utf-8'))

        return self.compose(m)

    def receive(self, message: bytearray, continuation: bool = False) -> BLIPMessage:
        m = BLIPMessage.construct()
        header = 0
        total = len(message)

        for line in FrameDump(message):
            logger.debug(line)

        r = BytesIO(message)

        message_num, n = binary.read_uvarint(r)
        header += n
        flags, n = binary.read_uvarint(r)
        header += n

        m.set_number(message_num)
        m.set_type(flags)
        m.set_flags(flags)

        if m.compressed:
            logger.debug("received compressed frame")
            compressed_block = message[header:total - 4]
            compressed_block = compressed_block + BLIPMessenger.DEFLATE_TRAILER
            inflated = self.unzip.decompress(compressed_block)
            self.r_crc = zlib.crc32(inflated, self.r_crc)
            inflated = inflated + message[-4:]
            r = BytesIO(inflated)
            for line in FrameDump(inflated):
                logger.debug(line)
        else:
            self.r_crc = zlib.crc32(message[header:total - 4], self.r_crc)

        if not continuation:
            prop_len, n = binary.read_uvarint(r)
            m.frame_extend(n + prop_len)
            prop_data = r.read(prop_len)
            m.prop_import(prop_data)

        remainder = r.getbuffer().nbytes - r.tell()
        if remainder > 4:
            body = r.read(remainder - 4)
            m.body_import(body)

        message_sum = r.read(4)
        r_crc = struct.unpack('>I', message_sum)

        if r_crc[0] != self.r_crc:
            raise CRCMismatch(f"message {message_num} CRC mismatch")

        return m
