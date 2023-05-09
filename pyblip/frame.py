##

import attr
import logging
from attr.validators import instance_of as io
from enum import Enum
import multiprocessing
import pyblip.varint as binary
from .exceptions import CRCMismatch
import zlib
import struct
from io import BytesIO

logger = logging.getLogger('pyblip.frame')
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
            bytearray()
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

    def body_as_string(self):
        return self.body.decode('utf-8')

    def body_import(self, data: bytes):
        self.body.extend(data)

    def has_body(self) -> bool:
        return len(self.body) > 0

    def prop_string(self):
        prop_string = ""
        for key in self.properties:
            begin = '\0' if len(prop_string) > 0 else ""
            prop_string = f"{prop_string}{begin}{key}\0{self.properties[key]}"
        prop_string = f"{prop_string}\0"
        return prop_string.encode('utf-8'), len(prop_string)

    def prop_import(self, data: bytes):
        data = data.rstrip(b'\0')
        prop_list = data.split(b'\0')
        for k, v in zip(*[iter(prop_list)]*2):
            self.properties[k.decode('utf-8')] = v.decode('utf-8')

    @property
    def as_dict(self):
        return self.__dict__


class BLIPMessenger(object):
    DEFLATE_TRAILER = "\x00\x00\xff\xff"

    def __init__(self):
        self.messages_number = MPAtomicIncrement()
        self.buffer = bytearray()
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

        prop_string, prop_length = m.prop_string()
        buffer, _ = binary.put_uvarint(binary.uint64(prop_length))
        message.extend(buffer)
        message.extend(prop_string)

        if m.has_body():
            message.extend(m.body)

        self.s_crc = zlib.crc32(message[header:], self.s_crc)

        message.extend(struct.pack('>I', self.s_crc))

        for line in FrameDump(message):
            logger.debug(line)

        return message

    def receive(self, message: bytearray) -> BLIPMessage:
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

        prop_len, _ = binary.read_uvarint(r)
        prop_data = r.read(prop_len)

        m.prop_import(prop_data)

        remainder = r.getbuffer().nbytes - r.tell()
        if remainder > 4:
            body = r.read(remainder - 4)
            m.body_import(body)

        message_sum = r.read(4)

        r_crc = struct.unpack('>I', message_sum)
        self.r_crc = zlib.crc32(message[header:total - 4], self.r_crc)

        if r_crc[0] != self.r_crc:
            raise CRCMismatch(f"message {message_num} CRC mismatch")

        return m
