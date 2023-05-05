##

import logging
from enum import Enum
import multiprocessing
import pyblip.varint as binary
import zlib
import struct


class MessageType(Enum):
    RequestType = 0,
    ResponseType = 1,
    ErrorType = 2,
    AckRequestType = 4,
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


class BLIPMessenger(object):
    DEFLATE_TRAILER = "\x00\x00\xff\xff"

    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.messages_number = MPAtomicIncrement()
        self.buffer = bytearray()
        self.crc = 0

    def compose(self, flags: int, properties: dict, body: str = None):
        header = bytearray()
        prop = bytearray()
        body = bytearray()
        message = bytearray()
        prop_string = ""

        for key in properties:
            begin = '\0' if len(prop_string) > 0 else ""
            prop_string = f"{prop_string}{begin}{key}\0{properties[key]}"
        # prop_string.replace(':', '\x00')

        buffer, _ = binary.put_uvarint(binary.uint64(self.messages_number.next))
        header.extend(buffer)
        buffer, _ = binary.put_uvarint(binary.uint64(0))
        header.extend(buffer)
        prop.append(len(prop_string) + 1)
        prop.extend(prop_string.encode('utf-8'))
        prop.append(0)

        message.extend(header)
        message.extend(prop)

        crc = zlib.crc32(prop, self.crc)
        print(crc)

        message.extend(struct.pack('>I', crc))

        print(''.join('{:02x} '.format(x) for x in message))

        return message

    def set_flag(self):
        pass

    def get_property(self):
        pass

    def set_property(self):
        pass

    def get_body(self):
        pass

    def set_body(self):
        pass
