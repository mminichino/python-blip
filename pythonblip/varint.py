##
##

import struct
from typing import Tuple
from io import BytesIO
import sys

MaxVarintLen16 = 3
MaxVarintLen32 = 5
MaxVarintLen64 = 10


def uint64(n: int):
    return struct.pack('Q', n)


def int64(n: int):
    return struct.pack('q', n)


def from_uint64(n: bytes):
    _output = bytearray(8)
    for n, b in enumerate(n):
        _output[n] = b
    return struct.unpack('Q', bytes(_output))[0]


def from_int64(n: bytes):
    _output = bytearray(8)
    for n, b in enumerate(n):
        _output[n] = b
    return struct.unpack('q', bytes(_output))[0]


def append_uvarint(buf: bytearray, x: bytes) -> bytearray:
    _output = bytearray()
    uvarint_ux = int.from_bytes(x, sys.byteorder)
    while uvarint_ux >= 0x80:
        b = uvarint_ux & 0xff
        b = b | 0x80
        _output.append(b)
        uvarint_ux >>= 7
    f = uvarint_ux & 0xff
    _output.append(f)
    buf.extend(_output)
    return buf


def put_uvarint(x: bytes) -> Tuple[bytearray, int]:
    _output = bytearray()
    uvarint_ux = int.from_bytes(x, sys.byteorder)
    i = 0
    while uvarint_ux >= 0x80:
        b = uvarint_ux & 0xff
        b = b | 0x80
        _output.append(b)
        uvarint_ux >>= 7
        i += 1
    f = uvarint_ux & 0xff
    _output.append(f)
    return _output, i + 1


def uvarint(buf: bytearray) -> Tuple[int, int]:
    x = 0
    s = 0
    for n in range(len(buf)):
        if n == MaxVarintLen64:
            return 0, -(n + 1)
        b = int.from_bytes(buf[n:n + 1], sys.byteorder)
        if b < 0x80:
            if n == MaxVarintLen64 - 1 and b > 1:
                return 0, -(n + 1)
            x = x | b << s
            return x, n + 1
        x = x | (b & 0x7f) << s
        s += 7
    return 0, 0


def append_varint(buf: bytearray, x: bytes) -> bytearray:
    varint_ux = struct.unpack('q', x)[0] << 1
    if varint_ux < 0:
        varint_ux = varint_ux ^ -1
    return append_uvarint(buf, struct.pack('Q', varint_ux))


def put_varint(x: bytes) -> Tuple[bytearray, int]:
    varint_ux = struct.unpack('q', x)[0] << 1
    if varint_ux < 0:
        varint_ux = varint_ux ^ -1
    return put_uvarint(struct.pack('Q', varint_ux))


def varint(buf: bytearray) -> Tuple[int, int]:
    uvarint_ux, n = uvarint(buf)
    varint_ux = uvarint_ux >> 1
    if (uvarint_ux & 1) != 0:
        varint_ux = varint_ux ^ -1
    return varint_ux, n


def read_uvarint(r: BytesIO) -> Tuple[int, int]:
    x = 0
    s = 0
    for n in range(MaxVarintLen64):
        b = int.from_bytes(r.read(1), sys.byteorder)
        if b < 0x80:
            if n == MaxVarintLen64 - 1 and b > 1:
                return 0, -(n + 1)
            x = x | b << s
            return x, n + 1
        x = x | (b & 0x7f) << s
        s += 7
    return 0, 0


def read_varint(r: BytesIO) -> Tuple[int, int]:
    uvarint_ux, n = read_uvarint(r)
    varint_ux = uvarint_ux >> 1
    if (uvarint_ux & 1) != 0:
        varint_ux = varint_ux ^ -1
    return varint_ux, n
