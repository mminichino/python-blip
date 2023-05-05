#!/usr/bin/env python3

import os
import sys

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)
sys.path.append(current)

from pyblip.varint import *


def test_uvarint_1():
    numbers = [
        1,
        2,
        999,
        1000,
        1024,
    ]

    for n in numbers:
        buffer, _ = put_uvarint(uint64(n))
        for b in buffer:
            print("{0:b} ".format(b), end='')
        print("")
        result, _ = uvarint(buffer)
        print(f"{n} =  {result}")

    u_numbers = [
        -1,
        -2,
        -999,
        -1000,
        -1024,
    ]

    for n in u_numbers:
        buffer, _ = put_varint(int64(n))
        for b in buffer:
            print("{0:b} ".format(b), end='')
        print("")
        result, _ = varint(buffer)
        print(f"{n} =  {result}")
