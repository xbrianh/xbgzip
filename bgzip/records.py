import struct
from collections import namedtuple
from typing import Any


class InsufficientDataError(Exception):
    pass

class Struct:
    fmt: str
    size: int
    fields: Any

    @classmethod
    def unpack(cls, data: memoryview):
        return cls.fields._make(struct.unpack(cls.fmt, data[:cls.size]))

class BlockHeader(Struct):
    _magic = b"\037\213\010\4"
    _magic_sz = len(_magic)
    fmt = "IccH"
    fields = namedtuple("BlockHeader", "mod_time extra_flags os_type extra_len")  # type: ignore
    size = _magic_sz + struct.calcsize(fmt)

    @classmethod
    def unpack(cls, data: memoryview):
        if len(data) < cls.size:
            raise InsufficientDataError()
        if cls._magic != data[:cls._magic_sz]:
            raise ValueError("Magic bytes not found in header. Is this block gzipped data?")
        res = cls.fields._make(struct.unpack(cls.fmt, data[cls._magic_sz:cls.size]))
        if not res.extra_len:
            raise ValueError("Extra length not found in header. Is this block gzipped data?")
        return res

class BlockHeaderSubfield(Struct):
    _magic = b"BC"
    _magic_sz = len(_magic)
    fmt = "H"
    fields = namedtuple("BlockHeaderSubfield", "length")  # type: ignore
    size = _magic_sz + struct.calcsize(fmt)

    @classmethod
    def unpack(cls, data: memoryview):
        if len(data) < cls.size:
            raise InsufficientDataError()
        if cls._magic != data[:cls._magic_sz]:
            raise ValueError("Magic bytes not found in header. Is this block gzipped data?")
        res = cls.fields._make(struct.unpack(cls.fmt, data[cls._magic_sz:cls.size]))
        if 2 != res.length:
            raise ValueError("Unexpected length found in sub-header. Cannot unpack data")
        return res

class BlockSizeField(Struct):
    fmt = "H"
    fields = namedtuple("BlockSizeField", "length")  # type: ignore
    size = struct.calcsize(fmt)

class BlockTailer(Struct):
    fmt = "II"
    fields = namedtuple("BlockTailer", "crc inflated_size")  # type: ignore
    size = struct.calcsize(fmt)

BZBlock = namedtuple("BZBlock", "size deflated_data inflated_size crc")
