#!/usr/bin/env python
import gzip

from bgzip import bgzip_utils as bgu  # type: ignore
from bgzip import records


class IncompleteBlockError(Exception):
    pass

def _read_block(offset: int, data: memoryview):
    block_offset = offset

    block_header = records.BlockHeader.unpack(data[offset:])
    offset += records.BlockHeader.size

    records.BlockHeaderSubfield.unpack(data[offset:])
    offset += records.BlockHeaderSubfield.size

    block_size = records.BlockSizeField.unpack(data[offset:])
    offset += records.BlockSizeField.size

    deflated_size = (1
                     + block_size.length
                     - records.BlockHeader.size
                     - block_header.extra_len
                     - records.BlockTailer.size)

    deflated_data = data[offset: offset + deflated_size]
    offset += deflated_size

    block_tailer = records.BlockTailer.unpack(data[offset:])
    offset += records.BlockTailer.size

    block = records.BZBlock(
        offset - block_offset,  # block size
        block_offset,
        deflated_data,
        block_tailer.inflated_size,
        block_tailer.crc
    )

    return block

def read_blocks(data: memoryview):
    offset = 0
    while True:
        try:
            block = _read_block(offset, data)
            offset += block.size
            yield block
        except records.InsufficientDataError:
            break

def inflate_data(data: memoryview, dst_buf: memoryview):
    blocks = [b for b in read_blocks(data)]

    dst_parts = list()
    total = 0
    for b in blocks:
        if total + b.inflated_size > len(dst_buf):
            break
        dst_parts.append(dst_buf[total: total + b.inflated_size])
        total += b.inflated_size
    bgu.inflate_parts(blocks[:len(dst_parts)], dst_parts, 1)
    blocks = blocks[len(dst_parts):]
    return total

filepath = "tests/fixtures/partial.vcf.gz"
with open(filepath, "rb") as fh:
    thedata = fh.read()
    expected_inflated_data = gzip.decompress(thedata)

buf = bytearray(1024 * 1024 * 20)
bytes_read = inflate_data(memoryview(thedata), memoryview(buf))
buf = buf[:bytes_read]
assert expected_inflated_data == buf
