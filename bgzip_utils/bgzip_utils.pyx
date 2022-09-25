import io
import zlib
import struct
from math import ceil

from libc.stdlib cimport abort
from libc.string cimport memset
from cython.parallel import prange

from czlib cimport *
from cpython_nogil cimport *


cdef enum:
    BLOCK_BATCH_SIZE = 300
    MAGIC_LENGTH = 4

block_batch_size = int(BLOCK_BATCH_SIZE)  # make BLOCK_BATCH_SIZE accessible in Python

cdef enum bgzip_err:
    BGZIP_CRC_MISMATCH = -8
    BGZIP_ZLIB_INITIALIZATION_ERROR
    BGZIP_BLOCK_SIZE_MISMATCH
    BGZIP_BLOCK_SIZE_NEGATIVE
    BGZIP_ZLIB_ERROR
    BGZIP_MALFORMED_HEADER
    BGZIP_INSUFFICIENT_BYTES
    BGZIP_ERROR
    BGZIP_OK

cdef const unsigned char * MAGIC = "\037\213\010\4"

ctypedef block_header_s BlockHeader
cdef struct block_header_s:
    unsigned char magic[MAGIC_LENGTH]
    unsigned int mod_time
    unsigned char extra_flags
    unsigned char os_type
    unsigned short extra_len

ctypedef block_header_bgzip_subfield_s BlockHeaderBGZipSubfield
cdef struct block_header_bgzip_subfield_s:
    unsigned char id_[2]
    unsigned short length
    unsigned short block_size

ctypedef block_tailer_s BlockTailer
cdef struct block_tailer_s:
    unsigned int crc
    unsigned int inflated_size

ctypedef block_s Block
cdef struct block_s:
    unsigned int deflated_size
    unsigned int inflated_size
    unsigned int crc
    unsigned short block_size
    Bytef * next_in
    unsigned int available_in
    Bytef * next_out
    unsigned int avail_out

class BGZIPException(Exception):
    pass

cdef py_memoryview_to_buffer(object py_memoryview, Bytef ** buf):
    cdef PyObject * obj = <PyObject *>py_memoryview
    if PyMemoryView_Check(obj):
        # TODO: Check buffer is contiguous, has normal stride
        buf[0] = <Bytef *>(<Py_buffer *>PyMemoryView_GET_BUFFER(obj)).buf
        assert NULL != buf
    else:
        raise TypeError("'py_memoryview' must be a memoryview instance.")

cdef bgzip_err inflate_block(Bytef * src, Bytef * dst, int deflated_size, int inflated_size, unsigned int crc) nogil:
    cdef z_stream zst
    cdef int err

    zst.zalloc = NULL
    zst.zfree = NULL
    zst.opaque = NULL
    zst.avail_in = deflated_size
    zst.avail_out = inflated_size
    zst.next_in = src
    zst.next_out = dst

    err = inflateInit2(&zst, -15)
    if Z_OK != err:
        return BGZIP_ZLIB_INITIALIZATION_ERROR
    err = inflate(&zst, Z_FINISH)
    if Z_STREAM_END == err:
        pass
    else:
        return BGZIP_ZLIB_ERROR
    inflateEnd(&zst)

    if inflated_size != zst.total_out:
        return BGZIP_BLOCK_SIZE_MISMATCH

    if crc != crc32(0, src, inflated_size):
        return BGZIP_CRC_MISMATCH

    return BGZIP_OK

    # Difference betwwen `compress` and `deflate`:
    # https://stackoverflow.com/questions/10166122/zlib-differences-between-the-deflate-and-compress-functions

def inflate_parts(list blocks, list dst_parts, int num_threads):
    cdef int i, err, num_parts = 0
    cdef int deflated_size[BLOCK_BATCH_SIZE]
    cdef int inflated_size[BLOCK_BATCH_SIZE]
    cdef unsigned int crc[BLOCK_BATCH_SIZE]
    cdef Bytef * src_bufs[BLOCK_BATCH_SIZE]
    cdef Bytef * dst_bufs[BLOCK_BATCH_SIZE]

    if len(blocks) != len(dst_parts):
        raise ValueError("Number of destination buffers not equal to number of input blocks!")

    num_parts = len(blocks)

    if num_parts > BLOCK_BATCH_SIZE:
        raise Exception(f"Cannot inflate more than {BLOCK_BATCH_SIZE} per call")

    for i in range(num_parts):
        py_memoryview_to_buffer(blocks[i].deflated_data, &src_bufs[i])
        py_memoryview_to_buffer(dst_parts[i], &dst_bufs[i])
        deflated_size[i] = len(blocks[i].deflated_data)
        inflated_size[i] = blocks[i].inflated_size
        crc[i] = blocks[i].crc

    with nogil:
        for i in prange(num_parts, num_threads=num_threads, schedule="dynamic"):
            inflate_block(src_bufs[i], dst_bufs[i], deflated_size[i], inflated_size[i], crc[i])

cdef bgzip_err compress_block(Block * block) nogil:
    cdef z_stream zst
    cdef int err = 0
    cdef BlockHeader * head
    cdef BlockHeaderBGZipSubfield * head_subfield
    cdef BlockTailer * tail
    cdef int wbits = -15
    cdef int mem_level = 8

    head = <BlockHeader *>block.next_out
    block.next_out += sizeof(BlockHeader)

    head_subfield = <BlockHeaderBGZipSubfield *>block.next_out
    block.next_out += sizeof(BlockHeaderBGZipSubfield)

    zst.zalloc = NULL
    zst.zfree = NULL
    zst.opaque = NULL
    zst.next_in = block.next_in
    zst.avail_in = block.available_in
    zst.next_out = block.next_out
    zst.avail_out = 1024 * 1024
    err = deflateInit2(&zst, Z_BEST_COMPRESSION, Z_DEFLATED, wbits, mem_level, Z_DEFAULT_STRATEGY)
    if Z_OK != err:
        return BGZIP_ZLIB_ERROR
    err = deflate(&zst, Z_FINISH)
    if Z_STREAM_END != err:
        return BGZIP_ZLIB_ERROR
    deflateEnd(&zst)

    block.next_out += zst.total_out

    tail = <BlockTailer *>block.next_out

    for i in range(MAGIC_LENGTH):
        head.magic[i] = MAGIC[i]
    head.mod_time = 0
    head.extra_flags = 0
    head.os_type = b"\377"
    head.extra_len = sizeof(BlockHeaderBGZipSubfield)

    head_subfield.id_[0] = b"B"
    head_subfield.id_[1] = b"C"
    head_subfield.length = 2
    head_subfield.block_size = sizeof(BlockHeader) + sizeof(BlockHeaderBGZipSubfield) + zst.total_out + sizeof(BlockTailer) - 1

    tail.crc = crc32(0, block.next_in, block.inflated_size)
    tail.inflated_size = block.inflated_size

    block.block_size = 1 + head_subfield.block_size

    return BGZIP_OK

cdef unsigned int _block_data_inflated_size = 65280
cdef unsigned int _block_metadata_size = sizeof(BlockHeader) + sizeof(BlockHeaderBGZipSubfield) + sizeof(BlockTailer)
block_data_inflated_size = _block_data_inflated_size
block_metadata_size = _block_metadata_size

cdef void _get_buffer(PyObject * obj, Py_buffer * view):
    cdef int err

    err = PyObject_GetBuffer(obj, view, PyBUF_SIMPLE)
    if -1 == err:
        raise Exception()

def deflate_to_buffers(py_input_buff, list py_deflated_buffers, int num_threads):
    """
    Compress the data in `py_input_buff` and write it to `handle`.

    `deflated_buffers` should contain enough buffers to hold the number of blocks compressed. Each
    buffer should hold `_block_data_inflated_size + _block_metadata_size` bytes.
    """
    cdef int i, chunk_size
    cdef unsigned int bytes_available = len(py_input_buff)
    cdef int number_of_chunks = min(ceil(bytes_available / block_data_inflated_size),
                                    len(py_deflated_buffers))
    cdef Block blocks[BLOCK_BATCH_SIZE]
    cdef PyObject * deflated_buffers = <PyObject *>py_deflated_buffers
    cdef PyObject * compressed_chunk

    cdef Py_buffer input_view 
    _get_buffer(<PyObject *>py_input_buff, &input_view)

    with nogil:
        for i in range(number_of_chunks):
            compressed_chunk = <PyObject *>PyList_GetItem(deflated_buffers, i)

            if bytes_available >= _block_data_inflated_size:
                chunk_size = _block_data_inflated_size
            else:
                chunk_size = bytes_available

            bytes_available -= _block_data_inflated_size

            blocks[i].inflated_size = chunk_size
            blocks[i].next_in = <Bytef *>input_view.buf + (i * _block_data_inflated_size)
            blocks[i].available_in = chunk_size
            blocks[i].next_out = <Bytef *>PyByteArray_AS_STRING(compressed_chunk)
            blocks[i].avail_out = _block_data_inflated_size + _block_metadata_size

        for i in prange(number_of_chunks, num_threads=num_threads, schedule="dynamic"):
            if BGZIP_OK != compress_block(&blocks[i]):
                with gil:
                    raise BGZIPException()

    PyBuffer_Release(&input_view)

    return [blocks[i].block_size for i in range(number_of_chunks)]
