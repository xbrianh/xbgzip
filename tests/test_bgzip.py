#!/usr/bin/env python
import io
import os
import sys
import gzip
import random
import unittest
from tempfile import TemporaryDirectory
from random import randint
from typing import Any, Generator, List, Sequence

pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))  # noqa
sys.path.insert(0, pkg_root)  # noqa

import xbgzip


class TestBGZipReader(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with gzip.GzipFile(fileobj=raw) as fh:
                cls.expected_data = fh.read()

    def test_read(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with xbgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                data = bytearray()
                while True:
                    d = fh.read(randint(1024 * 1, 1024 * 1024 * 1024))
                    if not d:
                        break
                    data.extend(d)
                    d.release()
        self.assertEqual(self.expected_data, data)

    def test_empty(self):
        with xbgzip.BGZipReader(io.BytesIO()) as fh:
            d = fh.read(1024)
            self.assertEqual(0, len(d))

    def test_read_all(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            with xbgzip.BGZipReader(raw) as fh:
                data = fh.read()
        self.assertEqual(data, self.expected_data)

    def test_read_into(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            data = bytearray()
            with xbgzip.BGZipReader(raw) as fh:
                while True:
                    d = fh.read(30 * 1024 * 1024)
                    if not d:
                        break
                    data.extend(d)
                    d.release()
        self.assertEqual(self.expected_data, data)

    def test_iter(self):
        with self.subTest("iter byte lines"):
            data = b""
            with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                with xbgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                    for line in fh:
                        data += line
            self.assertEqual(self.expected_data, data)

        with self.subTest("iter text lines"):
            content = ""
            with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
                with xbgzip.BGZipReader(raw, 1024 * 1024 * 1) as fh:
                    with io.TextIOWrapper(fh, "utf-8") as handle:
                        for line in handle:
                            content += line
            self.assertEqual(self.expected_data.decode("utf-8"), content)

    def test_inflate_chunks(self):
        size = (2 * xbgzip.bgu.block_batch_size + 1) * xbgzip.bgu.block_data_inflated_size
        expected_data = os.urandom(size)
        inflate_buf = memoryview(bytearray(30 * 1024 * 1024))

        data = memoryview(expected_data)
        deflated_blocks = list()
        deflater = xbgzip.Deflater()
        while data:
            bytes_deflated, blocks = deflater.deflate(data)
            deflated_blocks.extend([bytes(b) for b in blocks])
            data = data[bytes_deflated:]

        def _test_inflate_chunks(remaining_chunks: List[memoryview], atomic: bool=False):
            remaining_chunks = remaining_chunks.copy()
            reinflated_data = b""
            while remaining_chunks:
                inflate_info = xbgzip.inflate_chunks(remaining_chunks, inflate_buf, atomic=atomic)
                self.assertGreater(inflate_info['bytes_inflated'], 0)
                remaining_chunks = inflate_info['remaining_chunks']
                reinflated_data += b"".join(inflate_info['blocks'])
            self.assertEqual(expected_data, reinflated_data)

        with self.subTest("all blocks"):
            _test_inflate_chunks([memoryview(bytes(b)) for b in deflated_blocks])

        with self.subTest("chunked blocks"):
            _test_inflate_chunks([memoryview(b"".join(chunk))
                                  for chunk in _randomly_chunked(deflated_blocks)])

        with self.subTest("initial large chunk"):
            _test_inflate_chunks([memoryview(b"".join(deflated_blocks[:-1])),
                                  memoryview(deflated_blocks[-1])])

        with self.subTest("trailing large chunk"):
            _test_inflate_chunks([memoryview(deflated_blocks[0]),
                                  memoryview(b"".join(deflated_blocks[1:]))])

        with self.subTest("leading large chunk atomic"):
            inflate_buf = memoryview(bytearray(200 * 1024))
            chunks = [memoryview(b"".join(deflated_blocks[:-1])), memoryview(deflated_blocks[-1])]
            inflate_info = xbgzip.inflate_chunks(chunks, inflate_buf, atomic=True)
            self.assertEqual(inflate_info['remaining_chunks'], chunks)

        with self.subTest("trailing large chunk atomic"):
            inflate_buf = memoryview(bytearray(200 * 1024))
            chunks = [memoryview(deflated_blocks[0]), memoryview(b"".join(deflated_blocks[1:]))]
            inflate_info = xbgzip.inflate_chunks(chunks, inflate_buf, atomic=True)
            self.assertEqual(inflate_info['remaining_chunks'][0], chunks[1])

        with self.subTest("small inflate buf"):
            inflate_buf = memoryview(bytearray(200 * 1024))
            _test_inflate_chunks([memoryview(b"".join(chunk))
                                  for chunk in _randomly_chunked(deflated_blocks)])

        with self.subTest("buf too small to inflate anything"):
            inflate_buf = memoryview(bytearray(1))
            chunks = [memoryview(b"".join(deflated_blocks))]
            inflate_info = xbgzip.inflate_chunks(chunks, inflate_buf)
            self.assertEqual(0, inflate_info['bytes_read'])
            self.assertEqual(0, inflate_info['bytes_inflated'])
            self.assertEqual(chunks, inflate_info['remaining_chunks'])
            self.assertEqual(list(), inflate_info['block_sizes'])
            self.assertEqual(list(), inflate_info['blocks_per_chunk'])
            self.assertEqual(list(), inflate_info['blocks'])

        with self.subTest("passing in non-memoryview buffers should raise"):
            with self.assertRaises(TypeError):
                xbgzip.inflate_chunks([b"asfd"], inflate_buf)

    def test_inflate_streamed_chunk(self):
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            chunk = raw.read()
        with gzip.GzipFile(fileobj=io.BytesIO(chunk)) as fh:
            expected_data = fh.read()
        inflate_buf = memoryview(bytearray(1024 * 1024 * 50))
        input_buf, data = bytes(), bytes()
        with open("tests/fixtures/partial.vcf.gz", "rb") as raw:
            while True:
                input_buf += raw.read(random.randint(0, 100 * 1024))
                if not input_buf:
                    break
                inflate_info = xbgzip.inflate_chunks([memoryview(input_buf)], inflate_buf)
                input_buf = b"".join(inflate_info['remaining_chunks'])
                data += b"".join(inflate_info['blocks'])
        self.assertEqual(expected_data, data)

def _randomly_chunked(items: Sequence[Any]) -> Generator[Sequence[Any], None, None]:
    items = [i for i in items]
    while items:
        chunk_size = random.randint(1, 20)
        yield items[:chunk_size]
        items = items[chunk_size:]

class TestBGZipWriter(unittest.TestCase):
    def test_gen_buffers(self):
        xbgzip.Deflater._gen_buffers(xbgzip.bgu.block_batch_size)
        xbgzip.Deflater._gen_buffers(1)

        for num_bufs in [0, xbgzip.bgu.block_batch_size + 1]:
            with self.assertRaises(ValueError):
                xbgzip.Deflater._gen_buffers(num_bufs)

    def test_write(self):
        inflated_data = os.urandom(1024 * 1024 * 50)
        deflater = xbgzip.Deflater()
        deflated_with_buffers = bytes()
        data = memoryview(bytes(inflated_data))
        while data:
            bytes_deflated, deflated_blocks = deflater.deflate(data)
            data = data[bytes_deflated:]
            deflated_with_buffers += b"".join(deflated_blocks)
        deflated_with_buffers += xbgzip.xbgzip_eof

        fh_out = io.BytesIO()
        with xbgzip.BGZipWriter(fh_out) as writer:
            n = 987345
            writer.write(inflated_data[:n])
            writer.write(inflated_data[n:])
        deflated_with_writer = fh_out.getvalue()

        self.assertEqual(deflated_with_buffers, deflated_with_writer)

        fh_out.seek(0)
        with gzip.GzipFile(fileobj=fh_out) as fh:
            reinflated_data = fh.read()

        self.assertEqual(inflated_data, reinflated_data)
        self.assertTrue(deflated_with_writer.endswith(xbgzip.xbgzip_eof))

    def test_write_random_data(self):
        inflated_data = os.urandom(1024 * 1024)
        with xbgzip.BGZipWriter(io.BytesIO()) as writer:
            writer.write(inflated_data)

    def test_pathalogical_write(self):
        fh = io.BytesIO()
        with xbgzip.BGZipWriter(fh):
            fh.write(b"")

    def test_large_write(self):
        """Force write to use several batch calls to xbgzip_utils."""
        fh_out = io.BytesIO()
        with xbgzip.BGZipWriter(fh_out) as writer:
            number_of_blocks = 2 * xbgzip.bgu.block_batch_size + 1
            size = number_of_blocks * xbgzip.bgu.block_data_inflated_size
            writer.write(bytearray(size))

class TestOpenUtil(unittest.TestCase):
    def test_open(self):
        with TemporaryDirectory() as dirname:
            filepath = os.path.join(dirname, "blah")
            expected_data = os.urandom(2024)
            with xbgzip.xbgz_open(filepath, "w") as fh:
                fh.write(expected_data)

            with xbgzip.xbgz_open(filepath, "r") as fh:
                data = bytes()
                while True:
                    new_data = fh.read()
                    if not new_data:
                        break
                    data += new_data
            self.assertEqual(expected_data, data)

if __name__ == '__main__':
    unittest.main()
