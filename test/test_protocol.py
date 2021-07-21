import unittest
import bitstring

from torrenter.protocol import PeerStreamIterator, Handshake, Have, Request, Piece
from torrenter.protocol import Interested, Cancel, KeepAlive, BitField


class PeerStreamIteratorTests(unittest.TestCase):
    def test_parse_empty_buffer(self):
        iterator = PeerStreamIterator(None, None)
        iterator.buffer = ""
        self.assertIsNone(iterator.parse())

    def test_parse_keep_alive(self):
        iterator = PeerStreamIterator(None, None)
        iterator.buffer = b"\x00\x00\x00\x00"
        msg = iterator.parse()
        self.assertEqual(msg.encode(), KeepAlive().encode())

    def test_parse_multiple_messages(self):
        iterator = PeerStreamIterator(None, None)
        iterator.buffer = b"\x00\x00\x00\x00\x00\x00\x00\x05\x04\x00\x00\x00!\x00\x00\x00\x0b\x07\x00\x00\x00\x00\x00\x00\x00\x00ok"
        msg = iterator.parse()
        self.assertEqual(msg.encode(), KeepAlive().encode())
        msg = iterator.parse()
        self.assertEqual(msg.encode(), Have(33).encode())
        msg = iterator.parse()
        self.assertEqual(msg.encode(), Piece(0, 0, b"ok").encode())
        self.assertEqual(iterator.buffer, b"")
        

class HandshakeTest(unittest.TestCase):
    def test_construction(self):
        handshake = Handshake(
                info_hash=b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01",
                peer_id=b"-qB3200-iTiX3rvfzMpr")
        self.assertEqual(handshake.encode(), 
                b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00"
                b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01"
                b"-qB3200-iTiX3rvfzMpr")

    def test_parse(self):
        handshake = Handshake.decode(
                b"\x13BitTorrent protocol\x00\x00\x00\x00\x00\x00\x00\x00"
                b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01"
                b"-qB3200-iTiX3rvfzMpr")
        self.assertEqual(handshake.info_hash, b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01")
        self.assertEqual(handshake.peer_id, b"-qB3200-iTiX3rvfzMpr")

class HaveMessageTests(unittest.TestCase):
    def test_can_construct_have(self):
        have = Have(33)
        self.assertEqual(have.encode(), b"\x00\x00\x00\x05\x04\x00\x00\x00!")

    def test_can_parsehave(self):
        have = Have.decode(b"\x00\x00\x00\x05\x04\x00\x00\x00!")
        self.assertEqual(33, have.index)

class RequestTest(unittest.TestCase):
    def test_can_construct_request(self):
        request = Request(0, 2)

        self.assertEqual(request.encode(),
                b"\x00\x00\x00\r\x06\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00@\x00")

    def test_can_parse_request(self):
        request = Request.decode(b"\x00\x00\x00\r\x06\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00@\x00")

        self.assertEqual(request.index, 0)
        self.assertEqual(request.begin, 2)

class PieceTests(unittest.TestCase):
    def test_can_construct_piece(self):
        piece = Piece(0, 0, b"ok")

        self.assertEqual(piece.encode(),
                b"\x00\x00\x00\x0b\x07\x00\x00\x00\x00\x00\x00\x00\x00ok")

    def test_can_parse_piece(self):
        piece = Piece.decode(
                b"\x00\x00\x00\x0b\x07\x00\x00\x00\x00\x00\x00\x00\x00ok")
        self.assertEqual(piece.index, 0)
        self.assertEqual(piece.begin, 0)
        self.assertEqual(piece.block, b"ok")

class InterestedTests(unittest.TestCase):
    def test_can_encode(self):
        message = Interested()
        raw = message.encode()

        self.assertEqual(raw, b"\x00\x00\x00\x01\x02")


class CancelTests(unittest.TestCase):
    def test_can_encode(self):
        message = Cancel(0, 2)

        self.assertEqual(message.encode(),
                            b"\x00\x00\x00\r\x08\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00@\x00")

    def test_can_decode(self):
        message = Cancel.decode(b"\x00\x00\x00\r\x08\x00\x00\x00\x00\x00\x00\x00\x02\x00\x00@\x00")

        self.assertEqual(message.index, 0)
        self.assertEqual(message.begin, 2)

class BitFieldTests(unittest.TestCase):
    def test_can_encode(self):
        bitfield = bitstring.BitArray([1, 0, 0, 0, 1, 0, 0 ,0])
        message = BitField(bitfield.bytes)
        raw = b"\x00\x00\x00\x02\x05\x88"
        self.assertEqual(message.encode(), raw)

    def test_can_encode_long(self):
        bitfield = bitstring.BitArray([1 for _ in range(1624)])
        message = BitField(bitfield.bytes)
        raw = b'\x00\x00\x00\xcc\x05\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'
        self.assertEqual(message.encode(), raw)

    def test_can_decode(self):
        message = BitField.decode(b"\x00\x00\x00\x02\x05\x88")
        self.assertEqual(message.bitfield, bitstring.BitArray([1, 0, 0, 0, 1, 0, 0 ,0]))

    def test_can_decode_long(self):
        raw = b'\x00\x00\x00\xcc\x05\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff'
        message = BitField.decode(raw)
        bits = [1 for _ in range(1624)]
        self.assertEqual(message.bitfield, bitstring.BitArray(bits))
