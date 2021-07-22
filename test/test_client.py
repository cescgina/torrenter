import unittest

from bitstring import BitArray

from . import no_logging
from torrenter.client import Piece, Block, PieceManager, REQUEST_SIZE
from torrenter.torrent import Torrent


class PieceTests(unittest.TestCase):
    def test_empty_piece(self):
        p = Piece(None, 0, blocks=[])
        self.assertIsNone(p.next_request())

    def test_request_ok(self):
        blocks = [Block(0, offset, length=10) for offset in range(0, 100, 10)]
        p = Piece(None, 0, blocks)

        block = p.next_request()
        missing_blocks = [b for b in p.blocks if b.status is Block.Missing]
        pending_blocks = [b for b in p.blocks if b.status is Block.Pending]
        self.assertEqual(1, len(pending_blocks))
        self.assertEqual(9, len(missing_blocks))
        self.assertEqual(block, pending_blocks[0])

    def test_reset_mising_block(self):
        p = Piece(None, 0, blocks=[])
        with no_logging:
            p.block_received(123, b"")  # should not throw

    def test_reset_block(self):
        blocks = [Block(0, offset, length=10) for offset in range(0, 100, 10)]
        p = Piece(None, 0, blocks)

        p.block_received(10, b"")

        self.assertEqual(1, len([b for b in p.blocks if b.status is
            Block.Retrieved]))
        self.assertEqual(9, len([b for b in p.blocks if b.status is
            Block.Missing]))

class PieceManagerTests(unittest.TestCase):
    def setUp(self):
        self.piece_manager = PieceManager(Torrent("test/data/ubuntu-16.04-desktop-amd64.iso.torrent"))

    def test_bitfield(self):
        self.piece_manager.total_pieces = 5
        p1 = Piece(None, 0, [])
        p2 = Piece(None, 4, [])
        self.piece_manager.have_pieces = [p1, p2]
        self.assertEqual(b"\x88", self.piece_manager.bitfield)

    def test_uploaded(self):
        self.piece_manager.uploaded_bytes(5)
        self.piece_manager.uploaded_bytes(10)
        self.assertEqual(self.piece_manager.bytes_uploaded, 15)

    def test_peers(self):
        bits = BitArray([1, 0, 0])
        peer_id = b"test"
        self.piece_manager.add_peer(peer_id, bits)
        self.assertTrue(self.piece_manager.peers[peer_id][0])
        self.assertFalse(self.piece_manager.peers[peer_id][1])
        self.assertFalse(self.piece_manager.peers[peer_id][2])

    def test_blocks_torrent_size_multiple_request_size(self):
        torrent = Torrent("test/data/debian-edu-10.10.0-amd64-netinst.iso.torrent")
        piece_manager = PieceManager(torrent)
        self.assertTrue(len(piece_manager.missing_pieces), 1624)
        self.assertTrue(piece_manager.missing_pieces[-1].index, 1623)
        self.assertTrue(len(piece_manager.missing_pieces[-1].blocks), 16)
        for b in piece_manager.missing_pieces[-1].blocks:
            self.assertTrue(b.length, REQUEST_SIZE)
