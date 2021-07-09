import unittest

from . import no_logging
from torrenter.client import Piece, Block


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
            p.block_recieved(123, b"")  # should not throw

    def test_reset_block(self):
        blocks = [Block(0, offset, length=10) for offset in range(0, 100, 10)]
        p = Piece(None, 0, blocks)

        p.block_recieved(10, b"")

        self.assertEqual(1, len([b for b in p.blocks if b.status is
            Block.Retrieved]))
        self.assertEqual(9, len([b for b in p.blocks if b.status is
            Block.Missing]))