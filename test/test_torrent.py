import unittest

from torrenter.torrent import Torrent

class UbuntuTorrentTests(unittest.TestCase):
    def setUp(self):
        self.t = Torrent("test/data/ubuntu-16.04-desktop-amd64.iso.torrent")
    
    def test_instantiate(self):
        self.assertIsNotNone(self.t)

    def test_is_single_file(self):
        self.assertFalse(self.t.multi_file)

    def test_announce(self):
        self.assertEqual("http://torrent.ubuntu.com:6969/announce",
                self.t.announce)

    def test_piece_length(self):
        self.assertEqual(524288, self.t.piece_length)

    def test_file(self):
        self.assertEqual(1, len(self.t.files))
        self.assertEqual("ubuntu-16.04-desktop-amd64.iso", self.t.files[0].name)
        self.assertEqual(1485881344, self.t.files[0].length)

    def test_hash_value(self):

	# hexdigest of the SHA1 '4344503b7e797ebf31582327a5baae35b11bda01',
        self.assertEqual(b"CDP;~y~\xbf1X#'\xa5\xba\xae5\xb1\x1b\xda\x01", self.t.info_hash)

    def test_total_size(self):
        self.assertEqual(1485881344, self.t.total_size)

    def test_pieces(self):
        self.assertEqual(2835, len(self.t.pieces))

class DebianTorrentTests(unittest.TestCase):
    def setUp(self):
        self.t = Torrent("test/data/debian-edu-10.10.0-amd64-netinst.iso.torrent")

    def test_instantiate(self):
        self.assertIsNotNone(self.t)

    def test_is_single_file(self):
        self.assertFalse(self.t.multi_file)

    def test_announce(self):
        self.assertEqual("http://bttracker.debian.org:6969/announce",
                self.t.announce)

    def test_hash_value(self):
        self.assertEqual(b"%ZGM:\xd2!\xa2\xc15\x8c\x8e\xd0\xe4`\x7fu\xc7P\xc5", self.t.info_hash)
        
    def test_piece_length(self):
        self.assertEqual(262144, self.t.piece_length)

    def test_file(self):
        self.assertEqual(1, len(self.t.files))
        self.assertEqual("debian-edu-10.10.0-amd64-netinst.iso", self.t.files[0].name)
        self.assertEqual(425721856, self.t.files[0].length)

    def test_total_size(self):
        self.assertEqual(425721856, self.t.total_size)

    def test_pieces(self):
        self.assertEqual(1624, len(self.t.pieces))

class SXSWTorrentTests(unittest.TestCase):
    """
        Represents a multi_file torrent which is not supported
    """
    def test_instaniate(self):
        t = Torrent("test/data/SXSW_2016_Showcasing_Artists_Part1.torrent")
        self.assertTrue(t.multi_file)
