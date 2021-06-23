import unittest
from collections import OrderedDict
from torrenter.bencoding import Decoder

class TestDecoder(unittest.TestCase):
    def testInt(self):
        output = Decoder(b"i123e").decode()
        golden = 123
        self.assertEqual(output, golden)

    def testStr(self):
        output = Decoder(b"12:Middle Earth").decode()
        golden = b"Middle Earth"
        self.assertEqual(output, golden)

    def testList(self):
        output = Decoder(b"l4:spam4:eggsi123ee").decode()
        golden = [b'spam', b'eggs', 123]
        self.assertEqual(output, golden)

    def testDict(self):
        golden = OrderedDict()
        golden[b'cow'] = b'moo'
        golden[b'spam'] = b'eggs'
        output = Decoder(b"d3:cow3:moo4:spam4:eggse").decode()
        self.assertEqual(output, golden)

    def testListWithDict(self):
        d = OrderedDict()
        d[b'cow'] = b'moo'
        d[b'spam'] = b'eggs'
        output = Decoder(b"l4:spam4:eggsi123ed3:cow3:moo4:spam4:eggsee").decode()
        golden = [b'spam', b'eggs', 123, d]
        self.assertEqual(output, golden)

    def testDictWithList(self):
        golden = OrderedDict()
        golden[b'cow'] = b'moo'
        golden[b'spam'] = b'eggs'
        golden[b't'] = [b'spam', b'eggs', 123]
        output = Decoder(b"d3:cow3:moo4:spam4:eggs1:tl4:spam4:eggsi123eee").decode()
        self.assertEqual(output, golden)
