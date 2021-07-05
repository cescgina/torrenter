import unittest
from collections import OrderedDict
from torrenter.bencoding import Encoder

class TestEncoder(unittest.TestCase):
    def testInt(self):
        output = Encoder(123).encode()
        golden = b"i123e"
        self.assertEqual(output, golden)

    def testStr(self):
        output = Encoder("Middle Earth").encode()
        golden = b"12:Middle Earth"
        self.assertEqual(output, golden)

    def testByteStr(self):
        output = Encoder(b"test_str").encode()
        golden = b"8:test_str"
        self.assertEqual(output, golden)

    def testList(self):
        output = Encoder(['spam', 'eggs', 123]).encode()
        golden = b"l4:spam4:eggsi123ee"
        self.assertEqual(output, golden)

    def testDict(self):
        d = OrderedDict()
        d['cow'] = 'moo'
        d['spam'] = 'eggs'
        output = Encoder(d).encode()
        golden = b"d3:cow3:moo4:spam4:eggse"
        self.assertEqual(output, golden)

    def testListWithDict(self):
        d = OrderedDict()
        d['cow'] = 'moo'
        d['spam'] = 'eggs'
        output = Encoder(['spam', 'eggs', 123, d]).encode()
        golden = b"l4:spam4:eggsi123ed3:cow3:moo4:spam4:eggsee"
        self.assertEqual(output, golden)

    def testDictWithList(self):
        d = OrderedDict()
        d['cow'] = 'moo'
        d['spam'] = 'eggs'
        d['t'] = ['spam', 'eggs', 123]
        output = Encoder(d).encode()
        golden = b"d3:cow3:moo4:spam4:eggs1:tl4:spam4:eggsi123eee"
        self.assertEqual(output, golden)
