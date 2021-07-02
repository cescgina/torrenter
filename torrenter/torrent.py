from hashlib import sha1
from collections import namedtuple
from torrenter.bencoding import Decoder, Encoder

TorrentFile = namedtuple("TorrentFile", ["name", "length"])

class Torrent:
    def __init__(self, torrent_file):
        self.torrent_file = torrent_file
        self._data = self._read_torrent()
        self.files = []
        self.info_hash = sha1(Encoder(self._data[b"info"]).encode()).digest()
        self._identify_files()

    def __str__(self):
        filename = self._data[b'info'][b'name']
        file_len = self._data[b'info'][b'length']
        announce_url = self._data[b'announce']

        return f"Filename: {filename}\nFile length: {file_len}\n" \
                "Announce URL: {announce_url}\nHash: {self.info_hash}"

    def _read_torrent(self):
        with open(self.torrent_file, "rb") as f:
            return Decoder(f.read()).decode()

    def _identify_files(self):
        files = self._data[b"info"]
        if b"files" in files:
            # multi-file torrent
            files_dict = files[b"files"]
        else:
            files_dict = [files]

        for f in files_dict:
            name = f[b"name"].decode("utf-8")
            length = f[b"length"]
            self.files.append(TorrentFile(name, length))

    @property
    def announce(self) -> str:
        return self._data[b"announce"].decode("utf-8")

    @property
    def piece_length(self) -> int:
        return self._data[b"info"][b"piece length"]

    @property
    def total_size(self) -> int:
        return sum([x.length for x in self.files])

    @property
    def pieces(self):
        # The info pieces is a string representing all pieces SHA1 hashes
        # (each 20 bytes long). Read that data and slice it up into the
        # actual pieces
        data = self._data[b"info"][b"pieces"]
        pieces = []
        offset = 0
        length = len(data)
        while offset < length:
            pieces.append(data[offset:offset + 20])
            offset += 20
        return pieces

    @property
    def output_file(self):
        return self._data[b"info"][b"name"].decode("utf-8")

