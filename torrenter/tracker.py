import socket
import random
from struct import unpack
from urllib.parse import urlencode
import aiohttp
from torrenter import bencoding

class TrackerResponse:
    def __init__(self, response: dict):
        self.response = response

    @property
    def failure(self):
        """
            If this response was a failed response, this is the error message to
            why the tracker request failed.
            If no error occurred this will be None
        """
        if b"failure reason" in self.response:
            return self.response[b"failure reason"].decode("utf-8")

    @property
    def interval(self) -> int:
        """
            Interval in seconds that the client should wait between sending
            periodic requests to the tracker
        """
        return self.response.get(b"interval", 0)

    @property
    def complete(self) -> int:
        """
            Number of peers with the entire file, i.e. seeders
        """
        return self.response.get(b"complete", 0)

    @property
    def incomplete(self) -> int:
        """
            Number of non-seeder peers, i.e. leechers
        """
        return self.response.get(b"incomplete", 0)

    @property
    def peers(self):
        """
            A list of tuples for each peer structured as (ip, port)
        """
        # The BitTorrent specification specifies two types of responses. One
        # where the peers field is a list of dictionaries and one where all
        # the peers are encoded in a single string
        peers = self.response[b"peers"]
        if type(peers) == list:
            raise NotImplementedError()
        else:
	    # Split the string in pieces of length 6 bytes, where the first
            # 4 characters is the IP the last 2 is the TCP port.
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]

            return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:])) for p in peers]

    def __bool__(self):
        return not self.failure

    def __str__(self):
        return "incomplete: {self.incomplete}\n" \
                "complete: {self.complete}\n" \
                "interval: {self.interval}\n" \
                "peers: {', '.join([x for (x, _) in self.peers])}"


class Tracker:
    def __init__(self, torrent):
        self.id = _calculate_peer_id()
        self.torrent = torrent
        # self.http_client = aiohttp.ClientSession()

    async def fetch_request(self, client, params):
        url = self.torrent.announce + "?" + urlencode(params)
        async with client.get(url) as response:
            if not response.status == 200:
                raise ConnectionError("Unable to connect to tracker")
            data = await response.read()
            return TrackerResponse(bencoding.Decoder(data).decode())

    async def connect(self, first: bool=None, uploaded: int=0, downloaded: int=0):
        params = {"info_hash": self.torrent.info_hash, "peer_id": self.id,
        	"uploaded": uploaded, "downloaded": downloaded, 
        	"left": self.torrent.total_size-downloaded, "port": 6889,
        	"compact": 1}
        if first:
            params["event"] = "started"
        async with aiohttp.ClientSession() as client:
            return await self.fetch_request(client, params)

    def raise_for_error(self, tracker_response):
        """
        A (hacky) fix to detect errors by tracker even when the response has a status code of 200  
        """
        try:
            # a tracker response containing an error will have a utf-8 message only.
            # see: https://wiki.theory.org/index.php/BitTorrentSpecification#Tracker_Response
            message = tracker_response.decode("utf-8")
            if "failure" in message:
                raise ConnectionError('Unable to connect to tracker: {}'.format(message))

        # a successful tracker response will have non-uncicode data, so it's a safe to bet ignore this exception.
        except UnicodeDecodeError:
            pass

def _calculate_peer_id():
    """
    Calculate and return a unique Peer ID.
    The `peer id` is a 20 byte long identifier. This implementation use the
    Azureus style `-PC1000-<random-characters>`.
    Read more:
        https://wiki.theory.org/BitTorrentSpecification#peer_id
    """
    return '-PC0001-' + ''.join([str(random.randint(0, 9)) for _ in range(12)])

def _decode_port(port):
    """
        Converts a 32-bit packed binary port number to int
    """
    # convert from C style big-endian encoded as unsigned short
    return unpack(">H", port)[0]
