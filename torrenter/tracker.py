import time
import socket
import random
import logging
from struct import unpack, pack
from urllib.parse import urlencode, urlparse
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
            return [(p[b"ip"], p[b"port"], p[b"peer_id"]) for p in peers]
        else:
	    # Split the string in pieces of length 6 bytes, where the first
            # 4 characters is the IP the last 2 is the TCP port.
            peers = [peers[i:i+6] for i in range(0, len(peers), 6)]

            return [(socket.inet_ntoa(p[:4]), _decode_port(p[4:]), b"") for p in peers]

    def __bool__(self):
        return not self.failure

    def __str__(self):
        return "incomplete: {self.incomplete}\n" \
                "complete: {self.complete}\n" \
                "interval: {self.interval}\n" \
                "peers: {', '.join([x for (x, _) in self.peers])}"

class TrackerResponseUDP(TrackerResponse):
    def __init__(self, data, has_error):
        response = {}
        if has_error:
            response["failure reason"] = data
        else:
            interval, incomplete, complete = unpack(">III", data[:12])
            response[b"interval"] = interval
            response[b"complete"] = complete
            response[b"incomplete"] = incomplete
            response[b"peers"] = data[12:]
        super().__init__(response)

class UDPConnection:
    def __init__(self, url, params):
        self.params = params
        url_parsed = urlparse(url)
        self.address_tracker = url_parsed.hostname
        self.port_tracker = url_parsed.port
        self.transaction_id = _get_random_32_integer()
        self.attempts = 0
        self.start = time.time()
        self.connection_id = None
        self.udp_magic_number = 0x41727101980

    def has_timed_out(self):
        """
            Check if the attempt has gone beyond 15 seconds
        """
        return (time.time()-self.start) > 15

    def connect(self):
        # each attempt has a timeout of 15, after a minute we stop trying
        while self.attempts < 4:
            self.start = time.time()
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp_socket:
                udp_socket.settimeout(15)
                logging.debug(f"Trying to connect to udp tracker {self.address_tracker}:{self.port_tracker}, attempt {self.attempts+1}/4")
                udp_socket.connect((self.address_tracker, self.port_tracker))
                while True and not self.has_timed_out():
                    try:
                        self.send_connect_request(udp_socket)
                        action, transaction_id_recv, extra_data = self.retrieve_connect_response(udp_socket)
                    except socket.timeout:
                        logging.warning("Connection to tracker timed out!")
                        break
                    if action == 3:
                        return TrackerResponseUDP(extra_data, True)
                    if transaction_id_recv == self.transaction_id and action == 0:
                        logging.debug("Got connect response, announcing...")
                        break
                while True and not self.has_timed_out():
                    try:
                        self.send_announce_request(udp_socket)
                        action, transaction_id_recv, data = self.retrieve_announce_response(udp_socket)
                    except socket.timeout:
                        logging.warning("Connection to tracker timed out!")
                        break
                    if action in (1, 3) and transaction_id_recv == self.transaction_id:
                        logging.debug("Announce finished")
                        return TrackerResponseUDP(data, action==3)
                self.attempts += 1
        raise ConnectionError("Unable to connect to tracker")

    def send_connect_request(self, socket_conn):
        """
            Send a connect request to the tracker

            :param socket_conn: Socket to the tracker
        """
        msg = pack(">QII", self.udp_magic_number, 0, self.transaction_id)
        socket_conn.send(msg)

    def retrieve_connect_response(self, socket_conn):
        """
            Retrieve a connect response from the tracker

            :param socket_conn: Socket to the tracker
        """
        data = socket_conn.recv(16)
        if len(data) < 16:
            return None, None, None
        action, transaction_id_recv = unpack(">II", data[:8])
        if action == 3:
            # error
            data_extra = data[8:]
        else:
            data_extra = None
            self.connection_id = unpack(">Q", data[8:])[0]
        return action, transaction_id_recv, data_extra

    def send_announce_request(self, socket_conn):
        """
            Send announce request to the tracker

            :param socket_conn: Socket to the tracker
        """
        self.transaction_id = _get_random_32_integer()
        key = _get_random_32_integer()
        info_hash = self.params["info_hash"]
        peer_id = self.params["peer_id"].encode()
        left = self.params["left"]
        listen_port = self.params["port"]
        downloaded = self.params["downloaded"]
        uploaded = self.params["uploaded"]
        left = self.params["left"]
        num_want = -1
        event = ip_address = 0
        msg = pack(">QII20s20sQQQIIIiH", self.connection_id, 1,
                   self.transaction_id, info_hash, peer_id,
                   downloaded, left, uploaded, event, ip_address,
                   key, num_want, listen_port)
        socket_conn.send(msg)

    def retrieve_announce_response(self, socket_conn):
        """
            Retrieve an announce response from tracker

            :param socket_conn: Socket to the tracker
        """
        data = socket_conn.recv(1024)
        if len(data) < 20:
            return None, None, None
        action, transaction_id_recv = unpack(">II", data[:8])
        return action, transaction_id_recv, data[8:]



class Tracker:
    def __init__(self, torrent):
        self.id = _calculate_peer_id()
        self.torrent = torrent

    async def fetch_request(self, client, params):
        url = self.torrent.announce + "?" + urlencode(params)
        async with client.get(url) as response:
            if not response.status == 200:
                raise ConnectionError("Unable to connect to tracker")
            data = await response.read()
            return TrackerResponse(bencoding.Decoder(data).decode())

    def fetch_request_udp(self, params):
        return UDPConnection(self.torrent.announce, params).connect()

    async def connect(self, first: bool=None, uploaded: int=0, downloaded: int=0):
        params = {"info_hash": self.torrent.info_hash, "peer_id": self.id,
        	"uploaded": uploaded, "downloaded": downloaded, 
        	"left": self.torrent.total_size-downloaded, "port": 6889,
        	"compact": 1}
        if first:
            params["event"] = "started"
        if self.torrent.announce.startswith("udp"):
            # use udp protocol
            return self.fetch_request_udp(params)
        else:
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

def _get_random_32_integer():
    """
        Generate a random 32 integer, required for udp announce protocol
    """
    return random.randint(0, 0xffffffff)
