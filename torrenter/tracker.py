import time
import socket
import random
import logging
import asyncio
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

class UDPTrackerProtocol(asyncio.Protocol):
    def __init__(self, connection):
        self.connection = connection
        self.received_msg = None
        self.connection_lost_recieved = asyncio.Event()
        self.sent_msgs = {}

    def connection_made(self, transport):
        self.transport = transport

    def connection_lost(self, exc):
        self.connection_lost_recieved.set()

    def datagram_received(self, data, addr):
        if len(data) < 8:
            logging.warning("Invalid datagram received")
            return

        action, transaction_id = unpack(">II", data[:8])
        if transaction_id in self.sent_msgs:
            self.received_msg = (action, transaction_id, data[8:])
            self.sent_msgs[transaction_id].set()
        else:
            logging.warning("Invalid transaction id received")

    def error_received(self, exc):
        logging.info(f"UDP client transmission error {exc}")

    def get_transaction_id(self):
        transaction_id = _get_random_32_integer()
        while transaction_id in self.sent_msgs:
            transaction_id = _get_random_32_integer()
        self.sent_msgs[transaction_id] = asyncio.Event()
        return transaction_id

    async def send_msg(self, msg, transaction_id):
        try:
            self.transport.sendto(msg)
            await asyncio.wait_for(self.sent_msgs[transaction_id].wait(),
                                   timeout=self.connection.timeout)
            self.sent_msgs.pop(transaction_id)
        except asyncio.TimeoutError:
            self.connection.is_timedout = True
            self.sent_msgs.pop(transaction_id)
            logging.warning("Connection to tracker timed out!")
            return

class UDPConnection:
    def __init__(self, url, params):
        self.params = params
        self.url = url
        url_parsed = urlparse(url)
        self.address_tracker = url_parsed.hostname
        self.port_tracker = url_parsed.port
        self.udp_magic_number = 0x41727101980
        self.timeout = 15
        self.max_retransmissions = 4
        self.start = time.time()
        self.connection_id = None
        self.is_timedout = False

    def has_timed_out(self):
        """
            Check if the attempt has gone beyond 15 seconds
        """
        return self.is_timedout

    async def request(self):
        loop = asyncio.get_event_loop()
        self.transport, self.protocol = await loop.create_datagram_endpoint(
                lambda: UDPTrackerProtocol(self),
                remote_addr=(self.address_tracker, self.port_tracker))
        for attempts in range(1, self.max_retransmissions+1):
            self.is_timedout = False
            logging.debug(f"Trying to connect to udp tracker {self.address_tracker}:{self.port_tracker}, attempt {attempts}/{self.max_retransmissions}")
            while True and not self.has_timed_out():
                error, success, extra_data = await self.connect()
                if error:
                    return TrackerResponseUDP(extra_data, True)
                if success:
                    logging.debug("Got connect response, announcing...")
                    break
            while True and not self.has_timed_out():
                finished, action, data = await self.announce()
                if finished:
                    logging.debug("Announce finished")
                    return TrackerResponseUDP(data, action==3)
        raise ConnectionError(f"Unable to connect to tracker {self.url}")

    async def connect(self):
        """
            Send a connect request to the tracker
        """
        transaction_id = self.protocol.get_transaction_id()
        msg = pack(">QII", self.udp_magic_number, 0,
                transaction_id)
        await self.protocol.send_msg(msg, transaction_id)

        if self.protocol.received_msg:
            action, transaction_id_recv, data = self.protocol.received_msg
            if action == 3:
                # error
                logging.warning(f"An error was received in reply to connect {data.decode()}")
                data_extra = data
                success = False
            elif transaction_id_recv == transaction_id and action == 0:
                logging.debug("Got successful connect response")
                data_extra = None
                success = True
                self.connection_id = unpack(">Q", data)[0]
            else:
                logging.warning(f"Did not receive successful connect response action {action}, transaction id {transaction_id_recv} but sent {transaction_id}")
                success = False
                data_extra = None
            return action == 3, success, data_extra

        else:
            logging.warning("Did not receive connect response")
            return None, None, None

    async def announce(self):
        """
            Send announce request to the tracker

        """
        transaction_id = self.protocol.get_transaction_id()
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
                   transaction_id, info_hash, peer_id,
                   downloaded, left, uploaded, event, ip_address,
                   key, num_want, listen_port)
        await self.protocol.send_msg(msg, transaction_id)

        if self.protocol.received_msg:
            action, transaction_id_recv, data = self.protocol.received_msg
            if action == 3:
                logging.warning(f"An error was received in reply to announce {data.decode()}")
                finished = True
            elif transaction_id_recv == transaction_id and action == 1:
                logging.debug("Got successful action response")
                finished = True
            else:
                logging.warning(f"Did not receive successful announce response action {action}, transaction id {transaction_id_recv} but sent {transaction_id}")
                finished = False
            return action, finished, data
        else:
            logging.warning("Did not receive announce response")
            return None, None, None


class Tracker:
    def __init__(self, torrent):
        self.id = _calculate_peer_id()
        self.torrent = torrent
        self.timeout = 60 # consider the tracker dead after a minute

    async def fetch_request(self, client, params):
        url_tracker = self.torrent.announce
        url = url_tracker + "?" + urlencode(params)
        try:
            async with client.get(url, timeout=self.timeout) as response:
                if not response.status == 200:
                    raise ConnectionError(f"Unable to connect to tracker {url_tracker}")
                data = await response.read()
                return TrackerResponse(bencoding.Decoder(data).decode())
        except asyncio.TimeoutError:
            raise ConnectionError(f"Connection to tracker {url_tracker} timed-out")

    async def fetch_request_udp(self, params):
        return await UDPConnection(self.torrent.announce, params).request()

    async def connect(self, first: bool=None, uploaded: int=0, downloaded: int=0):
        params = {"info_hash": self.torrent.info_hash, "peer_id": self.id,
        	"uploaded": uploaded, "downloaded": downloaded, 
        	"left": self.torrent.total_size-downloaded, "port": 6889,
        	"compact": 1}
        if first:
            params["event"] = "started"
        if self.torrent.announce.startswith("udp"):
            # use udp protocol
            return await self.fetch_request_udp(params)
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
