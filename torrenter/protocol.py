import asyncio
import logging
import struct
from asyncio import Queue, TimeoutError
from typing import Optional
from concurrent.futures import CancelledError

import bitstring

REQUEST_SIZE = 2**14


class ProtocolError(BaseException):
        pass

class PeerConnection:
    """
	A peer connection used to download and upload pieces.
	The peer connection will consume one available peer from the given queue.
	Based on the peer details the PeerConnection will try to open a connection
	and perform a BitTorrent handshake.
	After a successful handshake, the PeerConnection will be in a *choked*
	state, not allowed to request any data from the remote peer. After sending
	an interested message the PeerConnection will be waiting to get *unchoked*.
	Once the remote peer unchoked us, we can start requesting pieces.
	The PeerConnection will continue to request pieces for as long as there are
	pieces left to request, or until the remote peer disconnects.
	If the connection with a remote peer drops, the PeerConnection will consume
	the next available peer from off the queue and try to connect to that one
	instead.
    """
    def __init__(self, queue: Queue, info_hash, peer_id, piece_manager, on_block_cb=None):
        """
	    Constructs a PeerConnection and add it to the asyncio event-loop.
	    Use `stop` to abort this connection and any subsequent connection
	    attempts
	    :param queue: The async Queue containing available peers
	    :param info_hash: The SHA1 hash for the meta-data's info
	    :param peer_id: Our peer ID used to to identify ourselves
	    :param piece_manager: The manager responsible to determine which pieces
				  to request
	    :param on_block_cb: The callback function to call when a block is
				received from the remote peer
        """
        self.my_state = []
        self.peer_state = []
        self.queue = queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.remote_id = None
        self.writer = None
        self.reader = None
        self.piece_manager = piece_manager
        self.on_block_cb = on_block_cb
        self.future = asyncio.ensure_future(self._start())  # Start this worker

    def are_interested(self):
        return "choked" not in self.my_state and "pending_request" not in self.my_state and "interested" in self.my_state

    async def _start(self):
        while 'stopped' not in self.my_state:
            ip, port = await self.queue.get()
            logging.info(f"Got assigned peer with ip {ip} and port {port}")

            try:
                #TODO: for some reason it does not seem to work to open a new
                # connection if the first one drops (i.e. second loop)
                logging.debug(f"Attempting to connect to peer: {ip} using port {port}")
                self.reader, self.writer = await asyncio.open_connection(ip, port)
                logging.info(f"Connection open to peer: {ip} using port {port}")

                # it's our responsability to initiate the handshake
                buffer = await self._handshake()

                #TODO Add support for sending data
                # Sending bitfield is options and not needed when client does not
                # have any pieces. Thus we do not send any bitfield message

                # let the peer know we're interested in downloading pieces
                await self._send_interested()
                self.my_state.append("interested")

                # Start reading responses as a stream of messages for as long as
                # the connection is open and data is transmitted

                async for message in PeerStreamIterator(self.reader, self.remote_id, buffer):
                    if "stopped" in self.my_state:
                        break
                    if type(message) is BitField:
                        self.piece_manager.add_peer(self.remote_id, message.bitfield)
                    elif type(message) is Interested:
                        self.peer_state.append('interested')
                    elif type(message) is NotInterested:
                        if 'interested' in self.peer_state:
                            self.peer_state.remove('interested')
                    elif type(message) is Choke:
                        self.my_state.append("choked")
                    elif type(message) is Unchoke:
                        if "choked" in self.my_state:
                            self.my_state.remove("choked")
                    elif type(message) is Have:
                        self.piece_manager.update_peer(self.remote_id,
                                message.index)
                    elif type(message) is KeepAlive:
                        pass
                    elif type(message) is Piece:
                        self.my_state.remove("pending_request")
                        self.on_block_cb(
                                peer_id=self.remote_id, 
                                piece_index=message.index,
                                block_offset=message.begin,
                                data=message.block)
                    elif type(message) is Request:
                        #TODO Add support for sending data
                        logging.info("Ignoring the received Request message")
                    elif type(message) is Cancel:
                        #TODO Add support for sending data
                        logging.info("Ignoring the received Cancel message")

                    # send block request to remote peer if we're interested
                    if self.are_interested():
                        self.my_state.append("pending_request")
                        await self._request_piece()

            except ProtocolError as e:
                logging.exception(f"ProtocolError with peer {self.remote_id}")
            except (ConnectionRefusedError, TimeoutError):
                logging.warning(f"Unable to connect to peer {self.remote_id}")
            except (ConnectionResetError, CancelledError):
                logging.warning(f"Connection to {self.remote_id} closed")
            except Exception as e:
                logging.exception(f"An error occurred with peer {self.remote_id}")
                self.cancel()
                raise e
            self.cancel()

    def cancel(self):
        """
            Sends the cancel message to the remote peer and closes the
            connection
        """
        logging.info(f"Closing peer {self.remote_id}")
        if not self.future.done():
            self.future.cancel()
        if self.writer:
            self.writer.close()

        self.queue.task_done()

    def stop(self):
        """
            Stop this connection from the current peer (if a connection exits)
            and from connecting to any new peer
        """
        # set state to stopped and cancel our future to break out of the loop.
        # The rest of the cleanup will eventually be managed by loop calling
        # cancel
        self.my_state.append("stopped")
        if not self.future.done():
            self.future.cancel()

    async def _request_piece(self):
        block = self.piece_manager.next_request(self.remote_id)
        if block:
            message = Request(block.piece, block.offset, block.length).encode()

            logging.debug(f"Requesting block {block.piece} for piece {block.offset} of {block.length} bytes from peer {self.remote_id}")

            self.writer.write(message)
            await self.writer.drain()

    async def _handshake(self):
        """
            Send the initial handshake to the remote peer and wait for the peer
            to respond with its handshake
        """
        self.writer.write(Handshake(self.info_hash, self.peer_id).encode())
        await self.writer.drain()

        buf = b""
        tries = 1
        while len(buf) < Handshake.length and tries < 10:
            tries += 1
            buf += await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
        response = Handshake.decode(buf[:Handshake.length])
        if not response:
            raise ProtocolError("Unable to receive and parse a handshake")
        if not response.info_hash == self.info_hash:
            raise ProtocolError("Handshake with invalid info_hash")

        # TODO According to spec we should validate that the peer id received
        # from the peer match the peer_id received from the tracker
        self.remote_id = response.peer_id
        logging.debug(f"peer_id {self.peer_id} received {response.peer_id}")
        logging.info("Handshake with peer was successful")

        # We need to return the remaining buffer data, since we might have read
        # more bytes than the size of the handshake and we need those bytes to
        # parse the next message
        return buf[Handshake.length:]

    async def _send_interested(self):
        message = Interested()
        logging.debug(f"Sending message: {message}")
        self.writer.write(message.encode())
        await self.writer.drain()

class PeerStreamIterator:
    """
        The PeerStreamIterator is an async iterator that continuosly read from
        the given stream reader and tries to parse valid BitTorrent messages
        from that stream of bytes

        If the connection is dropped or something fails the iterator will abort
        by raising the StopAsyncIteration error ending the calling iteration
    """
    CHUNK_SIZE = 10*1024

    def __init__(self, reader, remote_id, initial: bytes=None):
        self.reader = reader
        self.buffer = initial if initial else b""
        self.remote_id = remote_id

    async def __aiter__(self):
        return self

    async def __anext__(self):
        # Read data from the socket. When we have enough data to parse, parse
        # it and return the message. Until then keep reading from stream
        while True:
            try:
                # data = await self.reader.read(PeerStreamIterator.CHUNK_SIZE)
                # set a timeout for 3 seconds
                data = await asyncio.wait_for(self.reader.read(PeerStreamIterator.CHUNK_SIZE), 3)
                if data:
                    self.buffer += data
                    message = self.parse()
                    if message:
                        return message
                else:
                    logging.debug(f"No data read from stream from peer {self.remote_id} length of buffer {len(self.buffer)}")
                    if self.buffer:
                        message = self.parse()
                        if message:
                            return message
                    raise StopAsyncIteration()
            except ConnectionResetError:
                logging.debug(f"Connection closed by peer {self.remote_id}")
                raise StopAsyncIteration()
            except CancelledError:
                raise StopAsyncIteration()
            except TimeoutError:
                logging.debug(f"Timeout while reading from peer {self.remote_id}")
                raise StopAsyncIteration()
            except StopAsyncIteration as e:
                # catch to stop logging
                raise e
            except Exception:
                logging.exception(f"Error when iterating over stream from peer {self.remote_id}!")
                raise StopAsyncIteration()
        raise StopAsyncIteration()

    def parse(self):
        """
            Tries to parse protocol messages if there is enough bytes read in
            the buffer

            :return: The parsed message, or None if no message could be parsed
        """
        # Each message is structured as:
        #   <length prefix><message ID><payload>
        # The length prefix is a four byte big-endian value
        # The message ID is a decimal byte
        # The payload is the value of length prefix
        # the message length is not part of the actual length. So another
        # 4 bytes needs to be included when slicing the buffer
        header_length = 4

        if len(self.buffer) > 4: # 4 bytes is needed to identify the message
            message_length = struct.unpack(">I", self.buffer[0:4])[0]
            
            if message_length == 0:
                return KeepAlive()
            if len(self.buffer) >= message_length:
                message_id = struct.unpack(">b", self.buffer[4:5])[0]

                def _consume():
                    """
                        consume the current message from the read buffer
                    """
                    self.buffer = self.buffer[header_length+message_length:]

                def _data():
                    """
                        Extract the current message from the read buffer
                    """
                    return self.buffer[:header_length+message_length]

                if message_id is PeerMessage.BitField:
                    data = _data()
                    _consume()
                    return BitField.decode(data)
                elif message_id is PeerMessage.Interested:
                    _consume()
                    return Interested()
                elif message_id is PeerMessage.NotInterested:
                    _consume()
                    return NotInterested()
                elif message_id is PeerMessage.Choke:
                    _consume()
                    return Choke()
                elif message_id is PeerMessage.Unchoke:
                    _consume()
                    return Unchoke()
                elif message_id is PeerMessage.Have:
                    data = _data()
                    _consume()
                    return Have.decode(data)
                elif message_id is PeerMessage.Piece:
                    data = _data()
                    _consume()
                    return Piece.decode(data)
                elif message_id is PeerMessage.Request:
                    data = _data()
                    _consume()
                    return Request.decode(data)
                elif message_id is PeerMessage.Cancel:
                    data = _data()
                    _consume()
                    return Cancel.decode(data)
            else:
                logging.debug(f"Not enough in buffer in order to parse, message_length is {message_length}, but buffer length is  {len(self.buffer)}, peer {self.remote_id}")
        return None

class PeerMessage:
    """
        A message between two peers

        All of the remaining messages in the protocol take the form of:
            <length prefix><message ID><payload>

        - The length prefix is a four byte big-endian value
        - The message ID is a single decimal byte
        - The payload is message dependent

        NOTE: The Handshake message is different in layout compared to the
        other messages
    """
    Choke = 0
    Unchoke = 1
    Interested = 2
    NotInterested = 3
    Have = 4
    BitField = 5
    Request = 6
    Piece = 7
    Cancel = 8
    Port = 9
    Handshake = None # Handshake is not really part of the messages
    KeepAlive = None # KeepAlive has no ID according to spec

    def encode(self) -> Optional[bytes]:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        pass

    @classmethod
    def decode(cls, data:bytes):
        """
            Decodes the given BitTorrent message into a instance for the
            implementing type
        """
        pass

class Handshake(PeerMessage):
    """
        The handshake message is the first message sent and then received from
        a remote peer

        The message is always 68 bytes long 

        Message format:
            <pstrlen><pstr><reserved><info_hash><peer_id>

        In version 1.0 of the BitTorrent protocol:
            pstrlen = 19
            pstr = "BitTorrent protocol"

        Thus length is:
            49 + len(pstr) = 68 bytes long
    """
    length = 49+19

    def __init__(self, info_hash: bytes, peer_id: bytes):
        """
            Construct the handshake message
        """
        if isinstance(info_hash, str):
            info_hash = info_hash.encode("utf-8")
        if isinstance(peer_id, str):
            peer_id = peer_id.encode("utf-8")
        self.info_hash = info_hash
        self.peer_id = peer_id

    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        return struct.pack(
                ">B19s8x20s20s",
                19,                     # single byte (B)
                b"BitTorrent protocol", # string 19s
                                        # Reserved 8x (pad byte, no value)
                self.info_hash,         # string 20s
                self.peer_id)           # string 20s

    @classmethod
    def decode(cls, data: bytes):
        """
            Decodes the given BitTorrent message into a handshake message, if
            not a valid message, None is returned
        """
        if len(data) < 49+19:
            return None
        data_parsed = struct.unpack(">B19s8x20s20s", data)
        logging.debug(f"Decoding Handshake of length: {len(data)} with peer {data_parsed[3]} and hash {data_parsed[2]}")
        return cls(info_hash=data_parsed[2], peer_id=data_parsed[3])
                
    def __str__(self):
        return f"Handshake with peer {self.peer_id} and hash {self.info_hash}"

class KeepAlive(PeerMessage):
    """
        The KeepAlive message has no payload and length is set to zero

        Message format:
            <len=0000>
    """
    def __str__(self):
        return "KeepAlive"

class BitField(PeerMessage):
    """
        The BitField is a message with variable length where the payload is
        a bit array representing all the bits a peer have (1) or does not have
        (0)

        Message format:
            <len=0001+X><id=5><bitfield>
    """
    def __init__(self, data):
        self.bitfield = bitstring.BitArray(bytes=data)

    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        bits_length = len(self.bitfield)
        return struct.pack(
                ">Ib"+str(bits_length)+"s",
                1+bits_length,
                PeerMessage.BitField,
                self.bitfield)

    @classmethod
    def decode(cls, data: bytes):
        """
            Decodes the given BitTorrent message into a BitField message, if
            not a valid message, None is returned
        """
        message_length = struct.unpack(">I", data[:4])[0]
        data_parsed = struct.unpack(">Ib"+str(message_length-1)+"s", data)
        # logging.debug(f"Decoding BitField of length: {message_length} with data {int(data_parsed[2].hex(), base=16):b}")
        logging.debug(f"Decoding BitField of length: {message_length}")
        return cls(data_parsed[2])
                
    def __str__(self):
        return "BitField"

class Interested(PeerMessage):
    """
        The interested message is fixed length and has no payload other than the
        message identifiers. It is used to notify each other about interest in
        downloading pieces.

        Message format:
            <len=0001><id=2>
    """
    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        return struct.pack(">Ib",1, PeerMessage.Interested)

    def __str__(self):
        return "Interested"

class NotInterested(PeerMessage):
    """
        The interested message is fixed length and has no payload other than the
        message identifiers. It is used to notify each other that there is no interest in
        downloading pieces.

        Message format:
            <len=0001><id=3>
    """
    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        return struct.pack(">Ib",1, PeerMessage.NotInterested)

    def __str__(self):
        return "NotInterested"

class Choke(PeerMessage):
    """
        The choke message is used to tell the other peer to stop send request
        messages until unchoked

        Message format:
            <len=0001><id=0>
    """
    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        return struct.pack(">Ib",1, PeerMessage.Choke)

    def __str__(self):
        return "Choke"

class Unchoke(PeerMessage):
    """
        The unchoke message is used to tell the peer to start requesting pieces
        from the remote peer

        Message format:
            <len=0001><id=1>
    """
    def encode(self) -> bytes:
        """
            Encodes this object instance to the raw bytes representing the
            entire message (ready to be transmitted)
        """
        return struct.pack(">Ib",1, PeerMessage.Unchoke)

    def __str__(self):
        return "Unchoke"

class Have(PeerMessage):
    """
        Represents a piece successfully downloaded by the remote peer. The
        piece is a zero based index of the torrents pieces

        Message format:
            <len=0005><id=4><index>
    """
    def __init__(self, index: int):
        self.index = index

    def encode(self):
        return struct.pack(">IbI",
                5,
                PeerMessage.Have,
                self.index)

    @classmethod
    def decode(cls, data:bytes):
        logging.debug("Decoding Have message of length {len(data)}")
        index = struct.unpack(">IbI", data)[2]
        return cls(index)

    def __str__(self):
        return "Have"

class Request(PeerMessage):
    """
        The message used to request a block of a piece (i.e a partial piece).

        The request size for each block is 2^14 bytes, except the final block
        that might be smaller (since not all pieces might be evenly divided by
        the request size)

        Message format:
            <len=0013><id=6><index><begin><length>
    """
    def __init__(self, index: int, begin: int, length: int = REQUEST_SIZE):
        """
            Constructs the Request message

            :param index: The zero based piece index
            :param being: The zero based offset within a piece
            :param length: The requested length of data (default 2^14)
        """
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack(">IbIII",
                13,
                PeerMessage.Request,
                self.index,
                self.begin,
                self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f"Decoding request of length: {len(data)}")
        parsed_data = struct.unpack(">IbIII", data)
        return cls(parsed_data[2], parsed_data[3], parsed_data[4])

    def __str__(self):
        return "Request"

class Piece(PeerMessage):
    """
        A block is a part of a piece mentioned in the meta-info. The official
        specification refer to them as pieces as well- which is quite confusing
        the unofficial specification refers to them as blocks

        So this class is name Piece to match the message in the specification
        but really it represents a Block

        Message format:
            <length prefix><id=6><index><begin><block>
    """
    # The Piece message length without the block data
    length = 9
    def __init__(self, index : int, begin: int, block: bytes):
        """
            Constructs the Piece message

            :param index: The zero based piece index
            :param begin: The zero based offset within a piece
            :param block: The block data
        """
        self.index = index
        self.begin = begin
        self.block = block

    def encode(self):
        return struct.pack(">IbII"+str(len(self.block))+"s", self.length+len(self.block),
                PeerMessage.Piece, self.index, self.begin, self.block)

    @classmethod
    def decode(cls, data: bytes):
        message_len = struct.unpack(">I", data[:4])[0]
        parsed_data = struct.unpack(">IbII"+str(message_len-cls.length)+"s",
                data[:message_len+4])
        logging.debug(f"Decoding Piece message of length {len(data)} with index {parsed_data[2]} begin {parsed_data[3]} and block of length {len(parsed_data[4])}")
        return cls(parsed_data[2], parsed_data[3], parsed_data[4])

    def __str__(self):
        return "Piece"

class Cancel(PeerMessage):
    """
        The cancel message is used to cancel a previously requested block (in
        fact the message is identical to the Request message, except for the id)

        Message format:
            <len=0013><id=8><index><begin><length>
    """
    def __init__(self, index : int, begin: int, length: int = REQUEST_SIZE):
        """
            Constructs the Cancel message

            :param index: The zero based piece index
            :param begin: The zero based offset within a piece
            :param length: The requested length of data (default 2^14)
        """
        self.index = index
        self.begin = begin
        self.length = length

    def encode(self):
        return struct.pack(">IbIII",13, PeerMessage.Cancel,
                self.index, self.begin, self.length)

    @classmethod
    def decode(cls, data: bytes):
        logging.debug(f"Decoding Cancel message of length {len(data)}")
        parsed_data = struct.unpack(">IbIII", data)
        return cls(parsed_data[2], parsed_data[3], parsed_data[4])
    
    def __str__(self):
        return "Cancel"

class Port(PeerMessage):
    """
        The message sent by newer versions of the mainline that implements
        a DHT tracker. This will probably not be used at all, just leaving it
        here for completeness

        Message format:
            <len=0003><id=9><listen-port>
    """
    def __init__(self, listen_port: int):
        self.port = listen_port

    def enconde(self):
        return struct.pack(">IbI", 3, PeerMessage.Port, self.port)

    @classmethod
    def decode(cls, data:bytes):
        logging.debug(f"Decoding Port message of length {len(data)}")
        port = struct.unpack(">IbI", data)[2]
        return cls(port)

    def __str__(self):
        return "Port"
