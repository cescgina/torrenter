import os
import time
import math
import logging
from typing import Optional, List
import asyncio
from asyncio import Queue
from collections import namedtuple, defaultdict
from hashlib import sha1

from torrenter.protocol import PeerConnection, REQUEST_SIZE
from torrenter.tracker import Tracker

# The number of max peer connections per TorrentClient
MAX_PEER_CONNECTIONS = 40


class TorrentClient:
    def __init__(self, torrent):
        self.torrent = torrent
        self.tracker = Tracker(torrent)
        # The list of potential peers is the work queue, consumed by the
        # PeerConnections
        self.available_peers = Queue()
        # The list of peers is the list of workers that *might* be connected
        # to a peer. Else they are waiting to consume new remote peers from
        # the `available_peers` queue. These are our workers!
        self.peers = []
        # The piece manager implements the strategy on which pieces to
        # request, as well as the logic to persist received pieces to disk.
        self.piece_manager = PieceManager(torrent)
        self.abort = False

    async def start(self):
        self.peers = [PeerConnection(self.available_peers, 
            self.tracker.torrent.info_hash,
            self.tracker.id,
            self.piece_manager,
            self._on_block_retrieved)
            for _ in range(MAX_PEER_CONNECTIONS)]
        # The time we last made an announce call (timestamp)
        previous = None
        # Default interval between announce calls (in seconds)
        interval = 30*60

        while True:
            if self.piece_manager.complete or self.abort:
                break
            current = time.time()
            if (not previous) or (previous + interval < current):
                response = await self.tracker.connect(
                        first = True if previous else False, 
                        uploaded=self.piece_manager.bytes_uploaded,
                        downloaded=self.piece_manager.bytes_downloaded)
                if response:
                    previous = current
                    interval = response.interval
                    self._empty_queue()
                    for peer in response.peers:
                        self.available_peers.put_nowait(peer)
            else:
                await asyncio.sleep(5)
        self.stop()

    def _empty_queue(self):
        while not self.available_peers.empty():
            self.available_peers.get_nowait()

    def stop(self):
        """
            Stop the download or seeding process
        """
        self.abort = True
        for peer in self.peers:
            peer.stop()
        self.piece_manager.close()
        self.tracker.close()

    def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
            Callback function called by the `PeerConnection` when a block is
            retrieved from a peer

	    :param peer_id: The id of the peer the block was retrieved from
	    :param piece_index: The piece index this block is a part of
	    :param block_offset: The block offset within its piece
	    :param data: The binary data retrieved
        """
        self.piece_manager.block_received(peer_id=peer_id, piece_index=piece_index,
					  block_offset=block_offset, data=data)

class Block:
    """
	The block is a partial piece, this is what is requested and transferred
	between peers.
	A block is most often of the same size as the REQUEST_SIZE, except for the
	final block which might (most likely) is smaller than REQUEST_SIZE.
    """
    Missing = 0
    Pending = 1
    Retrieved = 2

    def __init__(self, piece: int, offset: int, length: int):
        self.piece = piece
        self.offset = offset
        self.length = length
        self.status = Block.Missing
        self.data = None
	
	
class Piece:
    """
        The piece is a part of of the torrents content. Each piece except the final
	piece for a torrent has the same length (the final piece might be shorter).
	A piece is what is defined in the torrent meta-data. However, when sharing
	data between peers a smaller unit is used - this smaller piece is refereed
	to as `Block` by the unofficial specification (the official specification
	uses piece for this one as well, which is slightly confusing).
    """
    def __init__(self, hash_value, index: int, blocks: list = []):
        self.index = index
        self.blocks = blocks
        self.hash = hash_value

    def reset(self):
        """
            Reset all blocks to Missing regardless of current state
        """
        for block in self.blocks:
            block.status = Block.Missing

    def next_request(self) -> Optional[Block]:
        """
            Get the next Block to be requested
        """
        missing = [b for b in self.blocks if b.status is Block.Missing]
        if missing:
            missing[0].status = Block.Pending
            return missing[0]
        return None

    def block_recieved(self, offset: int, data: bytes):
        """
            Update block information that the given block is now received
            
            :param offset: The block offset (whithin the piece)
            :param data: The block data
        """
        matches = [b for b in self.blocks if b.offset == offset]
        block = matches[0] if matches else None
        if block:
            block.status = Block.Retrieved
            block.data = data
        else:
            logging.warning(f"Trying to complete a non-existing block {offset}")
	
    def is_complete(self) -> bool:
        """
            Checks if all blocks for this piece is retrieved (regardless of
            SHA1)
        """
        blocks = [b for b in self.blocks if b.status is not Block.Retrieved]
        return len(blocks) == 0

    def is_hash_matching(self) -> bool:
        """
            Check if a SHA1 hash for all the received blocks match the piece
            hash from the torrent meta-info
        """
        piece_hash = sha1(self.data).digest()
        return self.hash == piece_hash

    @property
    def data(self):
        """
            Return the data for this piece (by concatenating all blocks in
            order)
        """
        retrieved = sorted(self.blocks, key=lambda b: b.offset)
        blocks_data = [b.data for b in retrieved]
        return b"".join(blocks_data)

# The type used for keeping track of pending request that can be re-issued
PendingRequest = namedtuple("PendingRequest", ["block", "added"])

class PieceManager:
    """
        The PieceManager is responsible for keeping track of all the available
        pieces for the connected peers as well as the pieces we have available
        for other peers

        The strategy on which piece to request is made as simple as possible in
        this implementation
    """
    def __init__(self, torrent):
        self.torrent = torrent
        self.peers = {}
        self.pending_blocks = []
        self.missing_pieces = []
        self.ongoing_pieces = []
        self.have_pieces = []
        self.max_pending_time = 300 * 1000 #  5 minutes
        self.missing_pieces = self._initiate_pieces()
        self.total_pieces = len(torrent.pieces)
        self.fd = os.open(self.torrent.output_file, os.O_RDWR | os.O_CREAT)

    def _initiate_pieces(self) -> List[Piece]:
        """
            Pre-construct the list of pieces and blocks based on the number of
            pieces and request size for this torrent
        """
        #TODO: adapt this for multiple files
        pieces = []
        total_pieces = len(self.torrent.pieces)
        std_piece_blocks = math.ceil(self.torrent.piece_length / REQUEST_SIZE)
        for index, hash_value in enumerate(self.torrent.pieces):
            # The number of blocks for each piece can be calculated using the
            # request size as divisor for the piece length
            # The final piece, however, will most likely have fewer blocks than
            # regular pieces, and that final block might be smaller than the
            # other blocks
            if index < (total_pieces-1):
                blocks = [Block(index, offset*REQUEST_SIZE, REQUEST_SIZE) for
                        offset in range(std_piece_blocks)]
            else:
                last_length = self.torrent.total_size % self.torrent.piece_length
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE) for
                        offset in range(num_blocks)]

                if last_length % REQUEST_SIZE > 0:
                    # last block of the last piece might be smaller than the
                    # ordinary request size
                    blocks[-1].length = last_length % REQUEST_SIZE
            pieces.append(Piece(hash_value, index, blocks))
        return pieces

    def close(self):
        if self.fd:
            os.close(self.fd)

    @property
    def complete(self):
        """
            Checks whether or not all pieces are downloaded for this torrent

            :return: True if all pieces are fully downloaded else False
        """
        return len(self.have_pieces) == self.total_pieces

    @property
    def bytes_downloaded(self) -> int:
        """
            Get the number of bytes downloaded

            This method only counts full, verified pieces, not single blocks
        """
        return len(self.have_pieces) * self.torrent.piece_length

    @property
    def bytes_uploaded(self) -> int:
        """
            Get the number of bytes uploaded
        """
        #TODO: Add support for sending data
        return 0

    def add_peer(self, peer_id, bitfield):
        """
            Adds a peer and the bitfield representing the pieces the peer has
        """
        self.peers[peer_id] = bitfield

    def update_peer(self, peer_id, index: int):
        """
            Updates the information about which pieces a peer has (reflects
            a Have message)
        """
        if peer_id in self.peers:
            self.peers[peer_id][index] = 1

    def remove_peer(self, peer_id):
        """
            Tries to remove a previously added peer (e.g. used if a peer
            connnection is dropped)
        """
        self.peers.pop(peer_id)

    def next_request(self, peer_id) -> Optional[Block]:
        """
            Get the next Block that should be requested from the given peer

            If there are no more blocks left to retrieve or if this peer does
            not have any of the missing pieces None is returned
        """
        # The algorithm implemented for which piece to retrieve is a simple
        # one. This should preferably be replaced with an implementation of
        # "rarest-piece-first" algorithm instead.
        #
        # The algorithm tries to download the pieces in sequence and will try
        # to finish started pieces before starting with new pieces.
        #
        # 1. Check any pending blocks to see if any request should be reissued
        #    due to timeout
        # 2. Check the ongoing pieces to get the next block to request
        # 3. Check if this peer have any of the missing pieces not yet started
        if peer_id not in self.peers:
            return None
        block = self._expired_requests(peer_id)
        if not block:
            block = self._next_ongoing(peer_id)
            if not block:
                block = self._get_rarest_piece(peer_id).next_request()
        return block

    def block_received(self, peer_id, piece_index, block_offset, data):
        """
            This method must be called when a block has succesfully been
            retrieved by a peer

            Once a full piece has been retrieved, a SHA1 hash control is made.
            If the check fails all the pieces blocks are put back in missing
            state to be fetched again. If the hash succeds the partial piece is
            written to disk and the piece is indicated as Have
        """
        logging.debug(f"Recieved block {block_offset} for piece {piece_index} from peer {peer_id}")
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece == piece_index and request.block_offset == block_offset:
                del self.pending_blocks[index]
                break

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces - len(self.missing_pieces)
                            - len(self.ongoing_pieces))
                    logging.info(f"{complete} / {self.total_pieces} pieces downloaded {100*complete/self.total_pieces:.3f}")
                else:
                    logging.info(f"Discarding corrupt piece {index}")
                    piece.reset()
        else:
            logging.warning("Trying to update piece that is not ongoing!")

    def _expired_requests(self, peer_id) -> Optional[Block]:
        """
            Go through previously request blocks, if any one have been in the
            requested state for longer than MAX_PENDING_TIME return the block
            to be re-requested

            If no pending blocks exist, None is returned
        """
        current = int(round(time.time() * 1000))
        for request in self.pending_blocks:
            if self.peers[peer_id][request.block.piece]:
                if request.added + self.max_pending_time < current:
                    logging.info(f"Re-requesting block {request.block.offset} for piece {request.block.piece}")
                    # reset expiration timer
                    request.added = current
                    return request.block
        return None

    def _next_ongoing(self, peer_id) -> Optional[Block]:
        """
            Go through the ongoing pieces and return the next block to be
            requested of None if no Block is left to be requested
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                # is there any block left to request in this piece?
                block = piece.next_request()
                if block:
                    self.pending_blocks.append(PendingRequest(block, int(round(time.time() * 1000))))
                    return block
        return None

    def _get_rarest_piece(self, peer_id):
        """
            Given the current list of missing pieces, get the rares one first
            (i.e. a piece which fewest of its neighboring peers have)
        """
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                piece_count[piece] += self.peers[p][piece.index]

        rarest_piece = min(piece_count, key=lambda p: piece_count[p])
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _next_missing(self, peer_id) -> Optional[Block]:
        """
            Go through the missing pieces and return the next block to request
            or None if no block is left to be requested

            This will change the state of the piece from missing to ongoing
            - thus the next call to this function will not continue with the
            blocks for that piece, rather get the next missing piece
        """
        for index, piece in enumerate(self.missing_pieces):
            if self.peers[peer_id][piece.index]:
                # Move this piece from missing to ongoing
                piece = self.missing_pieces.pop(index)
                self.ongoing_pieces.append(piece)
                # The missing pieces does not have any previously requested
                # blocks (then it is ongoing)
                return piece.next_request()
        return None

    def _write(self, piece):
        """
            Write the given piece to disk
        """
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fd, pos, os.SEEK_SET)
        os.write(self.fd, piece.data)
