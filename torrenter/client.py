import os
import time
import math
import errno
import random
import logging
from typing import Optional, List
import asyncio
from asyncio import Queue
from collections import namedtuple, defaultdict
from hashlib import sha1

from bitstring import BitArray

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
        self.endgame = False

    async def start(self):
        self.peers = [PeerConnection(i, self.available_peers, 
            self.tracker.torrent.info_hash,
            self.tracker.id,
            self.piece_manager,
            self._on_block_retrieved,
            self._on_request_send)
            for i in range(MAX_PEER_CONNECTIONS)]
        # The time we last made an announce call (timestamp)
        previous = None
        # Default interval between announce calls (in seconds)
        interval = 30*60

        while True:
            if self.abort:
                break

            current = time.time()
            if (not previous) or (previous + interval < current):
                logging.debug(f"Asking tracker {self.torrent.announce} for peers")
                try:
                    response = await self.tracker.connect(
                            first = True if previous else False,
                            uploaded=self.piece_manager.bytes_uploaded,
                            downloaded=self.piece_manager.bytes_downloaded)
                except ConnectionError as e:
                    logging.exception(f"Could not connect to tracker {e}")
                    self.torrent.demote_tracker()
                    await asyncio.sleep(5)
                    continue
                if response:
                    logging.debug(f"Got response from tracker, with {len(response.peers)} peers and interval of {response.interval}")
                    previous = current
                    interval = response.interval
                    self._empty_queue()
                    for peer in self.peers:
                        if not peer.active:
                            if peer.remote_id is not None:
                                logging.debug(f"Removing peer {peer.remote_id} from piece manager")
                                self.piece_manager.remove_peer(peer.remote_id)
                            peer.restart_peer()

                    for peer in response.peers:
                        self.available_peers.put_nowait(peer)
                else:
                    logging.warning("Got a failure from tracker, reason {response.failure}")
            else:
                logging.debug(f"Have to wait {(previous+interval)-current} seconds for next ping to tracker")
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

    def _on_request_send(self):
        """
            Callback function called by the `PeerConnection` when a request is
            send

            It is used to check whether all pieces have been requested and
            enter endgame mode
        """
        if not self.piece_manager.missing_pieces and self.piece_manager.have_pieces and not self.endgame:
            # if there are no missing pieces, but we have already downloaded
            # some, we enter endgame mode
            logging.info("Entering endgame mode")
            self.endgame = True
            self.piece_manager.endgame = True

    async def _on_block_retrieved(self, peer_id, piece_index, block_offset, data):
        """
            Callback function called by the `PeerConnection` when a block is
            retrieved from a peer

	    :param peer_id: The id of the peer the block was retrieved from
	    :param piece_index: The piece index this block is a part of
	    :param block_offset: The block offset within its piece
	    :param data: The binary data retrieved
        """
        index_piece = self.piece_manager.block_received(peer_id=peer_id, piece_index=piece_index,
                                                        block_offset=block_offset, data=data)
        if index_piece is not None:
            for peer in self.peers:
                await peer.send_have(index_piece)
        if self.endgame:
            for peer in self.peers:
                await peer.send_cancel(piece_index, block_offset, len(data))
        if self.piece_manager.complete:
            self.endgame = False
            self.piece_manager.endgame = False


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
    def __init__(self, hash_value, index: int, blocks: list = [], files: list = []):
        self.index = index
        self.blocks = blocks
        self.hash = hash_value
        # list of tuples containing (index of file, size of the file in the piece)
        self.files = files

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

    def next_request_endgame(self) -> Optional[Block]:
        """
            Get the next Block to be requested in the endgame
        """
        pending = [b for b in self.blocks if b.status in (Block.Pending, Block.Missing)]
        if pending:
            return pending[0]
        return None

    def block_received(self, offset: int, data: bytes):
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
        self._bytes_uploaded = 0
        self.fds = [os.open(filename, os.O_RDWR | os.O_CREAT) for filename in self.torrent.output_files]
        self.endgame = False
        self._check_previous_download()

    def _initiate_pieces(self) -> List[Piece]:
        """
            Pre-construct the list of pieces and blocks based on the number of
            pieces and request size for this torrent
        """
        pieces = []
        total_pieces = len(self.torrent.pieces)
        std_piece_blocks = math.ceil(self.torrent.piece_length / REQUEST_SIZE)
        size_allocated = 0
        limits = [(i, f.offset, f.length) for i, f in enumerate(self.torrent.files)]
        for index, hash_value in enumerate(self.torrent.pieces):
            # The number of blocks for each piece can be calculated using the
            # request size as divisor for the piece length
            # The final piece, however, will most likely have fewer blocks than
            # regular pieces, and that final block might be smaller than the
            # other blocks
            if index == (total_pieces-1):
                last_length = self.torrent.total_size - size_allocated
                files_list = calculates_files_in_piece(limits, size_allocated, 
                                                       self.torrent.total_size)
                num_blocks = math.ceil(last_length / REQUEST_SIZE)
                blocks = [Block(index, offset * REQUEST_SIZE, REQUEST_SIZE) for
                        offset in range(num_blocks)]

                if last_length % REQUEST_SIZE > 0:
                    # last block of the last piece might be smaller than the
                    # ordinary request size
                    blocks[-1].length = last_length % REQUEST_SIZE
            else:
                start_piece = size_allocated
                end_piece = start_piece+std_piece_blocks*REQUEST_SIZE
                files_list = calculates_files_in_piece(limits, start_piece, end_piece)

                size_allocated += std_piece_blocks*REQUEST_SIZE
                blocks = [Block(index, offset*REQUEST_SIZE, REQUEST_SIZE) for
                        offset in range(std_piece_blocks)]
            pieces.append(Piece(hash_value, index, blocks=blocks, files=files_list))
        return pieces

    def _check_previous_download(self):
        """
            Check if there is a previous download for the torrent and resume it
            if it exists
        """
        # TODO: adapt for multi-file
        # read all existing blocks and put them on have_pieces
        found_pieces = 0
        for piece in self.missing_pieces:
            pos = piece.index * self.torrent.piece_length
            os.lseek(self.fd, pos, os.SEEK_SET)
            data = os.read(self.fd, self.torrent.piece_length)
            if not data:
                continue
            # hash data and check if it's correct
            piece_hash = sha1(data).digest()
            if piece_hash == self.torrent.pieces[piece.index]:
                found_pieces += 1
                self.missing_pieces.remove(piece)
                self.have_pieces.append(piece)

        if found_pieces:
            logging.info(f"Found {found_pieces} pieces from a previous download")


    def close(self):
        try:
            for fd in self.fds:
                os.close(fd)
        except OSError as e:
            # catch and ignore Bad file descriptor error when closing
            if e.errno != errno.EBADF:
                raise e

    @property
    def bitfield(self):
        """
            Generate a bitfield of the pieces we have
        """
        if not self.have_pieces:
            return None
        num_bytes = math.ceil(self.total_pieces / 8)
        bitfield = BitArray(num_bytes*8)
        for piece in self.have_pieces:
            bitfield[piece.index] = 1
        return bitfield.bytes

    def get_block_from_piece(self, index: int, offset: int):
        """
            Get the piece with index, return None if we don't have it
        """
        piece = None
        for p in self.have_pieces:
            if p.index == index:
                piece = p
                break
        if piece:
            return piece.blocks[offset]

        
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
        return self._bytes_uploaded

    def uploaded_bytes(self, value : int):
        """
            Update the number of bytes uploaded
        """
        self._bytes_uploaded += value

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
        self.peers.pop(peer_id, None)

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
        if self.endgame:
            # in endgame everybody requests the same piece
            block = self._get_endgame_block(peer_id)
            return block

        block = self._expired_requests(peer_id)
        if block:
            return block
        else:
            block = self._next_ongoing(peer_id)

        if block:
            return block
        elif not self.have_pieces:
            # when  the download starts we pick a piece at random to get
            # a complete piece as fast as possible
            piece = self._get_random_piece(peer_id)
            if piece:
                block = piece.next_request()
            if block:
                return block
        else:
            rare_piece = self._get_rarest_piece(peer_id)
            if rare_piece:
                block = rare_piece.next_request()
            else:
                return None
        return block

    def _get_endgame_block(self, peer_id):
        """
            Select a block to request in the endgame
        """
        for piece in self.ongoing_pieces:
            if self.peers[peer_id][piece.index]:
                # is there any block left to request in this piece?
                block = piece.next_request_endgame()
                return block

    def peer_has_missing_pieces(self, peer_id):
        """
            Check if peer has any of the pieces that are missing or ongoing

            Returns two bools, whether it has any missing pieces and whether it
            has any ongoing pieces
        """
        if peer_id not in self.peers:
            return None
        missing = [self.peers[peer_id][p.index] for p in self.missing_pieces]
        ongoing = [self.peers[peer_id][p.index] for p in self.ongoing_pieces]
        return any(missing), any(ongoing)

    def block_received(self, peer_id, piece_index, block_offset, data):
        """
            This method must be called when a block has succesfully been
            retrieved by a peer

            Once a full piece has been retrieved, a SHA1 hash control is made.
            If the check fails all the pieces blocks are put back in missing
            state to be fetched again. If the hash succeds the partial piece is
            written to disk and the piece is indicated as Have
        """
        logging.debug(f"Received block {block_offset} for piece {piece_index} from peer {peer_id}")
        index = None
        for index, request in enumerate(self.pending_blocks):
            if request.block.piece == piece_index and request.block.offset == block_offset:
                del self.pending_blocks[index]
                break

        pieces = [p for p in self.ongoing_pieces if p.index == piece_index]
        piece = pieces[0] if pieces else None
        if piece:
            piece.block_received(block_offset, data)
            if piece.is_complete():
                if piece.is_hash_matching():
                    if self.torrent.multi_file:
                        self._write_multi_file(piece)
                    else:
                        self._write(piece)
                    self.ongoing_pieces.remove(piece)
                    self.have_pieces.append(piece)
                    complete = (self.total_pieces - len(self.missing_pieces)
                            - len(self.ongoing_pieces))
                    logging.info(f"{complete} / {self.total_pieces} pieces downloaded {100*complete/self.total_pieces:.2f} %")
                    if complete == self.total_pieces:
                        logging.info("Download completed!!")
                    return piece.index
                else:
                    logging.info(f"Discarding corrupt piece {index}")
                    piece.reset()
        else:
            logging.warning("Trying to update piece that is not ongoing!")

    def _expired_requests(self, peer_id) -> Optional[Block]:
        """
            Go through previously requested blocks, if any one have been in the
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
                    request = request._replace(added=current)
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
            Given the current list of missing pieces, get the rarest one first
            (i.e. a piece which fewest of its neighboring peers have)
        """
        piece_count = defaultdict(int)
        for piece in self.missing_pieces:
            if not self.peers[peer_id][piece.index]:
                continue
            for p in self.peers:
                piece_count[piece] += self.peers[p][piece.index]

        if not piece_count:
            return None
        rarest_piece = min(piece_count, key=lambda p: piece_count[p])
        most_common_piece = max(piece_count, key=lambda p: piece_count[p])
        logging.debug(f"Rarest piece found is {rarest_piece.index} with peer {peer_id} with {piece_count[rarest_piece]} occurences, most common one is {most_common_piece.index}, with {piece_count[most_common_piece]} occurences")
        self.missing_pieces.remove(rarest_piece)
        self.ongoing_pieces.append(rarest_piece)
        return rarest_piece

    def _get_random_piece(self, peer_id):
        """
            Get a random piece, this should only be called when the download
            has just started
        """
        bitfield = self.peers[peer_id]
        if not sum(bitfield):
            # peer has nothing
            return None
        pieces = [p for p in self.missing_pieces if bitfield[p.index]]
        piece = random.choice(pieces)
        logging.debug(f"Selecting piece {piece.index} at random")
        self.missing_pieces.remove(piece)
        self.ongoing_pieces.append(piece)
        return piece

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

    def _write_multi_file(self, piece):
        """
            Write the given piece to disk for a multi-file torrent 
        """
        data_used = 0
        for file_index, data_size in piece.files:
            fd = self.fds[file_index]
            data = piece.data[data_used:data_size]
            data_used = data_size
            if self.torrent.files[file_index].pieces == 1:
                # the whole file fits in one piece, simple case
                pos = 0
            else:
                # in piece index is 0, we start at the beginning of the file
                pos = min(piece.index * self.torrent.piece_length - self.torrent.files[file_index].offset, 0)
            os.lseek(fd, pos, os.SEEK_SET)
            os.write(fd, data)
                


    def _write(self, piece):
        """
            Write the given piece to disk
        """
        pos = piece.index * self.torrent.piece_length
        os.lseek(self.fds[0], pos, os.SEEK_SET)
        os.write(self.fds[0], piece.data)

def calculates_files_in_piece(files_limits, start, end):
    """
        Calculate which files belong to a specific piece and their sizes
    """
    files = []
    to_remove = []
    for i, (f, offset, length) in enumerate(files_limits):
        if start >= offset and end >= (offset+length):
            # whole file f fits in this piece
            files.append((f, length))
            start += length
            to_remove.append(i)
        elif start >= offset and end < (offset+length):
            # file starts here, but goes beyond this piece
            files.append((f, end-start))
    for index in to_remove[::-1]:
        files_limits.pop(index)
    return files
