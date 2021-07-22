import signal
import asyncio
import logging
import argparse

from concurrent.futures import CancelledError

from torrenter.torrent import Torrent
from torrenter.client import TorrentClient

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("torrent",
                        help="the .torrent to download")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="enable verbose output")
    parser.add_argument("-l", "--log", default=None,
                        help="Path of log file")

    args = parser.parse_args()
    level_log = logging.INFO
    if args.verbose:
        level_log = logging.DEBUG

    if args.log:
        logging.basicConfig(filename=args.log, filemode="a",
                level=level_log, format='%(asctime)s %(levelname)s %(message)s')
    else:
        logging.basicConfig(level=level_log)

    loop = asyncio.get_event_loop()
    client = TorrentClient(Torrent(args.torrent))
    task = loop.create_task(client.start())

    def signal_handler(*_):
        logging.info("Exiting, please wait until everything is shutdown...")
        client.stop()
        task.cancel()
        loop.stop()

    signal.signal(signal.SIGINT, signal_handler)

    try:
        loop.run_forever()
    except CancelledError:
        logging.warning("Event loop was cancelled")
