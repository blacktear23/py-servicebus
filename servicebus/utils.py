import threading
import logging
import asyncore


close_sockets_lock = threading.Lock()


def _do_close_sockets():
    for value in asyncore.socket_map.values():
        logging.info("Close Socket: %s" % str(value))
        value.socket.close()
    logging.info("Clean socket map!")
    asyncore.socket_map = {}


def close_sockets():
    global close_sockets_lock
    try:
        close_sockets_lock.acquire()
        _do_close_sockets()
    finally:
        close_sockets_lock.release()


def num_sockets():
    global close_sockets_lock
    try:
        close_sockets_lock.acquire()
        return len(asyncore.socket_map.values())
    finally:
        close_sockets_lock.release()
