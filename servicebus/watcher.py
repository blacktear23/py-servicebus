import time
import logging
import thread
import asyncore
from servicebus.command import cmd

DIDA_TIMEOUT = 60


class PingWatcher:
    def __init__(self, receiver):
        self.receiver = receiver

    def do_watch(self):
        ip = self.receiver.host
        while True:
            ret = self.ping(ip)
            if not ret and self.receiver.connected:
                self.receiver.connected = False
                logging.info("Ping Error!")
                for value in asyncore.socket_map.values():
                    logging.info("Close Socket: %s" % str(value))
                    value.socket.close()
                logging.info("Clean socket map!")
                asyncore.socket_map = {}
                # we close all connections then we do not to watch now.
                return
            time.sleep(DIDA_TIMEOUT)

    def do_ping(self, ip):
        command_linux  = "ping -c 1 %s | grep \" 1 received,\"" % (ip)
        command_macosx = "ping -c 1 %s | grep \" 1 packets received,\"" % (ip)
        for command in [command_linux, command_macosx]:
            ret = cmd(command)[0]
            if ret != "":
                return True
        return False
    
    def ping(self, ip):
        for i in range(3):
            ret = self.do_ping(ip)
            if ret: return True
            time.sleep(1)
        return False

    def run_watch(self, ignore):
        while True:
            try:
                self.do_watch()
            except Exception, e:
                logging.error(e)

    @classmethod
    def start_watch(cls, receiver):
        pw = PingWatcher(receiver)
        thread.start_new_thread(pw.run_watch, (None,))