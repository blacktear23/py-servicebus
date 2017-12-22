import time
import logging
from threading import Thread
from servicebus.command import cmd


DIDA_TIMEOUT = 60
JIFFY = 5


class PingWatcher:
    def __init__(self, receiver):
        self.run = True
        self.receiver = receiver

    def do_watch(self):
        ip = self.receiver.host
        ret = self.ping(ip)
        if not ret and self.receiver.connected:
            self.receiver.connected = False
            logging.info("Ping Error!")
            return True
        duration = 0
        while self.run:
            time.sleep(JIFFY)
            duration += JIFFY
            if duration >= DIDA_TIMEOUT:
                duration = 0
                ret = self.ping(ip)
                if not ret and self.receiver.connected:
                    self.receiver.connected = False
                    logging.info("Ping Error!")
                    return True
                if not self.receiver.connected:
                    logging.info("Message receiver exited, PingWatcher out.")
                    return True
        return False

    def stop(self):
        logging.info("PingWatcher Killed")
        self.run = False

    def do_ping(self, ip):
        command_linux = "ping -c 1 %s | grep \" 1 received,\"" % (ip)
        command_macosx = "ping -c 1 %s | grep \" 1 packets received,\"" % (ip)
        for command in [command_linux, command_macosx]:
            ret = cmd(command)[0]
            if ret != "":
                return True
        return False

    def ping(self, ip):
        for i in range(3):
            ret = self.do_ping(ip)
            if ret:
                return True
            time.sleep(1)
        return False

    def run_watch(self):
        while self.run:
            try:
                if self.do_watch():
                    return
                else:
                    if not self.run:
                        logging.info("PingWatcher out")
                        return
            except Exception as e:
                logging.error(e)

    def join(self):
        self.thread.join()

    @classmethod
    def start_watch(cls, receiver):
        pw = PingWatcher(receiver)
        thread = Thread(target=pw.run_watch)
        thread.start()
        pw.thread = thread
        return pw
