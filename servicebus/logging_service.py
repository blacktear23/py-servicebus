import signal
import pickle
import struct
import logging
import logging.handlers
from multiprocessing import Process

try:
    from SocketServer import BaseRequestHandler, ThreadingUDPServer
except Exception:
    from socketserver import BaseRequestHandler, ThreadingUDPServer


DEFAULT_FORMAT_PATTERN = '[%(asctime)s PID:%(process)d]%(levelname)s:%(message)s'


class LogRecordHandler(BaseRequestHandler):
    def handle(self):
        data = self.request[0].strip()
        self.process_data(data)

    def process_data(self, data):
        chunk = data[0:4]
        if len(chunk) < 4:
            return
        slen = struct.unpack(">L", chunk)[0]
        chunk = data[4:slen + 4]
        try:
            obj = self.unpickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handle_log_record(record)
        except Exception as e:
            logging.exception(e)

    def unpickle(self, data):
        return pickle.loads(data)

    def handle_log_record(self, record):
        logging.getLogger().handle(record)


class LoggingService(ThreadingUDPServer):
    max_packet_size = 16384

    @classmethod
    def init_logging(cls, port, level=logging.INFO):
        logger = logging.getLogger()
        shandler = logging.handlers.DatagramHandler('127.0.0.1', port)
        logger.addHandler(shandler)
        logger.setLevel(level)

    def __init__(self, file_name, port, level=logging.INFO,
                 format_pattern=DEFAULT_FORMAT_PATTERN):
        self.file_name = file_name
        self.level = level
        self.format_pattern = format_pattern
        ThreadingUDPServer.__init__(self, ('127.0.0.1', port), LogRecordHandler)
        self.abort = 0
        self.timeout = 1

    def serv_untile_stopped(self):
        import select
        abort = 0
        while not abort:
            rd, wr, ex = select.select([self.socket.fileno()],
                                       [], [],
                                       self.timeout)
            if rd:
                self.handle_request()
            abort = self.abort

    def configure_logger(self):
        self.logger = logging.getLogger()
        self.logger.setLevel(self.level)
        handler = logging.handlers.TimedRotatingFileHandler(
            filename=self.file_name, when='D', backupCount=15
        )
        handler.setFormatter(logging.Formatter(self.format_pattern))
        self.logger.addHandler(handler)

    @classmethod
    def run_in_process(cls, filename, port, after_fork_hook):
        if after_fork_hook:
            after_fork_hook()
        lserver = LoggingService(filename, port)
        lserver.configure_logger()
        logging.info("Start Logging Server")
        signal.signal(signal.SIGTERM, lserver.process_exit)
        lserver.serv_untile_stopped()

    def process_exit(self, signum, frame):
        self.abort = True

    @classmethod
    def start_logging_server(cls, filename, port, after_fork_hook):
        process = Process(target=cls.run_in_process, args=(filename, port, after_fork_hook))
        process.daemon = True
        process.start()
        return process
