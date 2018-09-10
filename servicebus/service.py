import sys
import time
import copy
import signal
import logging
import asyncore
from threading import Thread
from multiprocessing import Process
from servicebus import utils

if sys.version_info < (3, 0):
    from Queue import Queue
else:
    from queue import Queue


class ServiceBus(object):
    def __init__(self, configuration, queue_len=100):
        self.configuration = configuration
        self.rpc_services = {}
        self.message_services = {}
        self.rpc_services_threads = {}
        self.message_services_threads = {}
        self.node_name = ""
        self.after_fork_hook = None
        self.on_exit_hook = None
        self.queue_len = queue_len

    def after_fork(self):
        if self.after_fork_hook:
            self.after_fork_hook()

    def set_after_fork_hook(self, hook):
        self.after_fork_hook = hook

    def set_on_exit_hook(self, hook):
        self.on_exit_hook = hook

    def set_node_name(self, name):
        self.node_name = name

    def add_rpc_service(self, category, name, service):
        key = "%s.%s" % (category, name)
        self.rpc_services[key] = service

    def add_message_services(self, category, name, service):
        key = "%s.%s" % (category, name)
        self.message_services[key] = service

    def _prepare_message_service_threads(self):
        for key, service in self.message_services.items():
            thread = ServiceRunner(service, self.queue_len)
            thread.start()
            self.message_services_threads[key] = thread

    def _prepare_rpc_service_threads(self):
        for key, service in self.rpc_services.items():
            thread = ServiceRunner(service, self.queue_len)
            thread.start()
            self.rpc_services_threads[key] = thread

    def lookup_rpc_service_thread(self, category, name):
        key = "%s.%s" % (category, name)
        return self.rpc_services_threads.get(key, None)

    def lookup_message_service_thread(self, category, name):
        key = "%s.%s" % (category, name)
        return self.message_services_threads.get(key, None)

    def run_services(self, join=True):
        processes = []
        for host in self.configuration.hosts:
            process = Process(target=self.run_server, args=(self.configuration, host))
            process.daemon = True
            process.start()
            processes.append(process)

        # wait for all process
        if join:
            for process in processes:
                process.join()
        return processes

    def run_server(self, configuration, host):
        self.running = True
        receiver = None
        self.after_fork()
        self.prepare_service_threads()

        def process_exit(signum, frame):
            self.running = False
            try:
                self.stop_service_threads()
                if self.on_exit_hook is not None:
                    logging.info('[Server %s]: Call on_exit hook' % host)
                    self.on_exit_hook()
            except Exception as e:
                logging.exception(e)
            logging.info("[Server %s]: Shutdown" % host)
            raise SystemExit("Terminate")

        signal.signal(signal.SIGTERM, process_exit)

        while self.running:
            receiver = None
            try:
                logging.info('[Server %s]: Build Server' % host)
                receiver = configuration.create_receiver(host)
                receiver.set_service_bus(self)
                receiver.bind_queue_to_exchange(configuration.queue_name(), configuration.exchange_name)
                logging.info('[Server %s]: Start Receive' % host)
                self.running = receiver.start_receive()
            except Exception as e:
                logging.exception(e)

            if self.running:
                logging.info('[Server %s]: Connection lost, wait 10 second to retry' % host)
                time.sleep(10)
                if receiver is not None:
                    receiver.join_watcher()
        self.stop_service_threads()
        try:
            if self.on_exit_hook is not None:
                logging.info('[Server %s]: Call on_exit hook' % host)
                self.on_exit_hook()
        except Exception as e:
            logging.exception(e)
        logging.info("[Server %s]: Shutdown" % host)

    def __force_close_sockets(self):
        utils.close_sockets()

    def prepare_service_threads(self):
        self._prepare_message_service_threads()
        self._prepare_rpc_service_threads()

    def stop_service_threads(self):
        logging.info("Stop Service Threads")
        all_threads = self.message_services_threads.values() + self.rpc_services_threads.values()
        for sthread in all_threads:
            sthread.stop()
        for sthread in all_threads:
            sthread.join()


class ServiceRunner(Thread):
    def __init__(self, service, queue_len=1):
        super(ServiceRunner, self).__init__()
        self.service = service
        self.queue = Queue(queue_len)
        self.is_background = self.is_background_service(service)
        self.background_threads = []

    def is_background_service(self, service):
        if hasattr(service, 'background'):
            return service.background
        return False

    def run(self):
        while True:
            msg_type, params = self.queue.get()
            if msg_type == "stop":
                self.queue.task_done()
                break
            # We copy the service object is for each message process.
            # This copy is for Background Service running. If we do not
            # copy this object, Background Service will return last received
            # message's data. So if there has 2 same service running in background
            # will cause a status report bug.
            service = copy.copy(self.service)
            if self.is_background:
                logging.info("Run in background")
                thread = Thread(target=self.run_service, args=(service, msg_type, params))
                thread.start()
                self.add_background_service(thread)
            else:
                self.run_service(service, msg_type, params)
            self.queue.task_done()
        self.wait_background_services()

    def run_service(self, service, msg_type, params):
        try:
            if msg_type == "message":
                service.on_message(*params)
            elif msg_type == "call":
                service.on_call(*params)
        except Exception as e:
            logging.exception(e)

        if len(params) > 0:
            request = params[0]
            request.get_sender().close()

    def on_message(self, request):
        self.queue.put(("message", (request,)))

    def on_call(self, request, response):
        self.queue.put(("call", (request, response)))

    def stop(self):
        self.queue.put(("stop", None))

    def add_background_service(self, thread):
        nthreads = [thread]
        for t in self.background_threads:
            if t.is_alive():
                nthreads.append(t)
        self.background_threads = nthreads

    def wait_background_services(self):
        if len(self.background_threads) > 0:
            logging.info("Wait for background service threads")
            for t in self.background_threads:
                t.join()
