import time
import copy
import thread
import asyncore
import logging
from multiprocessing import Process
from servicebus import utils


class ServiceBus(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self.rpc_services = {}
        self.message_services = {}
        self.node_name = ""
        self.after_fork_hook = None

    def after_fork(self):
        if self.after_fork_hook:
            self.after_fork_hook()

    def set_after_fork_hook(self, hook):
        self.after_fork_hook = hook

    def set_node_name(self, name):
        self.node_name = name

    def add_rpc_service(self, category, name, service):
        key = "%s.%s" % (category, name)
        self.rpc_services[key] = service

    def add_message_services(self, category, name, service):
        key = "%s.%s" % (category, name)
        self.message_services[key] = service

    # We copy the service object is for each message process.
    # This copy is for Background Service running. If we do not
    # copy this object, Background Service will return last received
    # message's data. So if there has 2 same service running in background
    # will cause a status report bug.
    def lookup_rpc_service(self, category, name):
        key = "%s.%s" % (category, name)
        if key not in self.rpc_services:
            return None
        origin_obj = self.rpc_services[key]
        return copy.copy(origin_obj)

    def lookup_message_service(self, category, name):
        key = "%s.%s" % (category, name)
        if key not in self.message_services:
            return None
        origin_obj = self.message_services[key]
        return copy.copy(origin_obj)

    def is_background_service(self, service):
        if hasattr(service, 'background'):
            return service.background
        return False

    def run_services(self):
        processes = []
        for host in self.configuration.hosts:
            process = Process(target=self.run_server, args=(self.configuration, host))
            process.daemon = True
            process.start()
            processes.append(process)

        # wait for all process
        for process in processes:
            process.join()

    def run_server(self, configuration, host):
        self.after_fork()
        while True:
            receiver = None
            try:
                logging.info('[Server %s]: Build Server' % host)
                receiver = configuration.create_receiver(host)
                receiver.set_service_bus(self)
                receiver.bind_queue_to_exchange(configuration.queue_name(), configuration.exchange_name)
                logging.info('[Server %s]: Start Receive' % host)
                receiver.start_receive()
            except Exception as e:
                logging.exception(e)
            finally:
                try:
                    self.__force_close_sockets()
                except Exception as e:
                    logging.error(e)
            logging.info('[Server %s]: Connection lost, wait 10 second to retry' % host)
            time.sleep(10)

    def __force_close_sockets(self):
        utils.close_sockets()
