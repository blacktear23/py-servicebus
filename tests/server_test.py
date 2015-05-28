from servicebus.service import ServiceBus
from servicebus.configuration import Configuration
import sys
import logging
logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]%(levelname)s:%(message)s')

node_name = sys.argv[1]
CONFIG = Configuration({
    'hosts': ['172.16.10.210'],
    'user': 'admin',
    'password': '123456',
    'use_ssl': False,
    'node_name': node_name,
    'secret_token': 'secret token',
})


class AddService(object):
    background = True

    def on_call(self, request, response):
        params = request.get_params()
        a = int(params['a'])
        b = int(params['b'])
        response.send(a + b)

    def on_message(self, request):
        params = request.get_params()
        a = int(params['a'])
        b = int(params['b'])
        logging.info("%d + %d = %d" % (a, b, (a + b)))


class PrinterService(object):
    def on_message(self, request):
        logging.info(request.get_params())
        sender = request.get_sender()
        logging.info("Got Sender")
        ret = sender.call('TESTER-002.math.add', {'a': 1, 'b': 2}, 10)
        logging.info("Calculste 1 + 2 = %s" % (ret[1]))


def create_service_bus():
    sbus = ServiceBus(CONFIG)
    sbus.add_rpc_service("math", "add", AddService())
    sbus.add_message_services("math", "add", AddService())
    sbus.add_message_services("util", "print", PrinterService())
    return sbus


def main():
    create_service_bus().run_services()


main()
