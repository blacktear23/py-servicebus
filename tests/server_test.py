from servicebus.service import ServiceBus
from servicebus.configuration import Configuration
import logging
logging.basicConfig(level=logging.INFO, 
                    format='[%(asctime)s]%(levelname)s:%(message)s')
                    
CONFIG = Configuration({
    'hosts': ['localhost'],
    'user': 'admin',
    'password': '123456',
    'use_ssl': False,
    'node_name': 'TESTER-001',
    'secret_token': 'secret token',
})

class AddService(object):
    def on_call(self, request, response):
        params = request.get_params()
        a = int(params['a'])
        b = int(params['b'])
        response.send(a + b)

class PrinterService(object):
    def on_message(self, request):
        logging.info(request.get_params())
        sender = request.get_sender()
        ret = sender.call('TESTER-001.math.add', {'a': 1, 'b': 1})
        logging.info("Calculate 1+1=%s" % ret[1])

def create_service_bus():
    sbus = ServiceBus(CONFIG)
    sbus.add_rpc_service("math", "add", AddService())
    sbus.add_message_services("util", "print", PrinterService())
    return sbus

def main():
    create_service_bus().run_services()

main()