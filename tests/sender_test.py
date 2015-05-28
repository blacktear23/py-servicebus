from servicebus.configuration import Configuration
from servicebus.sender import Sender
import logging

logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s]%(levelname)s:%(message)s')
CONFIG = Configuration({
    'hosts': ['172.16.10.210'],
    'user': 'admin',
    'password': '123456',
    'use_ssl': False,
    'node_name': 'TESTER-001',
    'secret_token': 'secret token',
})


def main():
    sender = Sender(CONFIG)
    for i in xrange(100):
        # print sender.call('TESTER-001.math.add', {'a': 1, 'b': 2})
        sender.send('TESTER-001.util.print', "Hello World!")
    sender.close()

main()
