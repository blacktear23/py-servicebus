__version__ = '0.10.0'

import logging
try:
    # not available in python 2.6
    from logging import NullHandler
except ImportError:

    class NullHandler(logging.Handler):

        def emit(self, record):
            pass

# Add NullHandler to prevent logging warnings
logging.getLogger(__name__).addHandler(NullHandler())

from servicebus.pika.connection import ConnectionParameters
from servicebus.pika.connection import URLParameters
from servicebus.pika.credentials import PlainCredentials
from servicebus.pika.spec import BasicProperties

from servicebus.pika.adapters import BaseConnection
from servicebus.pika.adapters import BlockingConnection
from servicebus.pika.adapters import SelectConnection
from servicebus.pika.adapters import TornadoConnection
from servicebus.pika.adapters import TwistedConnection
from servicebus.pika.adapters import LibevConnection
