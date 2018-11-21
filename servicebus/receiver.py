import logging
import threading
from servicebus.parser import XmlMessageParser, XmlResponseGenerator
from servicebus.message import AbstractReceiver
from servicebus.request import Request


class RPCResponse(object):
    def __init__(self, event, channel, method, header, receiver):
        self.event = event
        self.channel = channel
        self.method = method
        self.header = header
        self.receiver = receiver

    def send(self, message):
        msg = XmlResponseGenerator(self.event.id, message)
        self.receiver.response_message(self.channel, self.method, self.header,
                                       msg.to_xml())


class MessageBusReceiver(AbstractReceiver):
    def get_message_parser(self):
        return self.message_parser

    def set_service_bus(self, service_bus):
        self.service_bus = service_bus
        self.message_parser = XmlMessageParser()
        self.message_parser.set_configuration(service_bus.configuration)

    def on_rpc(self, channel, method, header, body):
        event = self.message_parser.parse(body)
        if not self.message_parser.validate_token(event.token):
            logging.error('Token Error')
            msg = XmlResponseGenerator(event.id, "Token not valid!")
            self.response_message(channel, method, header, msg.to_xml())
            return

        response = RPCResponse(event, channel, method, header, self)
        request = Request(event, self)
        service = self.service_bus.lookup_rpc_service_thread(event.category, event.service)

        if service is None:
            error_msg = 'Cannot Find RPC Service: %s.%s' % (event.category, event.service)
            logging.error(error_msg)
            msg = XmlResponseGenerator(event.id, error_msg)
            self.response_message(channel, method, header, msg.to_xml())
        else:
            logging.info("Call RPC Service %s.%s" % (event.category, event.service))
            service.on_call(request, response)

    def on_message(self, channel, method, header, body):
        event = self.message_parser.parse(body)
        if not self.message_parser.validate_token(event.token):
            logging.error('Token Error')
            return

        request = Request(event, self)
        service = self.service_bus.lookup_message_service_thread(event.category, event.service)

        if service is None:
            error_msg = 'Cannot Find Message Service: %s.%s' % (event.category, event.service)
            logging.error(error_msg)
        else:
            logging.info("Call Message Service %s.%s" % (event.category, event.service))
            service.on_message(request)
