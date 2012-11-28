import logging
from servicebus.parser.xml_format import XmlRequestGenerator, XmlResponseParser

class Sender(object):
	def __init__(self, configuration):
		self.configuration = configuration
		self.exchange_name = configuration.exchange_name

	def parse_target(self, target):
		parts = target.split(".")
		if len(parts) != 3:
			raise Exception("Target not validate")
		return parts

	def call(self, target, params):
		target, category, service = self.parse_target(target)
		sender = self.configuration.create_sender()
		sender.set_exchange(self.exchange_name)
		req_msg = XmlRequestGenerator(self.configuration, category, service, params)
		ret = sender.call(target, req_msg.to_xml())
		resp_parser = XmlResponseParser()
		return resp_parser.parse(ret)

	def send(self, target, params):
		target, category, service = self.parse_target(target)
		sender = self.configuration.create_sender()
		sender.set_exchange(self.exchange_name)
		req_msg = XmlRequestGenerator(self.configuration, category, service, params)
		sender.send(target, req_msg.to_xml())