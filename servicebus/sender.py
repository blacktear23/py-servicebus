import logging
from servicebus.parser import XmlRequestGenerator, XmlResponseParser

class Sender(object):
	def __init__(self, configuration):
		self.configuration = configuration
		self.exchange_name = configuration.exchange_name
		self.caller = None

	def get_caller(self):
		if self.caller == None:
			self.caller = self.configuration.create_sender()
			self.caller.set_exchange(self.exchange_name)
		return self.caller

	def parse_target(self, target):
		parts = target.split(".")
		if len(parts) != 3:
			raise Exception("Target not validate")
		return parts

	def call(self, target, params):
		target, category, service = self.parse_target(target)
		caller = self.get_caller()
		req_msg = XmlRequestGenerator(self.configuration, category, service, params)
		ret = caller.call(target, req_msg.to_xml())
		resp_parser = XmlResponseParser()
		return resp_parser.parse(ret)

	def send(self, target, params):
		target, category, service = self.parse_target(target)
		caller = self.get_caller()
		req_msg = XmlRequestGenerator(self.configuration, category, service, params)
		caller.send(target, req_msg.to_xml())

	def close(self):
		if self.caller:
			self.caller.close()
			self.caller = None
