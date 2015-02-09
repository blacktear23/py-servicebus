import logging
from servicebus.parser import XmlRequestGenerator, XmlResponseParser


class Sender(object):
    def __init__(self, configuration):
        self.configuration = configuration
        self.exchange_name = configuration.exchange_name
        self.caller = None
        self.callers = None

    def get_caller(self, reverse=False):
        if self.caller is None:
            self.caller = self.configuration.create_sender(reverse)
            self.caller.set_exchange(self.exchange_name)
        return self.caller

    def get_callers(self):
        if self.callers is None:
            self.callers = self.configuration.create_senders()
            for caller in self.callers:
                caller.set_exchange(self.exchange_name)
        return self.callers

    def parse_target(self, target):
        parts = target.split(".")
        if len(parts) != 3:
            raise Exception("Target not validate")
        return parts

    def ping(self, target, timeout=3):
        caller = self.get_caller(True)
        ret = caller.call(target, "PING", timeout)
        return ret == "PONG"

    def ping_all(self, target, timeout=3):
        callers = self.get_callers()
        total = len(callers)
        success = 0
        for caller in callers:
            try:
                caller.set_exchange(self.exchange_name)
                ret = caller.call(target, "PING", timeout)
                if ret == "PONG":
                    success += 1
            except Exception as e:
                pass
        return success, total

    def call(self, target, params, timeout=300, reverse=False):
        target, category, service = self.parse_target(target)
        if not self.ping(target):
            raise Exception("Cannot connect to %s" % target)
        caller = self.get_caller(reverse)
        req_msg = XmlRequestGenerator(self.configuration, category, service, params)
        ret = caller.call(target, req_msg.to_xml(), timeout)
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

        if self.callers:
            for caller in self.callers:
                try:
                    caller.close()
                except Exception as e:
                    pass
            self.callers = None
