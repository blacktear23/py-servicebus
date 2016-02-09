import logging
from servicebus.parser import XmlRequestGenerator, XmlResponseParser


class Sender(object):
    def __init__(self, configuration, smart_route=True):
        self.configuration = configuration
        self.exchange_name = configuration.exchange_name
        self.caller = None
        self.callers = None
        self.smart_route = smart_route

    def get_caller(self, reverse=False):
        if self.caller is None:
            self.caller = self.configuration.create_sender(reverse)
            self.caller.set_exchange(self.exchange_name)
        return self.caller

    def choose_caller(self, target, reverse=False):
        if self.smart_route:
            return self.smart_route_choose_caller(target, reverse)
        else:
            return self.simple_choose_caller(target, reverse)

    def simple_choose_caller(self, target, reverse=False):
        return self.get_caller(reverse)

    def smart_route_choose_caller(self, target, reverse=False):
        callers = self.get_callers()
        if reverse:
            callers.reverse()
        ret = None
        for caller in callers:
            if self._caller_ping(caller, target):
                return caller
        if len(callers) > 0:
            return callers[0]
        return ret

    def _caller_ping(self, caller, target, timeout=3):
        try:
            ret = caller.call(target, "PING", timeout)
            return ret == "PONG"
        except Exception:
            return False

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
            except Exception:
                pass
        return success, total

    def call(self, target, params, timeout=300, reverse=False):
        target, category, service = self.parse_target(target)
        caller = self.choose_caller(target, reverse)
        if caller is None:
            raise Exception("Cannot connect to %s" % target)
        req_msg = XmlRequestGenerator(self.configuration, category, service, params)
        ret = caller.call(target, req_msg.to_xml(), timeout)
        resp_parser = XmlResponseParser()
        return resp_parser.parse(ret)

    def send(self, target, params):
        target, category, service = self.parse_target(target)
        caller = self.choose_caller(target)
        if caller is None:
            raise Exception("Cannot connect to %s" % target)
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
                except Exception:
                    pass
            self.callers = None
