from servicebus.sender import Sender


class Request(object):
    def __init__(self, event, receiver):
        self.event = event
        self.receiver = receiver
        self.sender = Sender(receiver.service_bus.configuration)

    def get_params(self):
        return self.event.params

    def get_event(self):
        return self.event

    def get_sender(self):
        return self.sender
