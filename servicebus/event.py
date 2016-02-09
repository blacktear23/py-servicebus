class Event(object):
    def __init__(self, eid, category, service, token, params, version):
        self.id = eid
        self.category = category
        self.service = service
        self.token = token
        self.params = params
        self.version = version
