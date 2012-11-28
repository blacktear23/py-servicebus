from datetime import datetime, timedelta
import hashlib

def do_generate_token(configuration, date=None):
    key = configuration.secret_token
    td = timedelta(1)
    if date == None:
        datestr = datetime.now().isoformat()[:10]
    elif date == "prev":
        datestr = (datetime.now() - td).isoformat()[:10]
    elif date == "next":
        datestr = (datetime.now() + td).isoformat()[:10]
    token_str = "%s - %s" % (key, datestr)
    return hashlib.sha1(token_str).hexdigest()

class AbstractMessageParser(object):
    def set_configuration(self, configuration):
        self.configuration = configuration

    def generate_token(self, date=None):
        return do_generate_token(self.configuration, date)