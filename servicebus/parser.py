import json
import hashlib
from datetime import datetime, timedelta
from xml.dom.minidom import parseString
from servicebus.event import Event


def do_generate_token(configuration, date=None):
    key = configuration.secret_token
    td = timedelta(1)
    if date is None:
        datestr = datetime.now().isoformat()[:10]
    elif date == "prev":
        datestr = (datetime.now() - td).isoformat()[:10]
    elif date == "next":
        datestr = (datetime.now() + td).isoformat()[:10]
    token_str = "%s - %s" % (key, datestr)
    return hashlib.sha1(token_str.encode()).hexdigest()


class AbstractMessageParser(object):
    def set_configuration(self, configuration):
        self.configuration = configuration

    def generate_token(self, date=None):
        return do_generate_token(self.configuration, date)


class XmlParserHelper(object):
    def get_text(self, node):
        rc = []
        for child in node.childNodes:
            if child.nodeType == node.TEXT_NODE or child.nodeType == node.CDATA_SECTION_NODE:
                rc.append(child.data)
        return ''.join(rc)


class JSONParamsParser(XmlParserHelper):
    def __init__(self):
        self.params = {}

    def parse(self):
        params_node = self.params_node
        self.parse_param(params_node)

    def set_params_node(self, value):
        self.params_node = value

    def parse_param(self, params_node):
        json_code = self.get_text(params_node)
        self.params = json.loads(json_code)


# XML message format:
#   <?xml version="1.0"?>
#   <event version="1">
#       <id>EVENT_ID</id>
#       <token>EVENT_TOKEN</token>
#       <catgory>EVENT_CATEGORY</catgory>
#       <service>SERVICE_NAME</service>
#       <params>
#           JSON_PARAMS
#       </params>
#   </event>
class XmlMessageParser(AbstractMessageParser, XmlParserHelper):
    def parse(self, xml_doc):
        try:
            doc = parseString(xml_doc)

            if not self.validate_xml(doc):
                return None
            root = doc.childNodes[0]
            version = self.get_message_version(root)
            eid = self.get_event_id(root)
            category = self.get_category(root)
            service = self.get_service(root)
            token = self.get_token(root)

            params_parser = JSONParamsParser()
            params_parser.set_params_node(root.getElementsByTagName('params')[0])
            params_parser.parse()
            params = params_parser.params

            return Event(eid, category, service, token, params, version)
        except Exception:
            # if got any exception in parse return None
            return None

    def validate_xml(self, doc):
        root = doc.childNodes[0]
        if not root.tagName == 'event':
            return False
        if not root.hasAttribute('version'):
            return False
        return True

    def validate_token(self, token):
        if self.generate_token() == token:
            return True
        elif self.generate_token("prev") == token:
            return True
        elif self.generate_token("next") == token:
            return True
        return False

    def get_message_version(self, root):
        return root.getAttribute('version')

    def get_event_id(self, root):
        node = root.getElementsByTagName('id')[0]
        return self.get_text(node)

    def get_category(self, root):
        node = root.getElementsByTagName('catgory')[0]
        return self.get_text(node)

    def get_service(self, root):
        node = root.getElementsByTagName('service')[0]
        return self.get_text(node)

    def get_token(self, root):
        node = root.getElementsByTagName('token')[0]
        return self.get_text(node)


class XmlResponseParser(XmlParserHelper):
    def parse(self, xml_doc):
        try:
            doc = parseString(xml_doc)
            root = doc.childNodes[0]
            rid = self.get_request_id(root)
            message = self.get_message(root)
            return (rid, message)
        except Exception:
            # if got any exception in parse return None
            return None

    def get_request_id(self, root):
        node = root.getElementsByTagName('id')[0]
        return self.get_text(node)

    def get_message(self, root):
        node = root.getElementsByTagName('message')[0]
        return self.get_text(node)


ID_SEED = 0
MESSAGE_TEMPLATE = """<?xml version="1.0"?>
<event version="1">
    <id>%s</id>
    <token>%s</token>
    <catgory>%s</catgory>
    <service>%s</service>
    <params><![CDATA[%s]]></params>
</event>
"""
REPORT_XML_TEMPLATE = """<?xml version="1.0"?>
<response>
    <id>%s</id>
    <message><![CDATA[%s]]></message>
</response>
"""


class XmlRequestGenerator(object):
    def __init__(self, configuration, category, service, message):
        self.message = message
        self.configuration = configuration
        self.category = category
        self.service = service
        self.message = message

    def generate_id(self):
        global ID_SEED
        ID_SEED = ID_SEED + 1
        return ID_SEED

    def generate_token(self):
        return do_generate_token(self.configuration)

    def encode_params(self):
        return json.dumps(self.message)

    def to_xml(self):
        return MESSAGE_TEMPLATE % (
            self.generate_id(),
            self.generate_token(),
            self.category, self.service, self.encode_params())


class XmlResponseGenerator(object):
    def __init__(self, eid, message):
        self.eid = eid
        self.message = message

    def to_xml(self):
        return REPORT_XML_TEMPLATE % (self.eid, self.message)
