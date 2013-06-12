import time
from servicebus.message import MessageSender
from servicebus.receiver import MessageBusReceiver
from servicebus.command import get_host_name

DEFAULT_EXCHANGE_NAME = 'py-servicebus'

class Configuration(object):
    """
    params: config
    config should be a hash:
    config['hosts']     = ['host1', 'host2']
    config['port']      = port default is 5672
    config['ssl_port']  = ssl port default is 5671
    config['user']      = user name
    config['password']  = password
    config['use_ssl']   = True or False default is False
    config['node_name'] = node name aka queue name if not set default
                          is host name.
    config['exchange_name'] = RabbitMQ Exchange name or use default:
                                py-servicebus
    config['secret_token']  = Message token secret token seed
    """
    def __init__(self, config):
        self.hosts = config['hosts']
        self.secret_token = config['secret_token']
        self.port = 5672
        if config.has_key('port'):
            self.port = int(config['port'])
        self.ssl_port = 5671
        if config.has_key('ssl_port'):
            self.ssl_port = int(config['ssl_port'])
        self.use_ssl = False
        if config.has_key('use_ssl'):
            self.use_ssl = config['use_ssl']
        self.user = None
        if config.has_key('user'):
            self.user = config['user']
        self.password = None
        if config.has_key('password'):
            self.password = config['password']
        self.node_name = get_host_name()
        if config.has_key('node_name'):
            self.node_name = config['node_name']
        self.exchange_name = DEFAULT_EXCHANGE_NAME
        if config.has_key('exchange_name'):
            self.exchange_name = config['exchange_name']

    """
    Thie method will create a message receiver.
    params: host    RabbitMQ server's host
    """
    def create_receiver(self, host):
        receiver = MessageBusReceiver(
            host,
            self.get_port(),
            self.user,
            self.password,
            self.use_ssl
        )
        return receiver

    """
    This method will get an availiable host to send message to.
    """
    def create_sender(self):
        for i in range(3):
            caller = self.__get_availiable_message_sender()
            if caller != None:
                break
            time.sleep(1)
        
        if not caller:
            raise Exception("Cannot Connect to Message Queue!")
        return caller

    """
    This method will create N senders N is equals length of host.
    Each of sender is connect to each host.
    If host cannot connected, return value will not contains this host's sender.
    """
    def create_senders(self):
        ret = []
        for host in self.hosts:
            try:
                caller = self.__create_message_sender(host)
                caller.ensure_connection()
                ret.append(caller)
            except Exception, e:
                pass
        return ret

    def queue_name(self):
        return self.node_name
        
    def exchange_name(self):
        return self.exchange_name

    def get_port(self):
        if self.use_ssl:
            port = self.ssl_port
        else:
            port = self.port
        return port

    def __get_availiable_message_sender(self):
        for host in self.hosts:
            try:
                caller = self.__create_message_sender(host)
                caller.ensure_connection()
                return caller
            except Exception, e:
                pass
        return None

    def __create_message_sender(self, host):
        caller = MessageSender(
            host,
            self.get_port(),
            self.user,
            self.password,
            self.use_ssl
        )
        return caller
