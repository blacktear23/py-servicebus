import time
import logging
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
        if 'port' in config:
            self.port = int(config['port'])
        self.ssl_port = 5671
        if 'ssl_port' in config:
            self.ssl_port = int(config['ssl_port'])
        self.use_ssl = False
        if 'use_ssl' in config:
            self.use_ssl = config['use_ssl']
        self.user = None
        if 'user' in config:
            self.user = config['user']
        self.password = None
        if 'password' in config:
            self.password = config['password']
        self.node_name = get_host_name()
        if 'node_name' in config:
            self.node_name = config['node_name']
        self.exchange_name = DEFAULT_EXCHANGE_NAME
        if 'exchange_name' in config:
            self.exchange_name = config['exchange_name']
        self.socket_timeout = 5
        if 'socket_timeout' in config:
            self.socket_timeout = config['socket_timeout']
        self.heartbeat_interval = 60
        if 'heartbeat_interval' in config:
            self.heartbeat_interval = config['heartbeat_interval']

    """
    Thie method will create a message receiver.
    params: host    RabbitMQ server's host
    """
    def create_receiver(self, host, close_ioloop=True):
        receiver = MessageBusReceiver(
            host,
            self.get_port(),
            self.user,
            self.password,
            self.use_ssl,
            self.socket_timeout,
            self.heartbeat_interval
        )
        try:
            receiver.ensure_connection()
        except Exception as e:
            if close_ioloop:
                try:
                    receiver.connection.close_ioloop()
                except Exception:
                    pass
            raise e
        return receiver

    """
    This method will get an availiable host to send message to.
    """
    def create_sender(self, reverse=False):
        for i in range(3):
            caller = self.__get_availiable_message_sender(reverse)
            if caller is not None:
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
            except Exception as e:
                logging.exception(e)
        return ret

    def queue_name(self):
        return self.node_name

    def get_port(self):
        if self.use_ssl:
            port = self.ssl_port
        else:
            port = self.port
        return port

    def __get_availiable_message_sender(self, reverse):
        hosts = self.hosts[:]
        if reverse:
            hosts.reverse()
        for host in hosts:
            try:
                caller = self.__create_message_sender(host)
                caller.ensure_connection()
                return caller
            except Exception as e:
                logging.exception(e)
        return None

    def __create_message_sender(self, host):
        caller = MessageSender(
            host,
            self.get_port(),
            self.user,
            self.password,
            self.use_ssl,
            self.socket_timeout
        )
        return caller
