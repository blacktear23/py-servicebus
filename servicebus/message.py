import time
import uuid
import logging
import thread
import asyncore
from servicebus import pika
from servicebus.command import cmd
from datetime import datetime

DIDA_TIMEOUT = 60

class TimeoutException(Exception):
    pass

class RabbitMQMessageDriver(object):
    """docstring for AbstractMessageReceiver"""
    def __init__(self, host, port, username, password, ssl=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.connection = self.create_connection()
        self.channel = self.connection.channel()
    
    def create_connection(self):
        connection = pika.AsyncoreConnection(pika.ConnectionParameters(
            self.host,
            self.port,
            credentials=pika.PlainCredentials(self.username, self.password),
            ssl=self.ssl
        ))
        return connection
        
    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self.channel.exchange_declare(exchange=exchange_name, type=exchange_type)
        
    def close(self):
        self.channel.close()
        self.connection.close()
        
    def bind_queue_to_exchange(self, queue_name, exchange_name, exchange_type='direct'):
        self.queue_name = queue_name
        self.declare_exchange(exchange_name, exchange_type)
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

class AbstractReceiver(RabbitMQMessageDriver):
    # this mothod just receive one message.
    # only used by test.
    def receive_one(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__on_receive, queue=self.queue_name)
        pika.asyncore_loop(count=1)

    def response_message(self, channel, method, header, message):
        channel.basic_publish(exchange='',
            routing_key=header.reply_to,
            properties=pika.BasicProperties(correlation_id=header.correlation_id),
            body=str(message))
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def on_rpc(self, channel, method, header, body):
        self.response_message(channel, method, header, None)

    def on_message(self, channel, method, header, body):
        pass
            
    def start_receive(self, count=None):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__on_receive, queue=self.queue_name)
        pika.asyncore_loop()
    
    def __on_receive(self, channel, method, header, body):
        try:
            if hasattr(header, 'reply_to') and header.reply_to != None:
                # Here is a RPC call
                if body == "PING":
                    self.response_message(channel, method, header, "PONG")
                else:
                    self.on_rpc(channel, method, header, body)
            else:
                # Here is just send a message
                channel.basic_ack(delivery_tag=method.delivery_tag)
                self.on_message(channel, method, header, body)
        except Exception, e:
            logging.error(e)
            
class AbstractMessageSender(RabbitMQMessageDriver):
    def set_exchange(self, exchange_name, exchange_type='direct'):
        self.exchange_name = exchange_name
        self.declare_exchange(exchange_name, exchange_type)

    def send(self, target, msg):
        try:
            if not self.connection.is_alive():
                self.connection = self.create_connection()
                self.channel = self.connection.channel()
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=str(target), body=msg)
        finally:
            self.close()
        
class MessageSender(AbstractMessageSender):
    def on_response(self, ch, method, props, body):
        if props.correlation_id == self.corr_id:
            self.response = body
            
    def ensuer_connection(self):
        if not self.connection.is_alive():
            self.connection = self.create_connection()
            self.channel = self.connection.channel()
            
    def call(self, target, msg, timeout=300):
        self.ensuer_connection()
        try:
            result = self.channel.queue_declare(exclusive=True)
            self.callback_queue = result.queue
            self.response = None
            self.corr_id = str(uuid.uuid4())
            self.channel.basic_publish(exchange=self.exchange_name,
                routing_key=str(target),
                properties=pika.BasicProperties(
                    reply_to=self.callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=msg)
            self.channel.basic_consume(self.on_response, queue=self.callback_queue, no_ack=True)
            t_start = datetime.now()
            while self.response is None:
                if timeout:
                    pika.asyncore_loop(count=1, timeout=timeout)
                else:
                    pika.asyncore_loop(count=1)
                t_end = datetime.now()
                t_spend = (t_end - t_start).seconds
                logging.debug("Spend %s sec, timeout: %s" % (t_spend, timeout))
                if timeout and t_spend >= timeout:
                    raise TimeoutException("RPC Call Timeout")
            return self.response
        finally:
            # A call is finish delete the queue
            self.channel.queue_delete(queue=self.callback_queue)

class PingWatcher:
    def __init__(self, receiver):
        self.receiver = receiver

    def do_watch(self):
        ip = self.receiver.host
        while True:
            time.sleep(DIDA_TIMEOUT)
            ret = self.ping(ip)
            if not ret and self.receiver.connected:
                self.receiver.connected = False
                logging.info("Ping Error!")
                for value in asyncore.socket_map.values():
                    logging.info("Close Socket: %s" % str(value))
                    value.socket.close()
                logging.info("Clean socket map!")
                asyncore.socket_map = {}
                # we close all connections then we do not to watch now.
                return

    def do_ping(self, ip):
        command = "ping -c 1 %s | grep \" 1 received,\"" % (ip)
        ret = cmd(command)[0]
        return ret != ""
    
    def ping(self, ip):
        for i in range(3):
            ret = self.do_ping(ip)
            if ret: return True
            time.sleep(1)
        return False

    def run_watch(self, ignore):
        while True:
            try:
                self.do_watch()
            except Exception, e:
                logging.error(e)

    @classmethod
    def start_watch(cls, receiver):
        pw = PingWatcher(receiver)
        thread.start_new_thread(pw.run_watch, (None,))