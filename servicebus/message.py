import uuid
import signal
import logging
import asyncore
from datetime import datetime
from servicebus import pika
from servicebus import utils
from servicebus.watcher import PingWatcher


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
        self.connection = None
        self.channel = None
        self.running = True
        self.connected = False

    def create_connection(self, timeout=300):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            self.host,
            self.port,
            credentials=pika.PlainCredentials(self.username, self.password),
            ssl=self.ssl,
            socket_timeout=timeout
        ))
        self.connected = True
        return connection

    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self.channel.exchange_declare(exchange=exchange_name, type=exchange_type)

    def close(self):
        if self.connected:
            self.connected = False
            self.connection.close()

    def bind_queue_to_exchange(self, queue_name, exchange_name, exchange_type='direct'):
        self.queue_name = queue_name
        self.declare_exchange(exchange_name, exchange_type)
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

    def ensure_connection(self, timeout=300):
        if not self.connected:
            self.connection = self.create_connection(timeout)
            self.channel = self.connection.channel()


class AbstractReceiver(RabbitMQMessageDriver):
    # this mothod just receive one message.
    # only used by test.
    def receive_one(self):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__on_receive, queue=self.queue_name)

    def loop(self):
        while self.connected:
            try:
                self.channel.start_consuming()
            except KeyboardInterrupt:
                self.connected = False
                self.running = False
                self.channel.stop_consuming()
        if not self.connected:
            self.connected = False
            self.channel.stop_consuming()
        logging.info("All connection closed!")

    def response_message(self, channel, method, header, message):
        channel.basic_publish(exchange='',
                              routing_key=header.reply_to,
                              properties=pika.BasicProperties(correlation_id=header.correlation_id),
                              body=str(message))

    def on_rpc(self, channel, method, header, body):
        self.response_message(channel, method, header, None)

    def on_message(self, channel, method, header, body):
        pass

    def start_receive(self, count=None):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__on_receive, queue=self.queue_name)
        PingWatcher.start_watch(self)
        signal.signal(signal.SIGTERM, self.process_exit)
        self.loop()
        return self.running

    def process_exit(self, signum, frame):
        logging.info("Shutdown")
        self.running = False
        self.connected = False
        self.channel.stop_consuming()
        self.close()

    def __on_receive(self, channel, method, header, body):
        try:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            if hasattr(header, 'reply_to') and header.reply_to is not None:
                # Here is a RPC call
                if body == "PING":
                    self.response_message(channel, method, header, "PONG")
                else:
                    self.on_rpc(channel, method, header, body)
            else:
                # Here is just send a message
                self.on_message(channel, method, header, body)
        except Exception as e:
            logging.exception(e)


class AbstractMessageSender(RabbitMQMessageDriver):
    def set_exchange(self, exchange_name, exchange_type='direct'):
        self.exchange_name = exchange_name
        self.declare_exchange(exchange_name, exchange_type)

    def send(self, target, msg):
        try:
            if not self.connected:
                self.connection = self.create_connection()
                self.channel = self.connection.channel()
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=str(target), body=msg)
        finally:
            self.close()


class MessageSender(AbstractMessageSender):
    def on_response(self, ch, method, props, body):
        if props.correlation_id == self.corr_id:
            self.response = body
            self.connection.remove_timeout(self.timeout_id)
            self.timeout_id = None
            self.channel.stop_consuming()

    def call(self, target, msg, timeout=300):
        self.ensure_connection()
        self.response = None
        self.timeout = False
        self.timeout_id = None
        try:
            self.corr_id = str(uuid.uuid4())
            result = self.channel.queue_declare(exclusive=True)
            callback_queue = result.method.queue
            self.channel.basic_publish(
                exchange=self.exchange_name,
                routing_key=str(target),
                properties=pika.BasicProperties(
                    reply_to=callback_queue,
                    correlation_id=self.corr_id,
                ),
                body=msg)
            self.channel.basic_consume(self.on_response, queue=callback_queue)

            def timeout_callback():
                print "timeout"
                self.channel.stop_consuming()
                self.timeout = True

            self.timeout_id = self.connection.add_timeout(timeout, timeout_callback)
            self.channel.start_consuming()
            if self.timeout:
                self.timeout_id = None
                raise Exception("Timeout")
            if self.response is None:
                raise Exception("Response is None")
            return self.response
        except Exception, e:
            logging.exception(e)
            raise e
        finally:
            if self.timeout_id:
                self.connection.remove_timeout(self.timeout_id)
