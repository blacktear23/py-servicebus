import uuid
import signal
import logging
from datetime import datetime
from servicebus import pika
from servicebus import utils
from servicebus.watcher import PingWatcher


class TimeoutException(Exception):
    pass


class RabbitMQMessageDriver(object):
    """docstring for AbstractMessageReceiver"""
    def __init__(self, host, port, username, password, ssl=False, socket_timeout=5, heartbeat_interval=60):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.ssl = ssl
        self.connection = None
        self.channel = None
        self.running = True
        self.connected = False
        self.socket_timeout = socket_timeout
        self.heartbeat_interval = heartbeat_interval

    def create_connection(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(
            self.host,
            self.port,
            credentials=pika.PlainCredentials(self.username, self.password),
            ssl=self.ssl,
            heartbeat_interval=self.heartbeat_interval,
            socket_timeout=self.socket_timeout
        ))
        self.connected = True
        return connection

    def declare_exchange(self, exchange_name, exchange_type='direct'):
        self.channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)

    def close(self):
        if self.connected:
            self.connected = False
            self.connection.close()

    def bind_queue_to_exchange(self, queue_name, exchange_name, exchange_type='direct'):
        self.queue_name = queue_name
        self.declare_exchange(exchange_name, exchange_type)
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key=queue_name)

    def ensure_connection(self):
        if not self.connected:
            self.connection = self.create_connection()
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
                self.running = False
                self.connected = False
                self.__safe_close()
            except EnvironmentError as e:
                if e.errno == 4 and not self.running:
                    logging.info("Got SIGTERM or SIGINT")
                    self.running = False
                    self.connected = False
                    self.__safe_close()
                else:
                    self.connected = False
                    self.__safe_close()
                    raise e
            except Exception as e:
                logging.exception(e)
                self.connected = False
                self.__safe_close()
                raise e
        if not self.connected:
            self.connected = False
            self.__safe_close()
        logging.info("All connection closed!")

    def __safe_close(self):
        try:
            self.channel.stop_consuming()
        except Exception:
            pass
        try:
            self.channel.close()
        except Exception:
            pass
        try:
            self.connection.close()
        except Exception:
            pass
        try:
            self.connection.close_ioloop()
        except Exception:
            pass

    def response_message(self, channel, method, header, message):
        channel.basic_publish(exchange='',
                              routing_key=header.reply_to,
                              properties=pika.BasicProperties(correlation_id=header.correlation_id),
                              body=message.encode())

    def on_rpc(self, channel, method, header, body):
        self.response_message(channel, method, header, None)

    def on_message(self, channel, method, header, body):
        pass

    def start_receive(self, count=None):
        self.channel.basic_qos(prefetch_count=1)
        self.channel.basic_consume(self.__on_receive, queue=self.queue_name)
        self.watcher = PingWatcher.start_watch(self)
        try:
            self.loop()
            return self.running
        finally:
            self.watcher.stop()

    def process_exit(self, signum, frame):
        logging.info("Shutdown")
        self.running = False
        self.connected = False
        self.__safe_close()

    def join_watcher(self):
        try:
            if self.watcher:
                self.watcher.join()
        except Exception:
            pass

    def __on_receive(self, channel, method, header, body):
        try:
            channel.basic_ack(delivery_tag=method.delivery_tag)
            ebody = body.decode()
            if hasattr(header, 'reply_to') and header.reply_to is not None:
                # Here is a RPC call
                if ebody == "PING":
                    self.response_message(channel, method, header, "PONG")
                else:
                    self.on_rpc(channel, method, header, ebody)
            else:
                # Here is just send a message
                self.on_message(channel, method, header, ebody)
        except Exception as e:
            logging.exception(e)


class AbstractMessageSender(RabbitMQMessageDriver):
    def set_exchange(self, exchange_name, exchange_type='direct'):
        self.exchange_name = exchange_name
        self.declare_exchange(exchange_name, exchange_type)

    def send(self, target, msg):
        try:
            self.ensure_connection()
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=str(target), body=msg)
        finally:
            self.close()


class MessageSender(AbstractMessageSender):
    def on_response(self, ch, method, props, body):
        if props.correlation_id == self.corr_id:
            self.response = body.decode()
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
                self.channel.stop_consuming()
                self.timeout = True

            self.timeout_id = self.connection.add_timeout(timeout, timeout_callback)
            self.channel.start_consuming()

            if self.timeout:
                self.timeout_id = None
                raise Exception("Timeout")
            return self.response
        except Exception as e:
            logging.exception(e)
            raise e
        finally:
            if self.timeout_id:
                self.connection.remove_timeout(self.timeout_id)
