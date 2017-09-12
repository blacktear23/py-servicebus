# Service Bus use RabbitMQ

Provide a basic RPC and Message process framework based on RabbitMQ message server.

Features:

1. Auto reconnect when network is down
2. Support SSL and dynamic token validation in message transfer
3. Use multi-path to auto switch RabbitMQ server when major RabbitMQ server is down
4. Python 2 and Python 3 support

## Archtecture

```
                           +---------+
                           | Message |
                           | Sender  |
                           +---------+
                                |
                      +---------+--------+
                      |                  |
                      V                  V
                +----------+        +----------+
                | RabbitMQ |        | RabbitMQ |
                |  Master  |        |  Slave   |
                +----------+        +----------+
                      ^                   ^
                      |                   |
              +------------------+-------------------+
              | Queue A          | Queue B           | Queue C
        +---------+         +---------+         +---------+
        |  Agent  |         |  Agent  |         |  Agent  |
        | Server  |         | Server  |         | Server  |
        +---------+         +---------+         +---------+
```

### Message Sender

Message Sender will send the message to Agent Server. Sender use specific Queue Name and Service Name to determin this message is send to which Agent Server's Service

Message Sender can have many RabbitMQ server's host when Message Sender need to connect to RabbitMQ server, it will use first connectable server. If none of then can connect it will raise an exception.

### Agent Server

Agent Server will connect to RabbitMQ server and listen a Queue to take messages. In this framework, Agent Server will be fork N process (N is number of RabbitMQ servers) to listen each RabbitMQ server's Queue, so that can handle when one RabbitMQ server is down, Agent can take messages from Other RabbitMQ server.

### Service

Service is business logic code. There will be two type of service: RPC Service and Message Service.
RPC Service will get message process process it then response a result message to sender provided tempory Queue.

Message Service will get message and process it but no need to send a result message to sender.

Each Service has two params to name it: category and name. When we use Sender to call this service you can use: NODE_NAME.CATEGORY.NAME to address the service.

In one agent server process, each Service will has it’s own thread. This design will let client call more than one service in concurrent.

### How it work when RabbitMQ is DOWN

In Agent Server, when your configuration has more than one RabbitMQ server host for example 2, Agent Server will fork 2 process (AS-A and AS-B) and connect to each RabbitMQ server(RMQ-A and RMQ-B). So if one RabbitMQ server(RMQ-A) is down, AS-A process will try to reconnect to RabbitMQ server RMQ-A and in Message Sender part it will find RMQ-A is not connectable, so Message Sender will connect to RMQ-B and send message via RMQ-B. Then this call message will process in AS-B process.

### How it work when network is Broken

In Agent Server, you will have a PingWatcher to ping each of your RabbitMQ server. If you have a network problem it will close all connection and trigger reconnect logic in Agent Server.


## Message Format

There has two type of message: Call Message and Response Message

Call Message:

```xml
<?xml version="1.0"?>
<event>
    <id>EVENT_ID</id>
    <token>EVENT_TOKEN</token>
    <category>SERVICE_CATEGORY</category>
    <service>SERVICE_NAME</service>
    <params><![CDATA[JSON_FORMAT_PARAMS]]></params>
</event>
```

Response Message:

```xml
<?xml version="1.0"?>
<response>
    <id>EVENT_ID</id>
    <message><![CDATA[JSON_FORMAT_MESSAGE]]></message>
</response>
```

## Usage

### Install

```bash
python setup.py install
```

or

```bash
pip install py-servicebus
```

### Agent part

Write a Service:

```python
class AddService:
    def on_call(self, request, response):
        params = request.get_params()
        ret = 0
        for i in params:
            ret += int(i)
        response.send(ret)
```

Then regist it to ServiceBus and run it:

```python
from servicebus.service import ServiceBus
from servicebus.configuration import Configuration

config = Configuration({
    'hosts': ['localhost'],
    'user': 'admin',
    'password': '123456',
    'use_ssl': False,
    'node_name': "NODE-01",
    'secret_token': 'secret token',
})
sbus = ServiceBus(config)
sbus.add_rpc_service("math", "add", AddService())
sbus.run_services()
```

### Call part

If we want to call NODE-01's math.add service, the code should be:

```python
from servicebus.configuration import Configuration
from servicebus.sender import Sender

config = Configuration({
    'hosts': ['localhost'],
    'user': 'admin',
    'password': '123456',
    'use_ssl': False,
    'node_name': "NODE-01",
    'secret_token': 'secret token',
})
sender = Sender(config)
ret = sender.call('NODE-01.math.add', [1, 2])
print ret
```

Then ret will be (1, 3). Sender#call will return a tuple, it contains 2 items first is Event ID second is result that Service return.

## Logging Service

In new version, py-servicebus add LoggingService class to provide UDP based multi-process logging service. If we use basic file handler for logging at multi-process program, you will get some problem on logging system, such as no logging message write to logging file.

py-servicebus LoggingService is design to fix this problem. LoggingService just use UDP to serve logging message and write those messages to a time rotated files. And it also very easy to use:
```python
import os
import time
import logging
from servicebus.logging_service import LoggingService

def write_pid():
    with open('/var/run/application.pid', 'a+') as fp:
        fp.write('%s\n' % str(os.getpid()))

LoggingService.start_logging_server('/var/log/application.log', 9999, write_pid)
time.sleep(1)
LoggingService.init_logging(9999, logging.INFO)
# Other codes that fork others processes...
```
