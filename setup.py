from distutils.core import setup

long_description = """Provide a basic RPC and Message process framework based on RabbitMQ message server.

Features:
      1. Auto reconnect when network is down
      2. Support SSL and dynamic token validation in message transfer
      3. Use multi-path to auto switch RabbitMQ server when major RabbitMQ server is down
"""

setup(
    name='py-servicebus',
    version='2.15',
    author='Rain Li',
    author_email='blacktear23@gmail.com',
    url='https://github.com/blacktear23/py-servicebus',
    download_url='https://github.com/blacktear23/py-servicebus/downloads',
    description='Provide a basic RPC and Message process framework based on RabbitMQ message server.',
    long_description=long_description,
    packages=['servicebus', 'servicebus.pika', 'servicebus.pika.adapters'],
    license='',
    classifiers=['Development Status :: 5 - Production/Stable',
                 'Intended Audience :: Developers',
                 'Operating System :: OS Independent',
                 'License :: OSI Approved :: BSD License',
                 'Programming Language :: Python :: 2.7',
                 'Programming Language :: Python :: 3',
                 'Programming Language :: Python :: Implementation :: CPython',
                 'Topic :: Software Development :: Libraries',
                 ]
)
