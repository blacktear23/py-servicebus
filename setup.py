from distutils.core import setup

long_description = """A message bus framework for transfer messages between servers.
System is using RabbitMQ as a message broker.
"""

setup(name='servicebus',
      version='1.0',
      author='Rain Li',
      author_email='blacktear23@gmail.com',
      url='https://github.com/blacktear23/servicebus',
      download_url='https://github.com/blacktear23/servicebus',
      description='Message Bus use for message transfer',
      long_description=long_description,
      packages=['servicebus', 'servicebus.pika'],
      license='',
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python :: 2',
                   'Topic :: Message Bus',
                   'Topic :: Software Development :: Libraries',
                  ],
)