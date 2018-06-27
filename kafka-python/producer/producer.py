#!/usr/bin/env python
import os
import logging
import time
from kafka import KafkaProducer
logging.basicConfig(level=logging.INFO)

# Debug
logging.info("Using BOOTSTRAP_SERVERS: %s" % (os.environ.get('BOOTSTRAP_SERVERS')))
logging.info("Using PRODUCE_TOPIC: %s" % (os.environ.get('PRODUCE_TOPIC')))

# Setup Producer
logging.info("Initiating Kafka connection for Producer...")
producer = KafkaProducer(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
logging.info("Kafka producer:")
logging.info(producer)

# Send messages
logging.info("Sending consumer hello message.")
producer.send(os.environ.get('PRODUCE_TOPIC'), 'Hello consumer!')
message_count = 0
while True:
	time.sleep(5)
	message_count = message_count + 1
	logging.info("Sending consumer message count %s." % (message_count))
	producer.send(os.environ.get('PRODUCE_TOPIC'), b'I have sent you %s messages' % (message_count))
