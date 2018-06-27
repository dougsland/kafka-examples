#!/usr/bin/env python
import os
import logging
import time
from kafka import KafkaProducer, KafkaConsumer
logging.basicConfig(level=logging.INFO)

# Debug
logging.info("Using BOOTSTRAP_SERVERS: %s" % (os.environ.get('BOOTSTRAP_SERVERS')))
logging.info("Using CONSUME_TOPIC: %s" % (os.environ.get('CONSUME_TOPIC')))
logging.info("Using PRODUCE_TOPIC: %s" % (os.environ.get('PRODUCE_TOPIC')))

# Setup Producer
logging.info("Initiating Kafka connection for Producer...")
producer = KafkaProducer(bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
logging.info("Kafka producer:")
logging.info(producer)

# Setup Consumer
logging.info("Initiating Kafka connection for Consumer...")
consumer = KafkaConsumer(os.environ.get('CONSUME_TOPIC'),
						 bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
logging.info("Kafka consumer:")
logging.info(consumer)

# Consume/Produce things
for msg in consumer:
	logging.info("Receiving message:")
	logging.info(msg)
	logging.info("Sending message to '%s'." % (os.environ.get('PRODUCE_TOPIC')))
	producer.send(os.environ.get('PRODUCE_TOPIC'), msg)
