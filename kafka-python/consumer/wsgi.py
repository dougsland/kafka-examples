#!/usr/bin/env python
import os
import logging
#from kafka import KafkaConsumer
logging.basicConfig(level=logging.INFO)

# Debug
logging.info("Using BOOTSTRAP_SERVERS: %s" % (os.environ.get('BOOTSTRAP_SERVERS')))
logging.info("Using CONSUME_TOPIC: %s" % (os.environ.get('CONSUME_TOPIC')))

# Setup Consumer
logging.info("Initiating Kafka connection for Consumer...")
#consumer = KafkaConsumer(os.environ.get('CONSUME_TOPIC'),
#						 bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS'))
logging.info("Kafka consumer:")
#logging.info(consumer)

# Consume things
#for msg in consumer:
#	logging.info("Receiving message:")
#	logging.info(msg)
