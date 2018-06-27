#!/usr/bin/env python
import os
import logging
from confluent_kafka import Consumer, KafkaError
logging.basicConfig(level=logging.INFO)

# Debug
logging.info("Using BOOTSTRAP_SERVERS: %s" % (os.environ.get('BOOTSTRAP_SERVERS')))
logging.info("Using SUBSCRIPTION_TOPICS: %s" % (os.environ.get('SUBSCRIPTION_TOPICS')))

# Setup the Confluent Consumer
c = Consumer({
    'bootstrap.servers': os.environ.get('BOOTSTRAP_SERVERS'),
    'group.id': os.environ.get('GROUP_ID'),
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})

# Subscribe to our topics
topic_subscriptions = os.environ.get('SUBSCRIPTION_TOPICS').split(',')
c.subscribe(topic_subscriptions)


# Poll the topics
while True:
    msg = c.poll(1.0)

    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            logging.info(msg.error())
            break

    logging.info('Received message: {}'.format(msg.value().decode('utf-8')))
    logging.info(msg)

# Close connection
c.close()
