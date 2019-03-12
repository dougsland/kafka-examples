#!/usr/bin/env python
import os
import json
import logging
import requests
import tarfile
import tempfile

from kafka import KafkaConsumer


def untar(fname):
    tar = tarfile.open(fname)
    tar.extractall(path=tempfile.gettempdir())
    tar.close()


if __name__ == '__main__':

    logging.basicConfig(level=logging.INFO)

    # Debug
    logging.info(
        "Using BOOTSTRAP_SERVERS: {btservers}"
        "CONSUME_TOPIC: {consume_topic}".format(
            btservers=os.environ.get('BOOTSTRAP_SERVERS'),
            consume_topic=os.environ.get('CONSUME_TOPIC')
        )
    )

    # Setup Consumer
    logging.info("Initiating Kafka connection for Consumer...")
    consumer = KafkaConsumer(
         os.environ.get('CONSUME_TOPIC'),
         bootstrap_servers=os.environ.get('BOOTSTRAP_SERVERS')
    )
    logging.info("Kafka consumer: {0}".format(consumer))

    # Consume things
    for msg in consumer:
        logging.info("Receiving message:")
        logging.info(msg)

        record = json.loads(msg.value)
        r = requests.get(record['url'])
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as err:
            logging.exception(err)

        fname = "{tmpdir}/{filename}-{rhaccount}.tar.gz".format(
            tmpdir=tempfile.gettempdir(),
            filename="rhv-log-collector-analyzer",
            rhaccount=record['rh_account']
        )

        logging.info("Writing {fname}...".format(fname=fname))

        with open(fname, 'wb') as f:
            f.write(r.content)

        logging.info("Extracting {fname}...".format(fname=fname))
        untar(fname)

        os.unlink(fname)
