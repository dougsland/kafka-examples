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

        JSON_TAR_GZ = "{tmpdir}/{filename}-rhaccount{rhaccount}.tar.gz".format(
            tmpdir=tempfile.gettempdir(),
            filename="rhv-log-collector-analyzer",
            rhaccount=record['rh_account']
        )

        logging.info("Writing {fname}...".format(fname=JSON_TAR_GZ))

        with open(JSON_TAR_GZ, 'wb') as f:
            f.write(r.content)

        logging.info("Extracting {fname}...".format(fname=JSON_TAR_GZ))
        untar(JSON_TAR_GZ)

        # Renamed extracted JSON to show RH Account number
        JSON_FILE = "{tmpdir}/rhv_log_collector_analyzer_{rh}.json".format(
            tmpdir=tempfile.gettempdir(),
            rh=record['rh_account']
        )

        os.rename(
            "{tmpdir}/rhv_log_collector_analyzer.json".format(
                 tmpdir=tempfile.gettempdir()),
            JSON_FILE
        )

        os.unlink(JSON_TAR_GZ)
