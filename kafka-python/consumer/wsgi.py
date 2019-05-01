#!/usr/bin/env python
import base64
import json
import os
import logging
import requests
import tarfile
import tempfile

from kafka import KafkaConsumer


logging.basicConfig(level=logging.INFO)

# TODO:
# parse JSON
# send to insights advisor the message
# remove JSON_FILE after parsed


def parse_json(jsonfile):
    with open(jsonfile, 'r') as f:
        json_file = f.read()

    json_data = json.loads(json_file)

    for data in json_data['rhv-log-collector-analyzer']:
        logging.info("Description: {0}".format(data['description']))
        logging.info("Knowledge Base: {0}".format(data['kb']))

        for result in data['result']:
            for info in result:
                # Converting to string for now, Insights API might expect
                # different data
                logging.info(
                    ', '.join("{0} {1}".format(
                        key, val) for (key, val) in info.items())
                )

        logging.info("=========")


def create_host(
    account_number,
    insights_id,
    bios_uuid,
    fqdn,
    ip_addresses
):
    """
    Create/Update host in the inventory
    """
    URL = "http://insights-inventory.platform-ci.svc:8080/api/inventory/v1/hosts"
    # URL = "http://insights-inventory.platform-prod.svc:8080/api/inventory/v1/hosts"

    headers = {'Content-type': 'application/json'}
    identity = {'identity': {'account_number': account_number}}
    headers["x-rh-identity"] = base64.b64encode(json.dumps(identity).encode())

    payload = {
        "account": account_number,
        "insights_id": insights_id,
        "bios_uuid": bios_uuid,
        "fqdn": fqdn,
        "display_name": fqdn,
        "ip_addresses": ip_addresses
    }

    logging.info("payload: {0}".format(payload))
    json_payload = json.dumps([payload])
    logging.info(json_payload)

    logging.info(URL)
    r = requests.post(URL, data=json_payload, headers=headers, verify=False)
    logging.info("response: {0}".format(r.text))
    logging.info("status_code {0}".format(r.status_code))

    results = json.loads(r.text)
    return results["data"][0]["host"]["id"]



def untar(fname):
    tar = tarfile.open(fname)
    tar.extractall(path=tempfile.gettempdir())
    tar.close()


if __name__ == '__main__':

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
        logging.info("JSON available: {0}".format(JSON_FILE))

        host_id = create_host(
            record["rh_account"],
            record["metadata"]["insights_id"],
            record["metadata"]["bios_uuid"],
            record["metadata"]["fqdn"],
            record["metadata"]["ip_addresses"],
        )

        parse_json(JSON_FILE)
        producer.send(os.environ.get('PRODUCE_TOPIC'), 'Hello consumer!')

        logging.info("Removing {0}".format(JSON_TAR_GZ))
        os.unlink(JSON_TAR_GZ)
