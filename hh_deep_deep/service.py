import argparse
import base64
import logging
import json
import pickle
from pprint import pformat
from typing import Dict, Optional
import zlib

from kafka import KafkaConsumer, KafkaProducer

from .utils import configure_logging


class Service:
    input_topic = 'dd-modeler-input'
    output_topic = 'dd-modeler-output'

    def __init__(self, kafka_host=None):
        kafka_kwargs = {}
        if kafka_host is not None:
            kafka_kwargs['bootstrap_servers'] = kafka_host
        self.consumer = KafkaConsumer(
            self.input_topic,
            **kafka_kwargs)
        self.producer = KafkaProducer(
            value_serializer=encode_message,
            **kafka_kwargs)
        self.stop_marker = object()

    def run(self) -> None:
        for message in self.consumer:
            try:
                value = json.loads(message.value.decode('utf8'))
            except Exception as e:
                logging.error('Error decoding message: {}'
                              .format(repr(message.value)),
                              exc_info=e)
                continue
            if value == {'from-tests': 'stop'}:
                logging.info('Got message to stop (from tests)')
                return self.stop_marker
            elif all(key in value for key in ['id', 'page_model', 'seeds']):
                self.start_crawl(value)
            elif 'id' in value and value.get('stop'):
                self.stop_crawl(value)
            else:
                logging.error(
                    'Dropping a message in unknown format: {}'
                    .format(pformat(value)))

    def send_result(self, topic: str, result: Dict) -> None:
        logging.info('Sending result for id "{}" to {}'
                     .format(result.get('id'), topic))
        self.producer.send(topic, result)
        self.producer.flush()

    def start_crawl(self, request: Dict) -> None:
        pass  # TODO

    def stop_crawl(self, request: Dict) -> None:
        pass  # TODO


def encode_message(message: Dict) -> bytes:
    try:
        return json.dumps(message).encode('utf8')
    except Exception as e:
        logging.error('Error serializing message', exc_info=e)
        raise


def encode_model(model: object) -> Optional[str]:
    if model is not None:
        return (
            base64.b64encode(
                zlib.compress(
                    pickle.dumps(model, protocol=pickle.HIGHEST_PROTOCOL)))
            .decode('ascii'))


def decode_model(data: Optional[str]) -> object:
    if data is not None:
        return pickle.loads(zlib.decompress(base64.b64decode(data)))


def main():
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('--kafka-host')
    args = parser.parse_args()

    configure_logging()
    service = Service(kafka_host=args.kafka_host)
    logging.info('Starting hh deep-deep service')
    service.run()
