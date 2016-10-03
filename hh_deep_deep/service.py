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
from .crawl import CrawlProcess


class Service:
    input_topic = 'dd-trainer-input'
    output_topic = 'dd-trainer-output'

    def __init__(self, kafka_host=None, deep_deep_image=None):
        kafka_kwargs = {}
        if kafka_host is not None:
            kafka_kwargs['bootstrap_servers'] = kafka_host
        self.consumer = KafkaConsumer(
            self.input_topic,
            consumer_timeout_ms=200,
            **kafka_kwargs)
        self.producer = KafkaProducer(
            value_serializer=encode_message,
            **kafka_kwargs)
        self.cp_kwargs = {'deep_deep_image': deep_deep_image}
        self.running = CrawlProcess.load_all_running(**self.cp_kwargs)

    def run(self) -> None:
        while True:
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
                    return
                elif all(key in value for key in ['id', 'page_model', 'seeds']):
                    logging.info(
                        'Got start crawl message with id "{}"'.format(value['id']))
                    self.start_crawl(value)
                elif 'id' in value and value.get('stop'):
                    logging.info(
                        'Got stop crawl meessage with id "{}"'.format(value['id']))
                    self.stop_crawl(value)
                else:
                    logging.error(
                        'Dropping a message in unknown format: {}'
                        .format(pformat(value)))
            self.send_updates()
            self.consumer.commit()

    def send_result(self, topic: str, result: Dict) -> None:
        logging.info('Sending result for id "{}" to {}'
                     .format(result.get('id'), topic))
        self.producer.send(topic, result)
        self.producer.flush()

    def start_crawl(self, request: Dict) -> None:
        id_ = request['id']
        current_process = self.running.get(id_)
        if current_process is not None:
            current_process.stop()
        seeds = request['seeds']
        page_clf_data = decode_model_data(request['page_model'])
        process = CrawlProcess(
            id_=id_, seeds=seeds, page_clf_data=page_clf_data,
            **self.cp_kwargs)
        process.start()
        self.running[id_] = process

    def stop_crawl(self, request: Dict) -> None:
        id_ = request['id']
        process = self.running.get(id_)
        if process is not None:
            process.stop()
            self.running.pop(id_)
        else:
            logging.info('Crawl with id "{}" is not running'.format(id_))

    def send_updates(self):
        pass # TODO


def encode_message(message: Dict) -> bytes:
    try:
        return json.dumps(message).encode('utf8')
    except Exception as e:
        logging.error('Error serializing message', exc_info=e)
        raise


def encode_model_data(data: Optional[bytes]) -> Optional[str]:
    if data:
        return base64.b64encode(zlib.compress(data)).decode('ascii')


def decode_model_data(data: Optional[str]) -> bytes:
    if data is not None:
        return zlib.decompress(base64.b64decode(data))


def main():
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('--kafka-host')
    arg('--deep-deep-image', default='deep-deep',
        help='Name of docker image for deep-deep')
    args = parser.parse_args()

    configure_logging()
    service = Service(
        kafka_host=args.kafka_host, deep_deep_image=args.deep_deep_image)
    logging.info('Starting hh deep-deep service')
    service.run()
