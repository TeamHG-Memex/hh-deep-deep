import argparse
import base64
import logging
import json
import pickle
from pprint import pformat
from typing import Dict, Optional
import zlib

from kafka import KafkaConsumer, KafkaProducer

from .utils import configure_logging, log_ignore_exception
from .crawl import CrawlProcess


class Service:
    input_topic = 'dd-trainer-input'
    output_topic = 'dd-trainer-output'

    def __init__(self, kafka_host=None, deep_deep_image=None):
        kafka_kwargs = {}
        if kafka_host is not None:
            kafka_kwargs['bootstrap_servers'] = kafka_host
        # Together with consumer_timeout_ms, this defines responsiveness.
        self.check_updates_every = 50
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
        counter = 0
        while True:
            counter += 1
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
            if counter % self.check_updates_every == 0:
                self.send_updates()
            self.consumer.commit()

    def send_result(self, topic: str, result: Dict) -> None:
        logging.info('Sending result for id "{}" to {}'
                     .format(result.get('id'), topic))
        self.producer.send(topic, result)
        self.producer.flush()

    @log_ignore_exception
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

    @log_ignore_exception
    def stop_crawl(self, request: Dict) -> None:
        id_ = request['id']
        process = self.running.get(id_)
        if process is not None:
            process.stop()
            self.running.pop(id_)
        else:
            logging.info('Crawl with id "{}" is not running'.format(id_))

    @log_ignore_exception
    def send_updates(self):
        for id_, process in self.running.items():
            updates = process.get_updates()
            if updates is not None:
                progress, page_urls = updates
                logging.info('Sending update for "{}": {}'.format(id_, progress))
                self.producer.send(
                    '{}-progress'.format(self.output_topic),
                    {
                        'id': id_,
                        'progress': progress,
                    })
                if page_urls:
                    logging.info('Sending {} sample urls for "{}"'
                                 .format(len(page_urls), id_))
                    self.producer.send(
                        '{}-pages'.format(self.output_topic),
                        {
                            'id': id_,
                            'page_samples': page_urls,
                        })
            new_model_data = process.get_new_model()
            if new_model_data is not None:
                self.producer.send(
                    '{}-model'.format(self.output_topic),
                    {
                        'id': id_,
                        'model': encode_model_data(new_model_data),
                    })


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
