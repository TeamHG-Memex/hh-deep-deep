import argparse
import base64
import concurrent.futures
import gzip
import hashlib
import logging
import json
from typing import Dict, Optional
import zlib

from kafka import KafkaConsumer, KafkaProducer

from .deepdeep_crawl import DeepDeepProcess
from .dd_crawl import DDCrawlerProcess
from .utils import configure_logging, log_ignore_exception


class Service:
    queue_prefix = ''
    jobs_prefix = ''
    max_message_size = 104857600

    def __init__(self,
                 queue_kind: str,
                 kafka_host: str=None,
                 check_updates_every: int=30,
                 debug: bool=False,
                 **crawler_process_kwargs):
        self.queue_kind = queue_kind
        self.required_keys = ['id', 'seeds'] + {
            'trainer': ['page_model'],
            'crawler': ['page_model', 'link_model'],
            }[queue_kind]
        self.process_class = {
            'trainer': DeepDeepProcess,
            'crawler': DDCrawlerProcess,
            }[queue_kind]
        kafka_kwargs = {}
        if kafka_host is not None:
            kafka_kwargs['bootstrap_servers'] = kafka_host
        # Together with consumer_timeout_ms, this defines responsiveness.
        self.check_updates_every = check_updates_every
        self.debug = debug
        self.input_topic = (
            '{}dd-{}-input'.format(self.queue_prefix, self.queue_kind))
        self.consumer = KafkaConsumer(
            self.input_topic,
            group_id='{}-group'.format(self.input_topic),
            consumer_timeout_ms=200,
            max_partition_fetch_bytes=self.max_message_size,
            **kafka_kwargs)
        self.producer = KafkaProducer(
            max_request_size=self.max_message_size,
            **kafka_kwargs)
        self.crawler_process_kwargs = dict(crawler_process_kwargs)
        if self.jobs_prefix:
            self.crawler_process_kwargs['jobs_prefix'] = self.jobs_prefix
        self.running = self.process_class.load_all_running(
            **self.crawler_process_kwargs)
        if self.running:
            for id_, process in sorted(self.running.items()):
                logging.info(
                    'Already running crawl "{id}", pid {pid} in {root}'
                    .format(id=id_,
                            pid=process.pid,
                            root=process.paths.root))
        else:
            logging.info('No crawls running')

    def run(self) -> None:
        counter = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            while True:
                counter += 1
                for message in self.consumer:
                    self._debug_save_message(message.value, 'incoming')
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
                    elif all(value.get(key) is not None
                             for key in self.required_keys):
                        executor.submit(self.start_crawl, value)
                    elif 'id' in value and value.get('stop'):
                        executor.submit(self.stop_crawl, value)
                    else:
                        logging.error('Dropping a message in unknown format: {}'
                                      .format(value.keys() if hasattr(value, 'keys')
                                              else type(value)))
                if counter % self.check_updates_every == 0:
                    self.send_updates()
                self.producer.flush()
                self.consumer.commit()

    @log_ignore_exception
    def start_crawl(self, request: Dict) -> None:
        id_ = request['id']
        logging.info(
            'Got start crawl message with id "{id}", {seeds} seeds, '
            'page model size {page_model:,} bytes{rest}.'.format(
                id=id_,
                seeds=len(request['seeds']),
                page_model=len(request['page_model']),
                rest=', link model size {:,}'.format(len(request['link_model']))
                     if request.get('link_model') else '',
            ))
        current_process = self.running.pop(id_, None)
        if current_process is not None:
            current_process.stop()
        kwargs = dict(self.crawler_process_kwargs)
        kwargs['seeds'] = request['seeds']
        kwargs['page_clf_data'] = decode_model_data(request['page_model'])
        if 'link_model' in request:
            kwargs['link_clf_data'] = decode_model_data(request['link_model'])
        if 'page_limit' in request:
            kwargs['page_limit'] = request['page_limit']
        process = self.process_class(id_=id_, **kwargs)
        process.start()
        self.running[id_] = process

    @log_ignore_exception
    def stop_crawl(self, request: Dict) -> None:
        id_ = request['id']
        logging.info('Got stop crawl message with id "{}"'.format(id_))
        process = self.running.get(id_)
        if process is not None:
            process.stop(verbose=request.get('verbose'))
            self.running.pop(id_)
        else:
            logging.info('Crawl with id "{}" is not running'.format(id_))

    @log_ignore_exception
    def send_updates(self):
        for id_, process in list(self.running.items()):
            if not process.is_running():
                logging.warning(
                    'Crawl should be running but it\'s not, stopping.')
                process.stop(verbose=True)
                self.running.pop(id_)
            else:
                updates = process.get_updates()
                self.send_progress_update(id_, updates)
                if hasattr(process, 'get_new_model'):
                    new_model_data = process.get_new_model()
                    if new_model_data is not None:
                        self.send_model_update(id_, new_model_data)

    def output_topic(self, kind: str) -> str:
        return '{}dd-{}-output-{}'.format(
            self.queue_prefix, self.queue_kind, kind)

    def send_progress_update(self, id_: str, updates):
        progress, page_sample = updates
        if progress is not None:
            progress_topic = self.output_topic('progress')
            logging.info('Sending update for "{}" to {}: {}'
                         .format(id_, progress_topic, progress))
            self.send(progress_topic, {'id': id_, 'progress': progress})
        if page_sample:
            pages_topic = self.output_topic('pages')
            logging.info('Sending {} sample urls for "{}" to {}'
                         .format(len(page_sample), id_, pages_topic))
            self.send(pages_topic, {'id': id_, 'page_sample': page_sample})

    def send_model_update(self, id_: str, new_model_data: bytes):
        encoded_model = encode_model_data(new_model_data)
        topic = self.output_topic('model')
        logging.info('Sending new model to {}, model size {:,} bytes'
                     .format(topic, len(encoded_model)))
        self.send(topic, {'id': id_, 'link_model': encoded_model})

    def send(self, topic: str, result: Dict):
        message = json.dumps(result).encode('utf8')
        self._debug_save_message(message, 'outgoing to {}'.format(topic))
        self.producer.send(topic, message).get()

    def _debug_save_message(self, message: bytes, kind: str) -> None:
        if self.debug:
            filename = ('hh-deep-deep-{}.json.gz'
                        .format(hashlib.md5(message).hexdigest()))
            logging.info('Saving {} message to {}'.format(kind, filename))
            with gzip.open(filename, 'wb') as f:
                f.write(message)


def encode_model_data(data: Optional[bytes]) -> Optional[str]:
    if data:
        return base64.b64encode(zlib.compress(data)).decode('ascii')


def decode_model_data(data: Optional[str]) -> bytes:
    if data is not None:
        return zlib.decompress(base64.b64decode(data))


def main():
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('kind', choices=['trainer', 'crawler'])
    arg('--docker-image', help='Name of docker image for the crawler')
    arg('--kafka-host')
    arg('--host-root', help='Pass host ${PWD} if running in a docker container')
    arg('--max-workers', type=int, help='Only for "crawler"')
    arg('--debug', action='store_true')
    args = parser.parse_args()

    configure_logging()
    cp_kwargs = {}
    if args.max_workers:
        cp_kwargs['max_workers'] = args.max_workers
    service = Service(
        args.kind, kafka_host=args.kafka_host, docker_image=args.docker_image,
        host_root=args.host_root, debug=args.debug,
        **cp_kwargs)
    logging.info('Starting hh dd-{} service'.format(args.kind))
    service.run()
