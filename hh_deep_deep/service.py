import argparse
import base64
import concurrent.futures
import gzip
import hashlib
import logging
import json
import time
from typing import Any, Dict, Optional
import zlib

from kafka import KafkaConsumer, KafkaProducer

from .crawl_utils import CrawlProcess
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
        self.required_keys = ['id', 'workspace_id', 'seeds'] + {
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

        topic = lambda x: '{}{}'.format(self.queue_prefix, x)
        self.input_topic = topic('dd-{}-input'.format(self.queue_kind))
        self.consumer = self._kafka_consumer(
            self.input_topic,
            consumer_timeout_ms=200,
            max_partition_fetch_bytes=self.max_message_size,
            **kafka_kwargs)
        if self.queue_kind == 'crawler':
            self.login_output_topic = topic('dd-login-input')
            self.login_input_topic = topic('dd-login-output')
            self.login_consumer = self._kafka_consumer(
                self.login_input_topic, **kafka_kwargs)
            self.hints_input_topic = topic('dd-crawler-hints-input')
            self.hints_consumer = self._kafka_consumer(
                self.hints_input_topic, **kafka_kwargs)
        else:
            self.hints_consumer = self.logn_consumer = None

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

    def _kafka_consumer(self, topic, consumer_timeout_ms=10, **kwargs):
        return KafkaConsumer(
            topic, group_id='{}-group'.format(topic),
            consumer_timeout_ms=consumer_timeout_ms,
            **kwargs)

    def run(self) -> None:
        counter = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            while True:
                counter += 1
                for value in self._read_consumer(self.consumer):
                    if value == {'from-tests': 'stop'}:
                        logging.info('Got message to stop (from tests)')
                        return
                    elif all(value.get(key) is not None
                             for key in self.required_keys):
                        executor.submit(self.start_crawl, value)
                    elif 'id' in value and value.get('stop'):
                        executor.submit(self.stop_crawl, value)
                    else:
                        logging.error(
                            'Dropping a message in unknown format: {}'
                            .format(value.keys() if hasattr(value, 'keys')
                                    else type(value)))
                for value in self._read_consumer(self.hints_consumer):
                    executor.submit(self.handle_hint, value)
                for value in self._read_consumer(self.login_consumer):
                    executor.submit(self.handle_login, value)
                if counter % self.check_updates_every == 0:
                    executor.submit(self.send_updates)
                self.producer.flush()
                self.consumer.commit()

    def _read_consumer(self, consumer):
        if consumer is None:
            return
        for message in consumer:
            self._debug_save_message(message.value, 'incoming')
            try:
                yield json.loads(message.value.decode('utf8'))
            except Exception as e:
                logging.error('Error decoding message: {}'
                              .format(repr(message.value)),
                              exc_info=e)

    @log_ignore_exception
    def start_crawl(self, request: Dict) -> None:
        id_ = request['id']
        workspace_id = request['workspace_id']
        logging.info(
            'Got start crawl message with id "{id}", {seeds} seeds, '
            'page model size {page_model:,} bytes{rest}.'.format(
                id=id_,
                seeds=len(request['seeds']),
                page_model=len(request['page_model']),
                rest=', link model size {:,}'.format(len(request['link_model']))
                     if request.get('link_model') else '',
            ))
        # stop all running processes for this workspace
        # (should be at most 1 usually)
        for p_id, process in list(self.running.items()):
            if process.workspace_id == workspace_id:
                logging.info('Stopping old process {} for workspace {}'
                             .format(p_id, workspace_id))
                process.stop()
                self.running.pop(p_id, None)
                self.send_stopped_message(process)
        kwargs = dict(self.crawler_process_kwargs)
        kwargs['seeds'] = request['seeds']
        kwargs['page_clf_data'] = decode_model_data(request['page_model'])
        if 'link_model' in request:
            kwargs['link_clf_data'] = decode_model_data(request['link_model'])
        kwargs.update({field: request[field]
                       for field in ['page_limit', 'hints', 'broadness']
                       if field in request})
        if 'page_limit' in request:
            kwargs['page_limit'] = request['page_limit']
        if 'hints' in request:
            kwargs['hints'] = request['hints']
        process = self.process_class(
            id_=id_, workspace_id=workspace_id, **kwargs)
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
                self.send_stopped_message(process)
            else:
                updates = process.get_updates()
                self.send_progress_update(process, updates)
                if hasattr(process, 'get_new_model'):
                    new_model_data = process.get_new_model()
                    if new_model_data:
                        self.send_model_update(id_, new_model_data)

    def output_topic(self, kind: str) -> str:
        return '{}dd-{}-output-{}'.format(
            self.queue_prefix, self.queue_kind, kind)

    def send_progress_update(
            self, process: CrawlProcess, updates: Dict[str, Any]):
        id_ = process.id_
        progress = updates.get('progress')
        if progress is not None:
            progress_topic = self.output_topic('progress')
            logging.info('Sending update for "{}" to {}: {}'
                         .format(id_, progress_topic, progress))
            self.send(progress_topic, {
                'id': id_,
                'progress': progress,
                'percentage_done': updates.get('percentage_done', 0.),
            })
        page_sample = updates.get('pages')
        if page_sample:
            pages_topic = self.output_topic('pages')
            logging.info('Sending {} sample urls for "{}" to {}'
                         .format(len(page_sample), id_, pages_topic))
            self.send(pages_topic, {'id': id_, 'page_sample': page_sample})
        login_urls = updates.get('login_urls')
        if login_urls:
            logging.info(
                'Sending {} login urls for "{}" to {}'
                    .format(len(page_sample), id_, self.login_output_topic))
            for url in login_urls:
                self.send(self.login_output_topic, {
                    'workspace_id': process.workspace_id,
                    'job_id': id_,
                    'url': url,
                    'keys': ['login', 'password'],
                    'screenshot': None,
                })

    def send_model_update(self, id_: str, new_model_data: bytes):
        encoded_model = encode_model_data(new_model_data)
        topic = self.output_topic('model')
        logging.info('Sending new model to {}, model size {:,} bytes'
                     .format(topic, len(encoded_model)))
        self.send(topic, {'id': id_, 'link_model': encoded_model})

    def send_stopped_message(self, process: CrawlProcess):
        self.send('events-input', {
            'action': 'finished',
            'timestamp': time.time(),
            'workspaceId': process.workspace_id,
            'event': 'dd-{}'.format(self.queue_kind),
            "arguments": json.dumps({'jobId': process.id_}),
        })

    @log_ignore_exception
    def handle_hint(self, value: dict):
        workspace_id = value['workspace_id']
        passed = False
        for process in self.running.values():  # type: DDCrawlerProcess
            if process.workspace_id == workspace_id:
                passed = True
                logging.info(
                    'Pass hint message from workspace "{workspace_id}" '
                    'to crawl "{id}", pinned={pinned}, url {url}'
                    .format(id=process.id_, **value))
                process.handle_hint(value['url'], value['pinned'])
        if not passed:
            logging.info('Got hint message from workspace "{}", '
                         'but no process from this workspace is running'
                         .format(workspace_id))

    @log_ignore_exception
    def handle_login(self, value: dict):
        process = self.running.get(value['job_id'])  # type: DDCrawlerProcess
        if process is None:
            logging.info(
                'Got login message for url {}, but process {} is not running'
                .format(value['url'], value['job_id']))
            return
        logging.info(
            'Passing login message for url {} to process {}'
            .format(value['url'], value['job_id']))
        params = value['key_values']
        process.handle_login(value['url'], login=params['login'],
                             password=params['password'])

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
    arg('--proxy-container', help='proxy container name')
    args = parser.parse_args()

    configure_logging()
    cp_kwargs = {}
    if args.max_workers:
        cp_kwargs['max_workers'] = args.max_workers
    if args.proxy_container:
        cp_kwargs['proxy_container'] = args.proxy_container
    service = Service(
        args.kind, kafka_host=args.kafka_host, docker_image=args.docker_image,
        host_root=args.host_root, debug=args.debug,
        **cp_kwargs)
    logging.info('Starting hh dd-{} service'.format(args.kind))
    service.run()
