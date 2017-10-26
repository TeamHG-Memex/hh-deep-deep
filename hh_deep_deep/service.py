import argparse
import base64
import concurrent.futures
import gzip
import hashlib
import logging
import json
from pathlib import Path
import time
from typing import Any, Dict, Optional
import zlib

import pykafka
from pykafka.common import OffsetType

from .crawl_utils import CrawlProcess, get_domain
from .deepdeep_crawl import DeepDeepProcess, is_trainer_started_by_crawler
from .dd_crawl import DDCrawlerProcess
from .deep_crawl import DeepCrawlerProcess
from .utils import configure_logging, log_ignore_exception


class Service:
    queue_prefix = ''
    jobs_prefix = ''
    max_message_size = 104857600
    group_id = 'hh-deep-deep-{}'
    reset_to_last = False

    def __init__(self,
                 queue_kind: str,
                 kafka_host: str=None,
                 check_updates_every: int=30,
                 debug: bool=False,
                 **crawler_process_kwargs):
        # some config depending on the queue kind
        self.queue_kind = queue_kind
        self.required_keys = ['workspace_id', 'urls'] + {
            'trainer': ['page_model'],
            'crawler-trainer': ['id', 'page_model'],
            'crawler': ['id', 'page_model'],
            'deepcrawler': ['id'],
            }[queue_kind]
        self.process_class = {
            'trainer': DeepDeepProcess,
            'crawler-trainer': DeepDeepProcess,
            'crawler': DDCrawlerProcess,
            'deepcrawler': DeepCrawlerProcess,
            }[queue_kind]
        self.needs_percentage_done = queue_kind != 'deepcrawler'
        self.single_crawl = queue_kind != 'deepcrawler'
        self.supports_login = queue_kind in {'crawler', 'deepcrawler'}
        self.outputs_model = queue_kind == 'crawler-trainer'
        self.needs_model = queue_kind == 'crawler'
        self.delay_stop = 300 if queue_kind == 'trainer' else 0
        self.delayed_requests = {}  # workspace_id -> request

        kafka_kwargs = {}
        if kafka_host is not None:
            kafka_kwargs['hosts'] = '{}:9092'.format(kafka_host)
        self.kafka_client = pykafka.KafkaClient(**kafka_kwargs)

        # Together with consumer_timeout_ms, this defines responsiveness.
        self.check_updates_every = check_updates_every
        self.debug = debug

        topic = lambda x: '{}{}'.format(self.queue_prefix, x)
        self.input_topic = topic('dd-{}-input'.format(self.queue_kind))
        logging.info('Listening on {} topic'.format(self.input_topic))
        C = self._kafka_consumer
        P = self._kafka_producer
        self.consumer = C(self.input_topic, consumer_timeout_ms=100)
        self.progress_producer = P(self.output_topic('progress'))
        self.pages_producer = P(self.output_topic('pages'))
        if self.outputs_model:
            self.crawler_producer = P(topic('dd-crawler-input'))
            self.crawler_progress_producer = (
                P(topic('dd-crawler-output-progress')))
        if self.needs_model:
            self.crawler_trainer_producer = P(topic('dd-crawler-trainer-input'))
        self.events_producer = P(topic('events-input'))
        if self.supports_login:
            self.login_consumer = C(topic('dd-login-output'))
            self.login_output_producer = P(topic('dd-login-input'))
            self.login_result_producer = P(topic('dd-login-result'))

        self.crawler_process_kwargs = dict(
            crawler_process_kwargs,
            jobs_root=get_jobs_root(queue_kind, self.jobs_prefix))
        self.running = (
            self.process_class.load_all_running(**self.crawler_process_kwargs))
        if self.running:
            for id_, process in sorted(self.running.items()):
                logging.info('Already running crawl "{id}", in {root}'
                             .format(id=id_, root=process.paths.root))
        else:
            logging.info('No crawls running')

        self.previous_progress = {}  # type: Dict[CrawlProcess, Dict[str, Any]]

    def _kafka_topic(self, topic: str) -> pykafka.Topic:
        return self.kafka_client.topics[topic.encode('ascii')]

    def _kafka_consumer(self, topic, consumer_timeout_ms=10,
                        ) -> pykafka.SimpleConsumer:
        kwargs = {}
        if self.reset_to_last:
            kwargs.update(dict(
                auto_offset_reset=OffsetType.LATEST,
                reset_offset_on_start=True
            ))
        return (
            self._kafka_topic(topic)
            .get_simple_consumer(
                consumer_group=(
                    self.group_id.format(self.queue_kind).encode('ascii')),
                consumer_timeout_ms=consumer_timeout_ms,
                fetch_message_max_bytes=self.max_message_size,
                **kwargs
            ))

    def _kafka_producer(self, topic: str) -> pykafka.Producer:
        return self._kafka_topic(topic).get_sync_producer(
            max_request_size=self.max_message_size)

    def run(self) -> None:
        counter = 0
        updates_futures = []
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
                for value in list(self.delayed_requests.values()):
                    executor.submit(self.start_crawl, value, delayed=True)
                if self.supports_login:
                    for value in self._read_consumer(self.login_consumer):
                        executor.submit(self.handle_login, value)
                if counter % self.check_updates_every == 0:
                    updates_futures = [f for f in updates_futures
                                       if not f.done()]
                    if not updates_futures:
                        updates_futures.append(
                            executor.submit(self.send_updates))

    def _read_consumer(self, consumer: pykafka.SimpleConsumer):
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
        consumer.commit_offsets()

    @log_ignore_exception
    def start_crawl(self, request: Dict, delayed=False) -> None:
        workspace_id = request['workspace_id']
        if delayed and workspace_id not in self.delayed_requests:
            # multiple futures piling up
            return
        id_ = request['id'] if 'id' in request else workspace_id
        if not delayed:
            logging.info(
                'Got start crawl message with id "{id}", {n_urls} urls{login}'
                .format(
                    id=id_,
                    n_urls=len(request['urls']),
                    login=(', {} login credentials'
                           .format(len(request['login_credentials']))
                           if 'login_credentials' in request else ''),
                ))
        if self.needs_model and 'link_model' not in request:
            self._start_trainer_from_crawler(request)
            return
        if self.single_crawl:
            delay_start = self._stop_running(workspace_id)
            if delay_start:
                self.delayed_requests[workspace_id] = request
                return
        if delayed:
            logging.info('Delayed start with id "{id}", {n_urls} urls'
                         .format(id=id_, n_urls=len(request['urls'])))
            del self.delayed_requests[workspace_id]
        kwargs = dict(self.crawler_process_kwargs)
        kwargs['seeds'] = request['urls']
        if 'page_model' in request:
            kwargs['page_clf_data'] = decode_model_data(request['page_model'])
        if 'link_model' in request:
            kwargs['link_clf_data'] = decode_model_data(request['link_model'])
        optional_fields = [
            'page_limit', 'broadness', 'login_credentials', 'crawler_params']
        kwargs.update({field: request[field] for field in optional_fields
                       if field in request})
        process = self.process_class(id_=id_, workspace_id=workspace_id,
                                     **kwargs)
        process.start()
        self.running[id_] = process

    def _stop_running(self, workspace_id: str):
        """ Stop all running processes for this workspace
        (should be at most 1 usually). If False is returned, no new process
        should be started.
        """
        delay_start = False
        for p_id, process in list(self.running.items()):
            if process.workspace_id == workspace_id:
                should_stop = True
                if self.delay_stop:
                    dt = time.time() - process.start_time
                    if dt < self.delay_stop:
                        should_stop = False
                        delay_start = True
                if should_stop:
                    logging.info('Stopping old process {}, workspace {}'
                                 .format(p_id, workspace_id))
                    process.stop()
                    self.running.pop(p_id, None)
                    self.send_stopped_message(process)
        return delay_start

    def _start_trainer_from_crawler(self, request: Dict):
        # Request from Sitehound: need to train a fresh deep-deep model.
        logging.info('Re-routing start crawler request to crawler-trainer')
        trainer_keys = {'id', 'workspace_id', 'page_model', 'urls'}
        # Original page_limit goes to crawler_params.
        trainer_request = {
            k: request[k] for k in trainer_keys if k in request}
        trainer_request['crawler_params'] = {
            k: request[k] for k in request if k not in trainer_keys}
        self.send(self.crawler_trainer_producer, trainer_request)

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
            is_running = process.is_running()
            if not is_running:
                logging.warning(
                    'Crawl should be running but it\'s not, stopping.')
                process.stop(verbose=True)
                self.running.pop(id_)
            updates = process.get_updates()
            self.send_progress_update(process, updates)
            if not is_running and is_trainer_started_by_crawler(process):
                # trainer was called by crawler and it's done training:
                # need to send back the start message.
                self.send_start_crawler(process)
            if not is_running:
                self.send_stopped_message(process)

    def output_topic(self, name: str) -> str:
        return output_topic(self.queue_prefix, self.queue_kind, name)

    def send_progress_update(
            self, process: CrawlProcess, updates: Dict[str, Any]):
        id_ = process.id_
        progress = updates.get('progress')
        if progress is not None:
            progress_message = {process.id_field: id_, 'progress': progress}
            if self.queue_kind != 'deepcrawler':
                progress_message['workspace_id'] = process.workspace_id
            if self.needs_percentage_done:
                progress_message['percentage_done'] = \
                    updates.get('percentage_done', 0)
            if is_trainer_started_by_crawler(process):
                progress_message['id'] = process.id_
            if progress_message != self.previous_progress.get(process):
                self.previous_progress[process] = progress_message
                if is_trainer_started_by_crawler(process):
                    progress_producer = self.crawler_progress_producer
                else:
                    progress_producer = self.progress_producer
                logging.info('Sending update for "{}": {}'
                             .format(id_, progress_message))
                self.send(progress_producer, progress_message)
        if is_trainer_started_by_crawler(process):
            return  # no other updates needed, only progress
        page_samples = updates.get('pages')
        if page_samples:
            for p in page_samples:
                p['domain'] = get_domain(p['url'])
            pages_topic = self.output_topic('pages')
            logging.info('Sending {} sample urls for "{}" to {}'
                         .format(len(page_samples), id_, pages_topic))
            message = {process.id_field: id_, 'page_samples': page_samples}
            if self.queue_kind != 'deepcrawler':
                message['workspace_id'] = process.workspace_id
            self.send(self.pages_producer, message)
        for url in updates.get('login_urls', []):
            logging.info('Sending login url for "{}": {}'.format(id_, url))
            self.send(self.login_output_producer, {
                'workspace_id': process.workspace_id,
                'job_id': id_,
                'url': url,
                'domain': get_domain(url),
                'keys': ['login', 'password'],
                'screenshot': None,
            })
        for cred_id, login_result in updates.get('login_results', []):
            logging.info('Sending login result "{}" for {}'
                         .format(login_result, cred_id))
            self.send(self.login_result_producer,
                      {'id': cred_id, 'result': login_result})

    def send_start_crawler(self, process: DeepDeepProcess):
        model_data = process.get_model()
        if not model_data:
            logging.warning('No model for crawler produced by crawler-trainer')
            return
        message = dict(process.crawler_params)
        message.update({
            'id': process.id_,
            'workspace_id': process.workspace_id,
            'urls': process.seeds,
            'page_model': encode_model_data(process.page_clf_data),
            'link_model': encode_model_data(model_data),
        })
        logging.info('Sending crawler start message')
        self.send(self.crawler_producer, message)

    def send_stopped_message(self, process: CrawlProcess):
        self.send(self.events_producer, {
            'action': 'finished',
            'timestamp': time.time(),
            'workspaceId': process.workspace_id,
            'event': 'dd-{}'.format(self.queue_kind),
            'arguments': json.dumps({'jobId': process.id_}),
        })

    @log_ignore_exception
    def handle_login(self, value: Dict):
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
        process.handle_login(url=value['url'],
                             login=params['login'],
                             password=params['password'],
                             cred_id=value['id'])

    def send(self, producer: pykafka.Producer, result: Dict):
        message = json.dumps(result).encode('utf8')
        self._debug_save_message(
            message, 'outgoing to {}'.format(producer._topic.name))
        producer.produce(message)

    def _debug_save_message(self, message: bytes, kind: str) -> None:
        if self.debug:
            filename = ('hh-deep-deep-{}.json.gz'
                        .format(hashlib.md5(message).hexdigest()))
            logging.info('Saving {} message to {}'.format(kind, filename))
            with gzip.open(filename, 'wb') as f:
                f.write(message)


def get_jobs_root(queue_kind: str, jobs_prefix: Optional[str]) -> Path:
    name = '{}-jobs'.format(queue_kind)
    if jobs_prefix:
        return Path(jobs_prefix).joinpath(name)
    else:
        return Path(name)


def output_topic(queue_prefix, queue_kind, name):
    return '{}dd-{}-output-{}'.format(
        queue_prefix, queue_kind, name)


def encode_model_data(data: Optional[bytes]) -> Optional[str]:
    if data:
        return base64.b64encode(zlib.compress(data)).decode('ascii')


def decode_model_data(data: Optional[str]) -> bytes:
    if data is not None:
        return zlib.decompress(base64.b64decode(data))


def main():
    parser = argparse.ArgumentParser()
    arg = parser.add_argument
    arg('kind',
        choices=['trainer', 'crawler-trainer', 'crawler', 'deepcrawler'])
    arg('--docker-image', help='Name of docker image for the crawler')
    arg('--kafka-host')
    arg('--host-root', help='Pass host ${PWD} if running in a docker container')
    arg('--max-workers', type=int, help='Only for "crawler" or "deepcrawler"')
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
