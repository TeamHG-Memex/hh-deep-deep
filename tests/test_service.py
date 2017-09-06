import json
import logging
import threading
import time
from pathlib import Path
import pickle
from typing import Dict, Callable, List

from hh_page_clf.models import DefaultModel
from kafka import KafkaConsumer, KafkaProducer
import pytest

from hh_deep_deep.service import Service, encode_model_data, decode_model_data
from hh_deep_deep.crawl_utils import get_domain
from hh_deep_deep.utils import configure_logging


configure_logging()


DEBUG = False
TEST_SERVER_CONTAINER = 'hh-deep-deep-test-server'


class ATestService(Service):
    queue_prefix = 'test-'
    jobs_prefix = 'tests'


def clear_topics(service):
    topics = [
        service.input_topic,
        service.output_topic('progress'),
        service.output_topic('pages'),
    ]
    if service.queue_kind == 'trainer':
        topics.append(service.output_topic('model'))
    if service.queue_kind == 'crawler':
        topics.append(service.login_output_topic)
    for topic in topics:
        consumer = KafkaConsumer(topic, consumer_timeout_ms=100,
                                 group_id='{}-group'.format(topic))
        for _ in consumer:
            pass
        consumer.commit()


@pytest.mark.slow
def test_service_deepdeep():
    _test_service(run_deepdeep=True, run_dd_crawl=False)


@pytest.mark.slow
def test_service_dd_crawl():
    _test_service(run_deepdeep=False, run_dd_crawl=True)


@pytest.mark.slow
def test_service():
    _test_service(run_deepdeep=True, run_dd_crawl=True)


def _test_service(run_deepdeep, run_dd_crawl):
    # This is a big integration test, better run with "-s"

    job_id = 'test-id'
    ws_id = 'test-ws-id'
    producer = KafkaProducer(value_serializer=encode_message)

    def send(topic: str, message: Dict):
        producer.send(topic, message).get()
        producer.flush()

    start_crawler_message = None
    start_message_path = Path('.tst.start-dd-crawl.pkl')
    if not run_deepdeep:
        if start_message_path.exists():
            with start_message_path.open('rb') as f:
                start_crawler_message = pickle.load(f)
                debug('Loaded cached start dd_crawl message')
        else:
            debug('No cached start dd_crawl message found, must run deepdeep')
            run_deepdeep = True
    if run_deepdeep:
        debug('Clearing deep-deep topics')
        clear_topics(ATestService('trainer'))
        start_crawler_message = _test_trainer_service(job_id, ws_id, send)
        with start_message_path.open('wb') as f:
            pickle.dump(start_crawler_message, f)
    assert start_crawler_message is not None

    if run_dd_crawl:
        debug('Clearing dd-crawl topics')
        clear_topics(ATestService('crawler'))
        _test_crawler_service(start_crawler_message, send)


def _test_trainer_service(job_id: str, ws_id: str,
                          send: Callable[[str, Dict], None]) -> Dict:
    """ Test trainer service, return start message for crawler service.
    """
    trainer_service = ATestService(
        'trainer', checkpoint_interval=50, check_updates_every=2, debug=DEBUG)
    progress_consumer, pages_consumer, model_consumer = [
        KafkaConsumer(trainer_service.output_topic(kind),
                      value_deserializer=decode_message)
        for kind in ['progress', 'pages', 'model']]
    trainer_service_thread = threading.Thread(target=trainer_service.run)
    trainer_service_thread.start()

    debug('Sending start trainer message')
    send(trainer_service.input_topic, start_trainer_message(job_id + '-early', ws_id))
    time.sleep(2)
    start_message = start_trainer_message(job_id, ws_id)
    debug('Sending another start trainer message (old should stop)')
    send(trainer_service.input_topic, start_message)
    try:
        _check_progress_pages(progress_consumer, pages_consumer,
                              check_trainer=True)
        debug('Waiting for model, this might take a while...')
        model_message = next(model_consumer).value
        debug('Got it.')
        assert model_message['id'] == job_id
        link_model = model_message['link_model']

    finally:
        send(trainer_service.input_topic, stop_crawl_message(job_id))
        send(trainer_service.input_topic, stop_crawl_message(job_id + '-early'))
        send(trainer_service.input_topic, {'from-tests': 'stop'})
        trainer_service_thread.join()

    start_message['link_model'] = link_model
    return start_message


def _check_progress_pages(progress_consumer, pages_consumer,
                          check_trainer=False):
    while True:
        debug('Waiting for pages message...')
        check_pages(next(pages_consumer))
        debug('Got it, now waiting for progress message...')
        progress_message = next(progress_consumer)
        debug('Got it:', progress_message.value.get('progress'))
        progress = check_progress(progress_message)
        if progress and (not check_trainer or
                         'Last deep-deep model checkpoint' in progress):
            return progress


def _test_crawler_service(
        start_message: Dict, send: Callable[[str, Dict], None]) -> None:
    start_message['broadness'] = 'N10'
    crawler_service = ATestService(
        'crawler', check_updates_every=2, max_workers=2, debug=DEBUG)
    progress_consumer, pages_consumer = [
        KafkaConsumer(crawler_service.output_topic(kind),
                      value_deserializer=decode_message)
        for kind in ['progress', 'pages']]
    login_consumer = KafkaConsumer(crawler_service.login_output_topic,
                                   value_deserializer=decode_message)
    crawler_service_thread = threading.Thread(target=crawler_service.run)
    crawler_service_thread.start()

    debug('Sending start crawler message')
    send(crawler_service.input_topic, start_message)
    try:
        _check_progress_pages(progress_consumer, pages_consumer)
        # FIXME - this should be sent after getting login message
        send(crawler_service.login_input_topic, {
            'job_id': start_message['id'],
            'url': 'http://news.ycombinator.com',
            'key_values': {
                'login': 'invalid',
                'password': 'invalid',
            }
        })
        debug('Waiting for login message...')
        login_message = next(login_consumer).value
        debug('Got login message for {}'.format(login_message['url']))
        assert login_message['job_id'] == start_message['id']
        assert login_message['workspace_id'] == start_message['workspace_id']
        assert login_message['keys'] == ['login', 'password']
    finally:
        send(crawler_service.input_topic, stop_crawl_message(start_message['id']))
        send(crawler_service.input_topic, {'from-tests': 'stop'})
        crawler_service_thread.join()


def debug(arg, *args: List[str]) -> None:
    print('{} '.format('>' * 60), arg, *args)


def check_progress(message):
    value = message.value
    assert value['id'] == 'test-id'
    progress = value['progress']
    if progress not in {
            'Crawl is not running yet', 'Crawl started, no updates yet'}:
        assert 'pages processed' in progress
        assert 'domains' in progress
        assert 'relevant' in progress
        assert 'average score' in progress.lower()
        debug('percentage_done', value['percentage_done'])
        assert 0 <= value['percentage_done'] <= 100
        return progress


def check_pages(message):
    value = message.value
    assert value['id'] in {'test-id', 'test-id-early'}
    page_samples = value['page_samples']
    assert len(page_samples) >= 1
    for s in page_samples:
        assert isinstance(s['score'], float)
        assert 100 >= s['score'] >= 0
        assert s['url'].startswith('http')


def start_trainer_message(id_: str, ws_id: str) -> Dict:
    model = DefaultModel()
    model.fit(
        [{'url': 'http://a.com', 'text': text}
         for text in ['a good day', 'feeling nice today', 'it is sunny',
                      'what a mess', 'who invented it', 'so boring', 'stupid']],
        [1, 1, 1, 0, 0, 0, 0])
    return {
        'id': id_,
        'workspace_id': ws_id,
        'page_model': encode_model_data(pickle.dumps(model)),
        'urls': ['http://wikipedia.org', 'http://news.ycombinator.com'],
    }


def stop_crawl_message(id_: str) -> Dict:
    return {'id': id_, 'stop': True, 'verbose': True}


def test_deepcrawl():
    producer = KafkaProducer(value_serializer=encode_message)

    def send(topic: str, message: Dict):
        producer.send(topic, message).get()
        producer.flush()

    # FIXME - max_workers set to 1 because with >1 workers the first might
    # exit due to running out of domains, and handle_login will fail
    crawler_service = ATestService(
        'deepcrawler',
        check_updates_every=2,
        max_workers=1,
        in_flight_ttl=5,
        idle_before_close=5,  # this is not supported yet
        test_server_container=TEST_SERVER_CONTAINER,
        debug=DEBUG)
    progress_consumer, pages_consumer, login_consumer, login_result_consumer = [
        KafkaConsumer(topic, value_deserializer=decode_message)
        for topic in [crawler_service.output_topic('progress'),
                      crawler_service.output_topic('pages'),
                      crawler_service.login_output_topic,
                      crawler_service.login_result_topic,
                      ]]
    crawler_service_thread = threading.Thread(target=crawler_service.run)
    crawler_service_thread.start()

    time.sleep(1)  # FIXME - figure out how to deliver the message reliably
    send(crawler_service.input_topic, {'create_topic': True})
    time.sleep(1)
    debug('Sending start crawler message')
    start_message = {
        'id': 'deepcrawl-test',
        'workspace_id': 'deepcrawl-test-ws',
        'urls': ['http://test-server-1:8781/',
                 'http://test-server-2:8781/',
                 'http://test-server-3:8781/',
                 'http://no-such-domain/',
                 ],
        'login_credentials': [
            {'id': 'cred-id-fajfh',
             'domain': 'test-server-1',
             'url': 'http://test-server-1/login',
             'key_values': {'login': 'admin', 'password': 'secret'},
             },
            {'id': 'cred-id-afhu',
             'domain': 'test-server-2',
             'url': 'http://test-server-2/login',
             'key_values': {'login': 'admin', 'password': 'wrong'},
             },
        ],
        'page_limit': 1000,
    }
    send(crawler_service.input_topic, start_message)
    expected_live_domains = {'test-server-1', 'test-server-2', 'test-server-3'}
    expected_domains = expected_live_domains | {'no-such-domain'}
    try:
        while True:
            debug('Waiting for pages message...')
            pages = next(pages_consumer)
            for p in pages.value.get('page_samples'):
                domain = p['domain']
                assert domain in expected_live_domains
                assert get_domain(p['url']) == domain
            debug('Got it, now waiting for progress message...')
            progress_message = next(progress_consumer)
            debug('Got it:', progress_message.value.get('progress'))
            progress = progress_message.value['progress']
            if progress and progress['domains'] and all(
                    d['pages_fetched'] > 0 for d in progress['domains']
                    if d['status'] == 'running'):
                domain_statuses = dict()
                for d in progress['domains']:
                    domain = get_domain(d['url'])
                    domain_statuses[domain] = d['status']
                    assert domain in expected_domains
                    assert domain == d['domain']
                    assert d['status'] in ['running', 'failed', 'finished']
                    if domain == 'no-such-domain':
                        assert d['status'] in {'failed', 'running'}
                        assert d['pages_fetched'] == 0
                    else:
                        assert d['status'] == 'running'
                        assert d['pages_fetched'] > 0
                    assert d['rpm'] is not None
                assert set(domain_statuses) == expected_domains
                if domain_statuses['no-such-domain'] == 'failed':
                    break
        debug('Waiting for login message...')
        login_message = next(login_consumer).value
        debug('Got login message for {}'.format(login_message['url']))
        assert login_message['job_id'] == start_message['id']
        assert login_message['workspace_id'] == start_message['workspace_id']
        assert login_message['keys'] == ['login', 'password']
        send(crawler_service.login_input_topic, {
            'job_id': start_message['id'],
            'workspace_id': start_message['workspace_id'],
            'id': 'cred-id-78liew',
            'domain': 'test-server-3',
            'url': 'http://test-server-3/login',
            'key_values': {'login': 'admin', 'password': 'secret'},
        })
        debug('Waiting for login result message...')
        login_result = next(login_result_consumer).value
        # TODO
    finally:
        send(crawler_service.input_topic,
             stop_crawl_message(start_message['id']))
        send(crawler_service.input_topic, {'from-tests': 'stop'})
        crawler_service_thread.join()


def test_encode_model():
    data = 'ё'.encode('utf8')
    assert isinstance(encode_model_data(data), str)
    assert data == decode_model_data(encode_model_data(data))
    assert decode_model_data(None) is None
    assert encode_model_data(None) is None


# TODO - test that encode_model_data and encode_object
# from hh_page_classifier are in sync


def decode_message(message: bytes) -> Dict:
    try:
        return json.loads(message.decode('utf8'))
    except Exception as e:
        logging.error('Error deserializing message', exc_info=e)
        raise


def encode_message(message: Dict) -> bytes:
    try:
        return json.dumps(message).encode('utf8')
    except Exception as e:
        logging.error('Error serializing message', exc_info=e)
        raise
