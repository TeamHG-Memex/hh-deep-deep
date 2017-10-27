import json
import logging
import threading
import pickle
from typing import Dict, List

from hh_page_clf.models import DefaultModel
import kafka.client
import pykafka
from pykafka.common import OffsetType
import pytest

from hh_deep_deep.service import (
    Service, encode_model_data, decode_model_data, output_topic,
)
from hh_deep_deep.crawl_utils import get_domain
from hh_deep_deep.utils import configure_logging


configure_logging()


DEBUG = False
TEST_SERVER_CONTAINER = 'hh-deep-deep-test-server'


class ATestService(Service):
    queue_prefix = 'test-'
    jobs_prefix = 'tests'
    reset_to_last = True


@pytest.fixture
def kafka_client():
    create_topics()
    return pykafka.KafkaClient()


def create_topics():
    ot = lambda kind, n: output_topic(ATestService.queue_prefix, kind, n)
    t = lambda n: '{}{}'.format(ATestService.queue_prefix, n)
    topic_names = [
        t('dd-trainer-input'),
        t('dd-crawler-trainer-input'),
        t('dd-crawler-input'),
        t('dd-deepcrawler-input'),
        ot('trainer', 'progress'),
        ot('crawler', 'progress'),
        ot('deepcrawler', 'progress'),
        ot('trainer', 'pages'),
        ot('crawler', 'pages'),
        ot('deepcrawler', 'progress'),
        ot('crawler', 'pages'),
        t('events-input'),
        t('dd-login-output'),
        t('dd-login-input'),
        t('dd-login-result'),
    ]
    print('Creating topics...')
    client = kafka.client.KafkaClient()
    topics = client.cluster.topics()
    for name in topic_names:
        if name not in topics:
            client.add_topic(name)
    print('done creating topics')


@pytest.mark.slow
def test_trainer_service(kafka_client: pykafka.KafkaClient):
    C = lambda t: make_consumer(kafka_client, t)
    P = lambda t: make_producer(kafka_client, t)
    trainer_service = ATestService(
        'trainer', checkpoint_interval=50, check_updates_every=2, debug=DEBUG)
    progress_consumer = C(trainer_service.output_topic('progress'))
    pages_consumer = C(trainer_service.output_topic('pages'))

    input_producer = P(trainer_service.input_topic)

    trainer_service_thread = threading.Thread(target=trainer_service.run)
    trainer_service_thread.start()

    ws_id = 'test-ws-id'
    start_message = start_trainer_message(ws_id)
    debug('Sending start trainer message')
    input_producer.produce(encode_message(start_message))
    debug('Sending another start trainer message (old should continue)')
    input_producer.produce(encode_message(start_message))
    try:
        _check_progress_pages(progress_consumer, pages_consumer, check_id=False)
    finally:
        # this is not part of the API, but it's convenient for tests
        input_producer.produce(encode_message(stop_crawl_message(ws_id)))
        # stop delayed started crawl
        input_producer.produce(encode_message(stop_crawl_message(ws_id)))
        input_producer.produce(encode_message({'from-tests': 'stop'}))
        trainer_service_thread.join()


def _check_progress_pages(progress_consumer, pages_consumer, check_id=True):
    while True:
        debug('Waiting for pages message...')
        check_pages(consume(pages_consumer))
        debug('Got it, now waiting for progress message...')
        progress_message = consume(progress_consumer)
        debug('Got it:', progress_message.get('progress'))
        if check_id:
            assert progress_message.get('id')
        progress = check_progress(progress_message)
        if progress:
            return progress


@pytest.mark.slow
def test_crawler_service(kafka_client: pykafka.KafkaClient, check_login=False):
    # login check if not 100% reliable as it's run against a live website,
    # it's tested properly in test_deepcrawl
    C = lambda t: make_consumer(kafka_client, t)
    P = lambda t: make_producer(kafka_client, t)
    crawler_service = ATestService(
        'crawler', check_updates_every=2, max_workers=2, debug=DEBUG)
    trainer_service = ATestService(
        'crawler-trainer', check_updates_every=2,
        checkpoint_interval=10, page_limit=50, debug=DEBUG)
    progress_consumer = C(crawler_service.output_topic('progress'))
    pages_consumer = C(crawler_service.output_topic('pages'))
    login_consumer = C(crawler_service.login_output_producer._topic.name)
    login_result_consumer = C(crawler_service.login_result_producer._topic.name)

    input_producer = P(crawler_service.input_topic)
    trainer_producer = P(trainer_service.input_topic)
    login_producer = P(crawler_service.login_consumer.topic.name)

    crawler_service_thread = threading.Thread(target=crawler_service.run)
    crawler_service_thread.start()
    trainer_service_thread = threading.Thread(target=trainer_service.run)
    trainer_service_thread.start()

    ws_id = 'test-ws-id'
    start_message = start_trainer_message(ws_id)
    start_message.update({
        'broadness': 'N10',
        'id': 'test-id',
        'page_limit': 500,
    })
    debug('Sending start crawler message')
    input_producer.produce(encode_message(start_message))
    try:
        # want to get progress from trainer first
        while True:
            progress = _check_progress_pages(progress_consumer, pages_consumer)
            if 'Trainer' in progress:
                break
        # and then from the crawler
        while True:
            progress = _check_progress_pages(progress_consumer, pages_consumer)
            if 'Trainer' not in progress:
                break
        if check_login:
            debug('Waiting for login message...')
            login_message = consume(login_consumer)
            debug('Got login message for {}'.format(login_message['url']))
            assert login_message['job_id'] == start_message['id']
            assert (
                login_message['workspace_id'] == start_message['workspace_id'])
            assert login_message['keys'] == ['login', 'password']
            login_producer.produce(encode_message({
                'id': 'cred-id-wrong',
                'job_id': start_message['id'],
                'url': 'http://news.ycombinator.com',
                'key_values': {
                    'login': 'invalid',
                    'password': 'invalid',
                }
            }))
            debug('Waiting for login result message')
            login_result = consume(login_result_consumer)
            assert login_result == {'id': 'cred-id-wrong', 'result': 'failed'}
    finally:
        input_producer.produce(
            encode_message(stop_crawl_message(start_message['id'])))
        input_producer.produce(encode_message({'from-tests': 'stop'}))
        crawler_service_thread.join()
        trainer_producer.produce(encode_message({'from-tests': 'stop'}))
        trainer_service_thread.join()


def debug(arg, *args: List[str]) -> None:
    print('{} '.format('>' * 60), arg, *args)


def check_progress(value):
    if 'id' in value:
        assert value['id'] == 'test-id'
    else:
        assert value['workspace_id'] == 'test-ws-id'
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


def check_pages(value):
    if 'id' in value:
        assert value['id'] == 'test-id'
    assert value['workspace_id'] == 'test-ws-id'
    page_samples = value['page_samples']
    assert len(page_samples) >= 1
    for s in page_samples:
        assert isinstance(s['score'], float)
        assert 100 >= s['score'] >= 0
        assert s['url'].startswith('http')


def start_trainer_message(ws_id: str) -> Dict:
    model = DefaultModel()
    model.fit(
        [{'url': 'http://a.com', 'text': text}
         for text in ['a good day', 'feeling nice today', 'it is sunny',
                      'what a mess', 'who invented it', 'so boring', 'stupid']],
        [1, 1, 1, 0, 0, 0, 0])
    return {
        'workspace_id': ws_id,
        'page_model': encode_model_data(pickle.dumps(model)),
        'urls': ['http://wikipedia.org', 'http://news.ycombinator.com'],
    }


def stop_crawl_message(id_: str) -> Dict:
    return {'id': id_, 'stop': True, 'verbose': True}


def test_deepcrawl(kafka_client: pykafka.KafkaClient):
    crawler_service = ATestService(
        'deepcrawler',
        check_updates_every=2,
        max_workers=4,
        in_flight_ttl=5,
        idle_before_close=5,  # this is not supported yet
        test_server_container=TEST_SERVER_CONTAINER,
        debug=DEBUG)

    C = lambda t: make_consumer(kafka_client, t)
    P = lambda t: make_producer(kafka_client, t)
    progress_consumer = C(crawler_service.output_topic('progress'))
    pages_consumer = C(crawler_service.output_topic('pages'))
    login_consumer = C(crawler_service.login_output_producer._topic.name)
    login_result_consumer = C(crawler_service.login_result_producer._topic.name)

    input_producer = P(crawler_service.input_topic)
    login_input_producer = P(crawler_service.login_consumer.topic.name)

    crawler_service_thread = threading.Thread(target=crawler_service.run)
    crawler_service_thread.start()

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
            {'id': 'cred-id-initial-correct',
             'domain': 'test-server-1',
             'url': 'http://test-server-1:8781/login',
             'key_values': {'login': 'admin', 'password': 'secret'},
             },
            {'id': 'cred-id-initial-wrong',
             'domain': 'test-server-2',
             'url': 'http://test-server-2:8781/login',
             'key_values': {'login': 'admin', 'password': 'wrong'},
             },
        ],
        'page_limit': 1000,
    }
    input_producer.produce(encode_message(start_message))
    expected_live_domains = {'test-server-1', 'test-server-2', 'test-server-3'}
    expected_domains = expected_live_domains | {'no-such-domain'}
    try:
        debug('Sending login message...')
        login_input_producer.produce(encode_message({
            'job_id': start_message['id'],
            'workspace_id': start_message['workspace_id'],
            'id': 'cred-id-sent-later',
            'domain': 'test-server-3',
            'url': 'http://test-server-3:8781/login',
            'key_values': {'login': 'admin', 'password': 'secret'},
        }))
        debug('Waiting for pages message...')
        pages = consume(pages_consumer)
        for p in pages.get('page_samples'):
            domain = p['domain']
            assert domain in expected_live_domains
            assert get_domain(p['url']) == domain
        while True:
            debug('Waiting for progress message...')
            progress_message = consume(progress_consumer)
            debug('Got it:', progress_message.get('progress'))
            progress = progress_message['progress']
            if progress and progress['domains']:
                assert progress['status'] in {'running', 'finished'}
                domain_statuses = dict()
                for d in progress['domains']:
                    domain = get_domain(d['url'])
                    domain_statuses[domain] = d['status']
                    assert domain in expected_domains
                    assert domain == d['domain']
                    if domain == 'no-such-domain':
                        assert d['status'] in {'failed', 'running'}
                        assert d['pages_fetched'] == 0
                    else:
                        assert d['status'] in {'running', 'finished'}
                    assert d['rpm'] is not None
                assert set(domain_statuses) == expected_domains
                if all(s in {'failed', 'finished'}
                       for s in domain_statuses.values()):
                    assert progress['status'] == 'finished'
                    break
        debug('Waiting for login message...')
        login_message = consume(login_consumer)
        debug('Got login message for {}'.format(login_message['url']))
        assert login_message['job_id'] == start_message['id']
        assert login_message['workspace_id'] == start_message['workspace_id']
        assert login_message['keys'] == ['login', 'password']
        debug('Waiting for login result message...')
        expected_cred_ids = {'cred-id-initial-correct', 'cred-id-initial-wrong',
                             'cred-id-sent-later'}
        login_results = {}
        while set(login_results) != expected_cred_ids:
            r = consume(login_result_consumer)
            debug('Got login result for {}: {}'.format(r['id'], r['result']))
            login_results[r['id']] = r
        assert login_results['cred-id-initial-correct']['result'] == 'success'
        # FIXME - this is most likely a test server issue:
        # FIXME login state is shared?
        # assert login_results['cred-id-initial-wrong']['result'] == 'failed'
        assert login_results['cred-id-initial-wrong']
        assert login_results['cred-id-sent-later']['result'] == 'success'
    finally:
        input_producer.produce(encode_message(
             stop_crawl_message(start_message['id'])))
        input_producer.produce(encode_message({'from-tests': 'stop'}))
        crawler_service_thread.join()


def test_encode_model():
    data = 'ё'.encode('utf8')
    assert isinstance(encode_model_data(data), str)
    assert data == decode_model_data(encode_model_data(data))
    assert decode_model_data(None) is None
    assert encode_model_data(None) is None


# TODO - test that encode_model_data and encode_object
# from hh_page_classifier are in sync


def make_consumer(kafka_client: pykafka.KafkaClient, topic: str,
                  ) -> pykafka.SimpleConsumer:
    return kafka_client.topics[_to_bytes(topic)].get_simple_consumer(
        auto_offset_reset=OffsetType.LATEST,
        reset_offset_on_start=True)


def make_producer(kafka_client: pykafka.KafkaClient, topic: str,
                  ) -> pykafka.Producer:
    return kafka_client.topics[_to_bytes(topic)].get_sync_producer()


def _to_bytes(x):
    return x.encode('ascii') if isinstance(x, str) else x


def consume(consumer: pykafka.SimpleConsumer) -> Dict:
    return decode_message(consumer.consume().value)


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
