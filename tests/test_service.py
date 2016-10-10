import json
import logging
import threading
import pickle
from typing import Dict, Callable

from kafka import KafkaConsumer, KafkaProducer
from sklearn.pipeline import Pipeline
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.linear_model import LogisticRegression

from hh_deep_deep.service import Service, encode_message, \
    encode_model_data, decode_model_data
from hh_deep_deep.utils import configure_logging


configure_logging()


class ATestService(Service):
    queue_prefix = 'test-'
    jobs_prefix = 'tests'


def clear_topics():
    for service in [ATestService('trainer'), ATestService('crawler')]:
        for topic in [
                service.input_topic,
                service.output_topic('progress'),
                service.output_topic('pages'),
                service.output_topic('model'),
                ]:
            consumer = KafkaConsumer(topic, consumer_timeout_ms=10)
            for _ in consumer:
                pass
            consumer.commit()


def test_service():
    # This is a big integration test, better run with "-s"

    debug('Clearing topics')
    clear_topics()
    test_id = 'test-id'
    producer = KafkaProducer(value_serializer=encode_message)

    def send(topic: str, message: Dict):
        producer.send(topic, message).get()

    start_crawler_message = _test_trainer_service(test_id, send)

    _test_crawler_service(test_id, send, start_crawler_message)


def _test_trainer_service(test_id: str, send: Callable[[str, Dict], None])\
        -> Dict:
    """ Test trainer service, return start message for crawler service.
    """
    trainer_service = ATestService(
        'trainer', checkpoint_interval=50, check_updates_every=2)
    (trainer_progress_consumer, trainer_pages_consumer,
     trainer_model_consumer) = [
        KafkaConsumer(trainer_service.output_topic(kind),
                      value_deserializer=decode_message)
        for kind in ['progress', 'pages', 'model']]
    trainer_service_thread = threading.Thread(target=trainer_service.run)
    trainer_service_thread.start()

    start_message = start_trainer_message(test_id)
    debug('Sending start trainer message')
    send(trainer_service.input_topic, start_message)
    try:
        while True:
            debug('Waiting for pages message...')
            check_pages(next(trainer_pages_consumer))
            debug('Got it, now waiting for progress message...')
            progress_message = next(trainer_progress_consumer)
            debug('Got it.')
            if check_trainer_progress(progress_message):
                break

        debug('Waiting for model, this might take a while...')
        model_message = next(trainer_model_consumer).value
        assert model_message['id'] == test_id
        link_model = model_message['link_model']

    finally:
        send(trainer_service.input_topic, stop_crawl_message(test_id))
        send(trainer_service.input_topic, {'from-tests': 'stop'})
        trainer_service_thread.join()

    start_message['link_model'] = link_model
    return start_message


def _test_crawler_service(test_id: str, send: Callable[[str, Dict], None],
                          start_message: Dict) -> None:
    crawler_service = ATestService(
        'crawler', check_updates_every=2, max_workers=2)
    crawler_progress_consumer, crawler_pages_consumer = [
        KafkaConsumer(crawler_service.output_topic(kind),
                      value_deserializer=decode_message)
        for kind in ['progress', 'pages']]
    crawler_service_thread = threading.Thread(target=crawler_service.run)
    crawler_service_thread.start()

    debug('Sending start crawler message')
    send(crawler_service.input_topic, start_message)
    try:
        debug('Waiting for pages message...')
        check_pages(next(crawler_pages_consumer))
        debug('Got it, now waiting for progress message...')
        progress_message = next(crawler_progress_consumer)
        # TODO - check it
        debug('Got it.')
    finally:
        send(crawler_service.input_topic, stop_crawl_message(test_id))
        send(crawler_service.input_topic, {'from-tests': 'stop'})
        crawler_service_thread.join()


def debug(s: str) -> None:
    print('{}  {}'.format('>' * 60, s))


def check_trainer_progress(message):
    value = message.value
    assert value['id'] == 'test-id'
    progress = value['progress']
    if progress not in {
            'Craw is not running yet', 'Crawl started, no updates yet'}:
        assert 'pages processed' in progress
        assert 'domains' in progress
        assert 'relevant' in progress
        assert 'average score' in progress
        return True


def check_pages(message):
    value = message.value
    assert value['id'] == 'test-id'
    page_sample = value['page_sample']
    assert len(page_sample) >= 1
    for s in page_sample:
        assert isinstance(s['score'], float)
        assert 100 >= s['score'] >= 0
        assert s['url'].startswith('http')


def start_trainer_message(id_: str) -> Dict:
    pipeline = Pipeline([
        ('vec', CountVectorizer(binary=True)),
        ('clf', LogisticRegression()),
        ])
    pipeline.fit(['a good day', 'feeling nice today', 'it is sunny',
                  'what a mess', 'who invented it', 'so boring', 'stupid'],
                 [1, 1, 1, 0, 0, 0, 0])
    return {
        'id': id_,
        'page_model': encode_model_data(pickle.dumps(pipeline)),
        'seeds': ['http://wikipedia.org', 'http://news.ycombinator.com'],
    }


def stop_crawl_message(id_: str) -> Dict:
    return {'id': id_, 'stop': True}


def test_encode_model():
    data = 'ё'.encode('utf8')
    assert isinstance(encode_model_data(data), str)
    assert data == decode_model_data(encode_model_data(data))
    assert decode_model_data(None) is None
    assert encode_model_data(None) is None


def decode_message(message: bytes) -> Dict:
    try:
        return json.loads(message.decode('utf8'))
    except Exception as e:
        logging.error('Error deserializing message', exc_info=e)
        raise
