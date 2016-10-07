import json
import logging
from pprint import pprint
import threading
import pickle
from typing import Dict

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

    clear_topics()
    trainer_service = ATestService('trainer')
    producer = KafkaProducer(value_serializer=encode_message)
    (trainer_progress_consumer, trainer_pages_consumer,
     trainer_model_consumer) = [
        KafkaConsumer(trainer_service.output_topic(kind),
                      value_deserializer=decode_message)
        for kind in ['progress', 'pages', 'model']]
    trainer_service_thread = threading.Thread(target=trainer_service.run)
    trainer_service_thread.start()

    def send(topic: str, message: Dict):
        producer.send(topic, message).get()

    test_id = 'test-id'
    send(trainer_service.input_topic, start_trainer_message(test_id))
    send(trainer_service.input_topic, stop_message(test_id))

    send(trainer_service.input_topic, {'from-tests': 'stop'})
    trainer_service_thread.join()


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


def stop_message(id_: str) -> Dict:
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
