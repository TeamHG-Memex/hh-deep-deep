import json
import logging
from pprint import pprint
import threading
from typing import Dict

from kafka import KafkaConsumer, KafkaProducer

from hh_deep_deep.service import Service, encode_message, \
    encode_model_data, decode_model_data
from hh_deep_deep.utils import configure_logging


configure_logging()


class ATestService(Service):
    input_topic = 'test-{}'.format(Service.input_topic)
    ouput_topic = 'test-{}'.format(Service.output_topic)


def clear_topics():
    for topic in [ATestService.input_topic, ATestService.output_topic]:
        consumer = KafkaConsumer(topic, consumer_timeout_ms=100)
        for _ in consumer:
            pass
        consumer.commit()


def test_service():
    clear_topics()
    producer = KafkaProducer(value_serializer=encode_message)
    consumer = KafkaConsumer(
        ATestService.output_topic,
        value_deserializer=decode_message)
    service = ATestService()
    service_thread = threading.Thread(target=service.run)
    service_thread.start()

    producer.send(ATestService.input_topic, {'from-tests': 'stop'})
    producer.flush()
    service_thread.join()


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
