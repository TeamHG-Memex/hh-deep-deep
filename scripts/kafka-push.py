#!/usr/bin/env python3
import argparse
import json
import logging

from kafka import KafkaProducer


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topic')
    parser.add_argument('filename')
    parser.add_argument('--kafka-host')
    parser.add_argument('--validate', action='store_true')
    args = parser.parse_args()

    if args.validate:
        with open(args.filename, 'rt') as f:
            data = json.dumps(json.load(f)).encode('utf8')
    else:
        with open(args.filename, 'rb') as f:
            data = f.read()

    logging.basicConfig(level=logging.INFO)
    kafka_kwargs = {}
    if args.kafka_host:
        kafka_kwargs['bootstrap_servers'] = args.kafka_host
    producer = KafkaProducer(
        max_request_size=104857600, **kafka_kwargs)

    producer.send(args.topic, data).get()
    print('Pushed {} bytes to {}'.format(len(data), args.topic))


if __name__ == '__main__':
    main()
