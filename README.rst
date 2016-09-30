THH deep-deep integration
=========================

This is a service that listens to kafka topic and starts/stops deep-deep crawler,
sends back progress updates, samples of crawled pages and


Protocol
--------

Incoming: start the crawl, ``dd-trainer-input``::

    {
      "id": "some crawl id",
      "page_model": "b64-encoded page classifier",
      "seeds": ["http://example.com", "http://example.com/2"]
    }

Incoming: stop the crawl, ``dd-trainer-input``::

    {
      "id": "the same id",
      "stop": true
    }


Outgoing: send update of the link model, ``dd-trainer-output-model``::

    {
      "id": "some crawl id",
      "link_model": "b64-encoded link classifier"
    }


Outgoing: send sample of crawled pages, ``dd-trainer-output-pages``::

    {
      "id": "some crawl id",
      "page_sample": [
        "http://example1.com",
        "http://example2.com"
      ]
    }

Outgoing: send progress update, ``dd-trainer-output-progress``::

    {
      "id": "some crawl id",
      "progress": "Crawled N pages and M domains, average reward is 0.122"
    }

Usage
-----

Run the service passing THH host (add hh-kafka to ``/etc/hosts``
if running on a different network)::

    hh-deep-deep-service --kafka-host hh-kafka


Testing
-------

Install ``pytest`` and ``pytest-cov``.

Start kafka with zookeper::

    docker run -p 2181:2181 -p 9092:9092 \
        --env ADVERTISED_HOST=127.0.0.1 \
        --env ADVERTISED_PORT=9092 \
        spotify/kafka

Run tests::

    py.test --doctest-modules \
        --cov=hh_deep_deep --cov-report=term --cov-report=html \
        tests hh_deep_deep
