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
        {"url": "http://example1.com", "score": 80},
        {"url": "http://example2.com", "score": 90}
      ]
    }

Outgoing: send progress update, ``dd-trainer-output-progress``::

    {
      "id": "some crawl id",
      "progress": "Crawled N pages and M domains, average reward is 0.122"
    }


Running with docker
-------------------

Install docker and docker-compose (assuming Ubuntu 16.04)::

    sudo apt install -y docker.io python-pip
    sudo -H pip install docker-compose

Add yourself to docker group (requires re-login)::

    sudo usermod -aG docker <username>

You **must** add the IP at which kafka is running to ``/etc/hosts``, making it
resolve to ``hh-kafka``. An alternative would be to add::

    extra_hosts:
        - "hh-kafka:${KAFKA_HOST}"

instead of ``network_mode: host``, but that will not work with local kafka.

Start trainer, modeler and crawler services with::

    docker-compose up -d

If you want to use a local kafka, just add ``127.0.0.1   hh-kafka`` to ``/etc/hosts``,
and star kafka with::

    docker run -it --rm --name kafka \
        --add-host hh-kafka:127.0.0.1 \
        -p 2181:2181 -p 9092:9092 \
        --env ADVERTISED_HOST=hh-kafka \
        --env ADVERTISED_PORT=9092 \
        spotify/kafka

By default, kafka limits message size to 1Mb, which is too small in our case.
In order to raise the limit, do the following in the kafka container::

    docker exec -it kafka /bin/bash
    cd /opt/kafka_2.11-0.8.2.1/config
    echo "message.max.bytes=104857600" >> server.properties
    echo "replica.fetch.max.bytes=104857600" >> server.properties
    echo "fetch.message.max.bytes=104857600" >> server.properties
    echo "fetch.message.max.bytes=104857600" >> consumer.properties
    kill -15 `ps aux | grep kafka.Kafka | grep -v grep | awk '{print $2}'`
    exit

For some reason, pushing messages does not work after stop/start.

Usage without Docker
--------------------

Run the service passing kafka host as ``--kafka-host``
(or leave it blank if testing locally)::

    hh-deep-deep-service [trainer|crawler] --kafka-host hh-kafka


Running local kafka
-------------------

Start local kafka with::

    docker run -d --name kafka \
        -p 2181:2181 -p 9092:9092 \
        --env ADVERTISED_HOST=127.0.0.1 \
        --env ADVERTISED_PORT=9092 \
        spotify/kafka

Also tweak it's config in the same way as described above, at the end of
"Running with docker" section.


Testing
-------

Install test requirements::

    pip install -r tests/requirements.txt

Start kafka (see above).

Run tests::

    py.test --doctest-modules \
        --cov=hh_deep_deep --cov-report=term --cov-report=html \
        tests hh_deep_deep

One test (``tests/test_service.py::test_service``) takes much longer than the others
and can leave docker containers running if there is some error, so it's better
to run it separately during development, adding ``-s`` flag.

