THH deep-deep integration
=========================

This is a service that listens to kafka topic and starts/stops deep-deep crawler,
sends back progress updates, samples of crawled pages and the link model.
It also starts the broad crawl using trained link model.

Docker-compose (see "Running with docker" below) also starts the hh-page-classifier
image that is responsible for training the page score model
(see https://github.com/TeamHG-Memex/hh-page-classifier).

The same protocol is also documented in the wiki:
https://hyperiongray.atlassian.net/wiki/pages/viewpage.action?pageId=85753859,
and there is also a diagram that shows how THH and hh-deep-deep work together
from the user point of view:
https://hyperiongray.atlassian.net/wiki/display/THH/THH+Deep-deep+workflow
and from the system point of view:
https://hyperiongray.atlassian.net/wiki/pages/viewpage.action?pageId=96796696


.. contents::

Protocol: dd-trainer
--------------------

Incoming: start the crawl, ``dd-trainer-input``::

    {
      "id": "some crawl id",
      "page_model": "b64-encoded page classifier",
      "seeds": ["http://example.com", "http://example.com/2"]
    }

An optional ``page_limit`` key can be added.

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


Protocol: dd-crawler
--------------------

Incoming: start the crawl, ``dd-crawler-input``::

    {
      "id": "some crawl id",
      "page_model": "b64-encoded page classifier",
      "link_model": "b64-encoded deep-deep model",
      "seeds": ["http://example.com", "http://example.com/2"],
    }

An optional ``page_limit`` key can be added.

Stopping the crawl via ``dd-crawler-input``, and
``dd-crawler-output-pages``, ``dd-crawler-output-progress`` work exactly the same
as the corresponding dd-trainer queues.


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

Download ``lda.pkl`` and ``random-pages.jl.gz`` from ``s3://darpa-memex/thh/``
and put them to ``./models`` folder.

Start trainer, modeler and crawler services with::

    docker-compose up --build -d

In order to update existing installation, do::

    git pull
    git submodule update --init


Local Kafka with Docker
+++++++++++++++++++++++

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
    cd /opt/kafka_*
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


Local kafka without docker
++++++++++++++++++++++++++

Start local kafka with::

    docker run -it --rm --name kafka \
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

Start kafka (see above in "Local kafka without docker").

Make sure you have ``dd-crawler-hh`` and ``deep-deep-hh`` images
(set in ``default_docker_image`` property of
``DDCrawlerProcess`` and ``DeepDeepProcess``).
These images can be built using dockerfiles in the ``./docker/`` folder::

    docker build -t dd-crawler-hh -f docker/dd-crawler.docker docker/
    docker build -t deep-deep-hh -f docker/deep-deep.docker docker/

Run tests::

    py.test --doctest-modules \
        --cov=hh_deep_deep --cov-report=term --cov-report=html \
        tests hh_deep_deep

One test (``tests/test_service.py::test_service``) takes much longer than the others
and can leave docker containers running if there is some error and
you are unlucky or press Ctrl+C more than once before crawls are stopped.
It's better to run it separately during development, adding ``-s`` flag.

To run all other tests, use::

    py.test tests/ -k-slow

