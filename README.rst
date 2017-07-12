THH deep-deep integration
=========================

This is a service that listens to kafka topic and starts/stops deep-deep crawler,
sends back progress updates, samples of crawled pages and the link model.
It also starts the broad crawl using trained link model.

Docker-compose (see "Running with docker" below) also starts the hh-page-classifier
image that is responsible for training the page score model
(see https://github.com/TeamHG-Memex/hh-page-classifier).


.. contents::

Protocol: dd-trainer
--------------------

Incoming: start the crawl, ``dd-trainer-input``::

    {
      "id": "some crawl id",
      "workspace_id": "some workspace id",
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
      "workspace_id": "some workspace id",
      "page_model": "b64-encoded page classifier",
      "link_model": "b64-encoded deep-deep model",
      "seeds": ["http://example.com", "http://example.com/2"],
      "hints": ["http://example2.com", "http://example2.com/2"],
      "broadness": "DEEP",
      "page_limit": 10000000,
    }

Fields ``hints`` and ``page_limit`` are optional. Possible values for
``broadness`` field are ``DEEP``, ``N<number>``, ``BROAD``.

Stopping the crawl via ``dd-crawler-input``, and
``dd-crawler-output-pages``, ``dd-crawler-output-progress`` work exactly the same
as the corresponding dd-trainer queues.

Incoming: add/remove hints, ``dd-crawler-hints-input``::

    {
      "workspace_id": "id of the workspace",
      "url": "the pinned url",
      "pinned": true / false,
    }


Running with docker
-------------------

Note that the docker client API version on host must be
not older than server docker API version in Ubuntu 16.04
(currently 1.24, you can check it with ``docker version``).
Minimal ``docker-compose`` version is 1.10.

Install docker and docker-compose (assuming Ubuntu 16.04)::

    sudo apt install -y docker.io python-pip
    sudo -H pip install docker-compose

Add yourself to docker group (requires re-login)::

    sudo usermod -aG docker <username>

For development, clone the repo and init submodules::

    git clone git@github.com:TeamHG-Memex/hh-deep-deep.git
    cd hh-deep-deep
    git submodule update --init

For production, just get ``docker-compose.yml`` and ``docker-compose.kafka-host.yml``
from this repo.

Download ``lda.pkl`` (not used at the moment)
and ``random-pages.jl.gz`` from ``s3://darpa-memex/thh/``
and put them to ``./models`` folder::

    cd models
    wget https://s3-us-west-2.amazonaws.com/darpa-memex/thh/random-pages.jl.gz
    wget https://s3-us-west-2.amazonaws.com/darpa-memex/thh/lda.pkl
    cd ..


If you are running kafka docker on the same host, include it in the docker-compose
config via several ``-f`` options. It should be exposed as ``hh-kafka`` and have
a health-check defined. An example is in ``docker-compose.kafka.yml``::

    docker-compose \
        -f docker-compose.yml \
        -f docker-compose.kafka-example.yml \
        up -d

If you are running kafka docker on a different host, export the host name::

    export KAFKA_HOST=1.2.3.4

and start all services with::

    docker-compose -f docker-compose.yml -f docker-compose.kafka-host.yml up -d

For development, in order to include locally built images,
include ``docker-compose.dev.yml`` file as well, and pass ``--build``,
for example::

    docker-compose \
        -f docker-compose.yml \
        -f docker-compose.kafka-example.yml \
        -f docker-compose.dev.yml \
        up --build

In order to update existing development installation, do::

    git pull
    git submodule update --init


Usage without Docker
--------------------

Run the service passing kafka host as ``--kafka-host``
(or leave it blank if testing locally)::

    hh-deep-deep-service [trainer|crawler] --kafka-host hh-kafka


Testing
-------

Install test requirements::

    pip install -r tests/requirements.txt

Start local kafka with::

    docker run -it --rm --name kafka \
        -p 2181:2181 -p 9092:9092 \
        --env ADVERTISED_HOST=127.0.0.1 \
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

For some reason, pushing messages does not work after container stop/start.

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

