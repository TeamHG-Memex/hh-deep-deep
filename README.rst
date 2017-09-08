THH deep-deep integration
=========================

This is a service that listens to kafka topic and starts/stops deep-deep crawler,
sends back progress updates, samples of crawled pages and the link model.
It also starts the broad crawl using trained link model.

Docker-compose (see "Running with docker" below) also starts the hh-page-classifier
image that is responsible for training the page score model
(see https://github.com/TeamHG-Memex/hh-page-classifier).


.. contents::

API
---

API docs are in ``docs/API.rst`` in https://github.com/TeamHG-Memex/sitehound/
repo.


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

Build images for testing::

    docker build -t dd-crawler-hh -f docker/dd-crawler.docker docker/
    docker build -t deep-deep-hh -f docker/deep-deep.docker docker/
    docker build -t hh-deep-deep-test-server tests/
    docker build -t hh-kafka -f docker/kafka.docker docker/

Start kafka test server::

    docker run -it --rm --name kafka \
        -p 2181:2181 -p 9092:9092 \
        --env ADVERTISED_HOST=127.0.0.1 \
        --env ADVERTISED_PORT=9092 \
        hh-kafka

Start login test server::

    docker run --rm -it --name hh-deep-deep-test-server \
        hh-deep-deep-test-server login

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

