FROM python:3.5

WORKDIR /opt/hh-deep-deep

RUN apt-get update && apt-get install -y tree

COPY requirements.txt .
RUN pip install -U pip wheel && \
    pip install -r requirements.txt

COPY . .
RUN tree
RUN pip install -e .

CMD hh-deep-deep-service --kafka-host hh-kafka
