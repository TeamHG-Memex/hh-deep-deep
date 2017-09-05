FROM python:3.5

RUN pip3 install twisted==17.5.0
COPY server.py .

EXPOSE 8781

ENTRYPOINT ["python3", "server.py"]
