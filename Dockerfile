FROM rappdw/docker-java-python:openjdk1.8.0_171-python3.6.6
WORKDIR /TRG

COPY src /TRG/src
COPY resources /TRG/resources
COPY requirements.txt .
ENV PYTHONPATH="/TRG"

RUN pip3 install -r requirements.txt