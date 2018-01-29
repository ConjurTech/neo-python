FROM ubuntu:16.04

RUN apt-get update && apt-get -y install git python3-dev python3-pip libleveldb-dev libssl-dev man

WORKDIR neo-python

#RUN pip3 install -r requirements.txt

COPY requirements.txt /tmp/
RUN pip3 install --requirement /tmp/requirements.txt
COPY . /tmp/

COPY . .

CMD python3 eventsManager.py
