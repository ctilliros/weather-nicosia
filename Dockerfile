FROM python:3.8
RUN mkdir /usr/src/app
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN apt-get update
RUN apt-get install -y libspatialindex-dev libspatialindex-c5 libspatialindex5 vim
RUN pip3 install  -r requirements.txt
COPY . .