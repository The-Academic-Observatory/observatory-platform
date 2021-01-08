# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN /bin/bash -c "pip3 install --no-cache-dir -r requirements.txt"

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 server:app

#FROM ubuntu:20.04
#
#ENV LC_ALL=C.UTF-8
#ENV LANG=C.UTF-8
#
#RUN apt-get update -y && apt-get install -y \
#    python3-pip \
#    git
#
#ADD requirements.txt /app/
#WORKDIR /app
#RUN /bin/bash -c "pip3 install --no-cache-dir -r requirements.txt"
#
#ADD . /app/
#
#EXPOSE 5000
#
#CMD ["gunicorn", "-b", "0.0.0.0:5000", "server"]