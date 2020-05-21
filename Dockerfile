####################
# Course: CSE138
# Date: Spring 2020
# Assignment: 1
# Author: Alan Vasilkovsky, Mariah Maninang, Bradley Gallardo, Omar Quinones
# Description: A dockerfile which initiates the latest python then downloads
#              and installs all of the necessary dependencies, and finally
#              sets the docker container up to run the flask application.
###################

# https://runnable.com/docker/python/dockerize-your-flask-application
# This link was used as a template for the dockerfile and requirements.txt


FROM python:latest

COPY . /app

WORKDIR /app

RUN pip install -r requirements.txt

EXPOSE 8085

ENTRYPOINT [ "python3" ]

CMD [ "app.py" ]