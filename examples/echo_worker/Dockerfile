FROM python:3.6.7
MAINTAINER morishita@sprocket.bz

# Install Libraries
RUN mkdir -p /var/lib/app
WORKDIR /var/lib/app
RUN pip install -U pip pipenv

COPY ./Pipfile ./
# COPY ./Pipfile.lock ./
RUN pipenv install

# Copy Sources
COPY ./src/ ./src/
COPY status.txt ./

ENTRYPOINT ["pipenv", "run", "echo"]
