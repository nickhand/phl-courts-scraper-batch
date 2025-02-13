FROM python:3.9-slim AS base

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=2.0.1 \ 
    DISPLAY=:99

# Update
RUN apt-get update

# install the needed packages including java and a perl library which we
# need for fastqc
RUN apt-get -y install wget gnupg git

# install google chrome
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# RUN apt-get -y update --fix-missing
# RUN apt-get install -y google-chrome-stable

# Install firefox
RUN apt-get install -y firefox-esr

# Install poetry
RUN pip install "poetry==$POETRY_VERSION"

# Copy only requirements to cache them in docker layer
WORKDIR /code
COPY poetry.lock pyproject.toml /code/

# Install the dependencies
RUN poetry install --no-root

# Creating folders, and files for a project:
COPY . /code
RUN poetry install 

# Run the executable
ENTRYPOINT [ "poetry" ]
CMD [ "run", \
    "phl-courts-scraper-batch", \
    "scrape", \
    "portal", \
    "s3://phl-courts-scraper/tests/test.csv", \
    "s3://phl-courts-scraper/tests/results", \
    "--search-by", \
    "Incident Number", \
    "--sample=3", \
    "--log-freq=1", \
    "--sleep=2", \
    "--browser=firefox" ]

