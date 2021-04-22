# Local JSONL Destination 

This is the repository for the JSONL Destination source connector, written in Python. 
This is meant for illustration purposes and not for use in production cases. 

## Local development

### Prerequisites
**To iterate on this connector, make sure to complete this prerequisites section.**

#### Build & Activate Virtual Environment and install dependencies
From this connector directory, create a virtual environment:
```
python -m venv .venv
```

This will generate a virtualenv for this module in `.venv/`. Make sure this venv is active in your
development environment of choice. To activate it from the terminal, run:
```
source .venv/bin/activate
pip install -r requirements.txt
```
If you are in an IDE, follow your IDE's instructions to activate the virtualenv.

Note that while we are installing dependencies from `requirements.txt`, you should only edit `setup.py` for your dependencies. `requirements.txt` is
used for editable installs (`pip install -e`) to pull in Python dependencies from the monorepo and will call `setup.py`.
If this is mumbo jumbo to you, don't worry about it, just put your deps in `setup.py` but install using `pip install -r requirements.txt` and everything
should work as you expect.

### Locally running the connector
```
# Get the required config parameters to run the connector
python main.py spec
# Check if the provided config is valid
python main.py check --config sample_files/local_config.json
# Write data using the config and catalog by piping some records into the destination to simulate real input 
cat sample_files/inputs.jsonl | python main.py read --config sample_files/local_config.json --catalog sample_files/configured_catalog.json
```

### Locally running the connector docker image

#### Build
First, make sure you build the latest Docker image:
```
docker build . -t airbyte/destination-jsonl-python:dev
```

#### Run
Then run any of the connector commands as follows:
```
docker run --rm airbyte/destination-jsonl-python:dev spec
docker run --rm -v $(pwd)/sample_files:/sample_files airbyte/destination-jsonl-python:dev -v /tmp/airbyte_jsonl:/local check --config /sample_files/docker_config.json
cat sample_files/inputs.jsonl | docker run --rm -v $(pwd)/sample_files:/sample_files -v /tmp/airbyte_jsonl:/local airbyte/destination-jsonl-python:dev write --config /sample_files/docker_config.json --catalog /sample_files/configured_catalog.json
```

## Dependency Management
All of your dependencies should go in `setup.py`, NOT `requirements.txt`. The requirements file is only used to connect internal Airbyte dependencies in the monorepo for local development.

### Publishing a new version of the connector
You've checked out the repo, implemented a million dollar feature, and you're ready to share your changes with the world. Now what?
1. Make sure your changes are passing unit and integration tests
1. Bump the connector version in `Dockerfile` -- just increment the value of the `LABEL io.airbyte.version` appropriately (we use SemVer).
1. Create a Pull Request
1. Pat yourself on the back for being an awesome contributor
1. Someone from Airbyte will take a look at your PR and iterate with you to merge it into master
