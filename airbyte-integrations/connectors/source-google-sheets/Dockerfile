FROM airbyte/integration-base-python:0.1.1

RUN apt-get update && apt-get install -y jq curl bash && rm -rf /var/lib/apt/lists/*

ENV CODE_PATH="google_sheets_source"
ENV AIRBYTE_IMPL_MODULE="google_sheets_source"
ENV AIRBYTE_IMPL_PATH="GoogleSheetsSource"

WORKDIR /airbyte/integration_code
COPY $CODE_PATH ./$CODE_PATH
COPY setup.py ./
RUN pip install .

ENV AIRBYTE_ENTRYPOINT "/airbyte/base.sh"

LABEL io.airbyte.version=0.2.4
LABEL io.airbyte.name=airbyte/source-google-sheets
