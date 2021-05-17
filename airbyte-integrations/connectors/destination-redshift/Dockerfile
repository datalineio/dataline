FROM airbyte/integration-base-java:dev

WORKDIR /airbyte

ENV APPLICATION destination-redshift

RUN apt-get update && apt-get install -y procps && rm -rf /var/lib/apt/lists/*

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

EXPOSE 6000
RUN tar xf ${APPLICATION}.tar --strip-components=1

LABEL io.airbyte.version=0.3.7
LABEL io.airbyte.name=airbyte/destination-redshift
