FROM openjdk:15.0.1-slim AS server

EXPOSE 8000

ENV WAIT_VERSION=2.7.2
ENV APPLICATION airbyte-server

WORKDIR /app

# Install wait
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/${WAIT_VERSION}/wait wait
RUN chmod +x wait

COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "./wait && bin/${APPLICATION}"]
