FROM openjdk:15.0.1-slim AS scheduler


# Install Docker to launch worker images. Eventually should be replaced with Docker-java.
# See https://gitter.im/docker-java/docker-java?at=5f3eb87ba8c1780176603f4e for more information on why we are not currently using Docker-java
RUN apt-get update && apt-get install -y \
                          apt-transport-https \
                          ca-certificates \
                          curl \
                          gnupg-agent \
                          software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository \
       "deb [arch=amd64] https://download.docker.com/linux/debian \
       $(lsb_release -cs) \
       stable"
RUN apt-get update && apt-get install -y docker-ce-cli jq

ENV WAIT_VERSION=2.7.2
ENV APPLICATION airbyte-scheduler

WORKDIR /app

# Install wait
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/${WAIT_VERSION}/wait wait
RUN chmod +x wait

# Install kubectl
RUN curl -LO https://storage.googleapis.com/kubernetes-release/release/v1.17.14/bin/linux/amd64/kubectl
RUN chmod +x ./kubectl
RUN mv ./kubectl /usr/local/bin

# Move and run scheduler
COPY build/distributions/${APPLICATION}*.tar ${APPLICATION}.tar

RUN tar xf ${APPLICATION}.tar --strip-components=1

# wait for upstream dependencies to become available before starting server
ENTRYPOINT ["/bin/bash", "-c", "./wait && bin/${APPLICATION}"]
