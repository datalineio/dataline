FROM golang:1.14

ENV GO111MODULE=on


# todo (cgardens) - replace this forked commit with an official version when a fix is released.
#RUN go get -u github.com/danielgtaylor/restish@v0.7.0
RUN go get -u github.com/danielgtaylor/restish@47de8a55eae6c0ab38e35119bef52ccf2837e3b3

ENTRYPOINT ["restish"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/cli
