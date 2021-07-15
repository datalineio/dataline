FROM elixir:1.11-alpine as builder

ENV MIX_ENV=prod

WORKDIR /app

ADD . .

# this is due to hackney being intalled as a git dependency
# remove when hackney 1.16.1 is released
RUN apk add --no-cache git

# Fetch dependencies
RUN mix local.hex --force && \
  mix local.rebar --force && \
  mix deps.get

# Compile app
RUN mix compile && mix escript.build

FROM erlang:23-alpine

WORKDIR /app

COPY --from=builder /app/airbyte_source .

ENTRYPOINT ["/app/airbyte_source"]

LABEL io.airbyte.version=0.1.0
LABEL io.airbyte.name=airbyte/source-google-analytics
