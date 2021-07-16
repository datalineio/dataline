# Rockset

Rockset is a serverless real-time database serving millisecond-latency queries
on terabytes of data.

## Sync overview

### Output schema

Each stream will correspond to a single collection in Rockset. All collections created will
exist under the workspace configured when the destination is created. The data schema will remain simply
as given by the source, with the usual `_id`, `_meta`, and `_event_time` fields added to each record. See
the [Rockset documentation](https://docs.rockset.com/special-fields/#the-_id-field) for more information on these fields.

### Features

| Feature | Supported?(Yes/No) |
| :--- | :--- |
| Full Refresh Sync | Yes  |
| Incremental Sync | Yes |

### Performance considerations

Collection counts should be considered when using Airbyte on Rockset from multiple Airbyte sources.
By default, 30 collections are allowed in the Shared tier and 10 collections in the Free tier.

## Getting started

### Requirements

The Rockset Airbyte destination is quite simple. To get started, you will need:

* A valid Rockset Api Key

With your api key entered during the destination creation step and a workspace name of your choice
(which does not need to exist prior to creating the connection), you are ready to get started using Airbyte
on top of Rockset!
