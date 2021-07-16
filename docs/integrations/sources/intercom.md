# Intercom

## Overview

The Intercom source supports both Full Refresh and Incremental syncs. You can choose if this connector will copy only the new or updated data, or all rows in the tables and columns you set up for replication, every time a sync is run.

This Source Connector is based on a [Airbyte CDK](https://docs.airbyte.io/contributing-to-airbyte/python).

### Output schema

Several output streams are available from this source:

* [Admins](https://developers.intercom.com/intercom-api-reference/reference#list-admins) \(Full table\)
* [Companies](https://developers.intercom.com/intercom-api-reference/reference#list-companies) \(Incremental\)
    * [Company Segments](https://developers.intercom.com/intercom-api-reference/reference#list-attached-segments-1) \(Incremental\)
* [Conversations](https://developers.intercom.com/intercom-api-reference/reference#list-conversations) \(Incremental\)
  * [Conversation Parts](https://developers.intercom.com/intercom-api-reference/reference#get-a-single-conversation) \(Incremental\)
* [Data Attributes](https://developers.intercom.com/intercom-api-reference/reference#data-attributes) \(Full table\)
  * [Customer Attributes](https://developers.intercom.com/intercom-api-reference/reference#list-customer-data-attributes) \(Full table\)
  * [Company Attributes](https://developers.intercom.com/intercom-api-reference/reference#list-company-data-attributes) \(Full table\)
* [Contacts](https://developers.intercom.com/intercom-api-reference/reference#list-contacts) \(Incremental\)
* [Segments](https://developers.intercom.com/intercom-api-reference/reference#list-segments) \(Incremental\)
* [Tags](https://developers.intercom.com/intercom-api-reference/reference#list-tags-for-an-app) \(Full table\)
* [Teams](https://developers.intercom.com/intercom-api-reference/reference#list-teams) \(Full table\)

If there are more endpoints you'd like Airbyte to support, please [create an issue.](https://github.com/airbytehq/airbyte/issues/new/choose)

### Features

| Feature | Supported? |
| :--- | :--- |
| Full Refresh Sync | Yes |
| Incremental - Append Sync | Yes |
| Replicate Incremental Deletes | Coming soon |
| SSL connection | Yes |
| Namespaces | No |

### Performance considerations

The Intercom connector should not run into Intercom API limitations under normal usage. Please [create an issue](https://github.com/airbytehq/airbyte/issues) if you see any rate limit issues that are not automatically retried successfully.

## Getting started

### Requirements

* Intercom Access Token

### Setup guide

Please read [How to get your Access Token](https://developers.intercom.com/building-apps/docs/authentication-types#section-how-to-get-your-access-token).

