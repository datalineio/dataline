# Connector Catalog

## Connector grades
Airbyte uses a grading system for connectors to help users understand what to expect from a connector. There are three grades, explained below:

**Certified**: This connector has been proven to be robust via usage by a large number of users and extensive testing.

**Beta**: While this connector is well tested and is expected to work a majority of the time, it was released recently. There may be some unhandled edge cases but Airbyte will provide very quick turnaround for support on any issues (we'll publish our target KPIs for support turnaround very soon). All beta connectors will make their way to certified status after enough field testing.

**Alpha**: This connector is either not sufficiently tested, has extremely limited functionality (e.g: created as an example connector), or for any other reason may not be very mature.

### Sources
| Connector | Grade |
|----|----|
|[Amazon Seller Partner](./sources/amazon-seller-partner.md)| Alpha |
|[Amplitude](./sources/amplitude.md)| Beta |
|[Appstore](./sources/appstore.md)| Alpha |
|[Asana](./sources/asana.md) | Beta |
|[AWS CloudTrail](./sources/aws-cloudtrail.md)| Beta |
|[Braintree](./sources/braintree.md)| Alpha |
|[ClickHouse](./sources/clickhouse.md)| Beta |
|[CockroachDB](./sources/cockroachdb.md)| Beta |
|[Db2](./sources/db2.md)| Beta |
|[Dixa](./sources/dixa.md) | Alpha |
|[Drift](./sources/drift.md)| Beta |
|[Exchange Rates API](./sources/exchangeratesapi.md)| Certified |
|[Facebook Marketing](./sources/facebook-marketing.md)| Beta |
|[Files](./sources/file.md)| Certified |
|[Freshdesk](./sources/freshdesk.md)| Certified |
|[GitHub](./sources/github.md)| Beta |
|[GitLab](./sources/gitlab.md)| Beta |
|[Google Ads](./sources/google-ads.md)| Beta |
|[Google Adwords](./sources/google-adwords.md)| Beta |
|[Google Analytics](./sources/googleanalytics.md)| Beta |
|[Google Directory](./sources/google-directory.md)| Certified |
|[Google Search Console](./sources/google-search-console.md)| Beta |
|[Google Sheets](./sources/google-sheets.md)| Certified |
|[Google Workspace Admin Reports](./sources/google-workspace-admin-reports.md)| Certified |
|[Greenhouse](./sources/greenhouse.md)| Beta |
|[HTTP Request](./sources/http-request.md)| Alpha |
|[Hubspot](./sources/hubspot.md)| Certified |
|[Instagram](./sources/instagram.md)| Certified |
|[Intercom](./sources/intercom.md)| Beta |
|[Iterable](./sources/iterable.md)| Beta |
|[Jira](./sources/jira.md)| Certified |
|[Looker](./sources/looker.md)| Beta |
|[Klaviyo](./sources/klaviyo.md)| Beta |
|[Mailchimp](./sources/mailchimp.md)| Certified |
|[Marketo](./sources/marketo.md)| Certified |
|[Microsoft SQL Server \(MSSQL\)](./sources/mssql.md)| Certified |
|[Microsoft Teams](./sources/microsoft-teams.md)| Certified |
|[Mixpanel](./sources/mixpanel.md)| Beta |
|[Mongo DB](./sources/mongodb.md)| Alpha |
|[MySQL](./sources/mysql.md)| Certified |
|[Okta](./sources/okta.md)| Beta |
|[Oracle DB](./sources/oracle.md)| Certified |
|[PayPal Transaction](./sources/paypal-transaction.md)| Beta |
|[Plaid](./sources/plaid.md)| Alpha |
|[PokéAPI](./sources/pokeapi.md)| Beta |
|[Postgres](./sources/postgres.md)| Certified |
|[PostHog](./sources/posthog.md)| Beta |
|[Quickbooks](./sources/quickbooks.md)| Beta |
|[Recharge](./sources/recharge.md)| Beta |
|[Recurly](./sources/recurly.md)| Beta |
|[Redshift](./sources/redshift.md)| Certified |
|[Salesforce](./sources/salesforce.md)| Certified |
|[Sendgrid](./sources/sendgrid.md)| Certified |
|[Shopify](./sources/shopify.md)| Certified |
|[Slack](./sources/slack.md)| Beta |
|[Smartsheets](./sources/smartsheets.md)| Beta |
|[Snowflake](./sources/snowflake.md)| Beta |
|[Square](./sources/square.md)| Beta |
|[Stripe](./sources/stripe.md)| Certified |
|[SurveyMonkey](./sources/surveymonkey.md)| Beta |
|[Tempo](./sources/tempo.md)| Beta |
|[Twilio](./sources/twilio.md)| Beta |
|[Zendesk Chat](./sources/zendesk-chat.md)| Certified |
|[Zendesk Sunshine](./sources/zendesk-sunshine.md)| Beta |
|[Zendesk Support](./sources/zendesk-support.md)| Certified |
|[Zendesk Talk](./sources/zendesk-talk.md)| Certified |
|[Zoom](./sources/zoom.md)| Beta |

### Destinations
| Connector | Grade |
|----|----|
|[BigQuery](./destinations/bigquery.md)| Certified |
|[Google Cloud Storage (GCS)](./destinations/s3.md)| Alpha |
|[Google Pubsub](./destinations/pubsub.md)| Alpha |
|[Local CSV](./destinations/local-csv.md)| Certified |
|[Local JSON](./destinations/local-json.md)| Certified |
|[MeiliSearch](./destinations/meilisearch.md)| Beta |
|[MySQL](./destinations/mysql.md)| Alpha |
|[Oracle](./destinations/oracle.md)| Alpha |
|[Postgres](./destinations/postgres.md)| Certified |
|[Redshift](./destinations/redshift.md)| Certified |
|[S3](./destinations/s3.md)| Certified |
|[SQL Server (MSSQL)](./destinations/mssql.md)| Alpha |
|[Snowflake](./destinations/snowflake.md)| Certified |
