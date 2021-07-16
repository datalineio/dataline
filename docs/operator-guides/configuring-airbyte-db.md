# Configuring the Airbyte Internal Database

Airbyte uses different objects to store internal state and metadata. This data is stored and manipulated by the various Airbyte components, but you have the ability to manage the deployment of this database in the following two ways:
- Using the default Postgres database that Airbyte spins-up as part of the Docker service described in the `docker-compose.yml` file: `airbyte/db`.
- Through a dedicated custom Postgres instance (the `airbyte/db` is in this case unused, and can therefore be removed or de-activated from the `docker-compose.yml` file).

The various entities are persisted in two internal databases:

- Job database
  - Data about executions of Airbyte Jobs and various runtime metadata.
  - Data about the internal orchestrator used by Airbyte, Temporal.io (Tasks, Workflow data, Events, and visibility data).
- Config database
  - Connectors, Sync Connections and various Airbyte configuration objects.

Note that no actual data from the source (or destination) connectors ever transits or is retained in this internal database.

If you need to interact with it, for example, to make back-ups or perform some clean-up maintenances, you can also gain access to the Export and Import functionalities of this database via the API or the UI (in the Admin page, in the Configuration Tab).

## Connecting to an External Postgres database

Let's walk through what is required to use a Postgres instance that is not managed by Airbyte. First, for the sake of the tutorial, we will run a new instance of Postgres in its own docker container with the command below. If you already have Postgres running elsewhere, you can skip this step and use the credentials for that in future steps.
```bash
docker run --rm --name airbyte-postgres -e POSTGRES_PASSWORD=password -p 3000:5432 -d postgres
```

In order to configure Airbyte services with this new database, we need to edit the following environment variables declared in the `.env` file (used by the docker-compose command afterward):

```bash
DATABASE_USER=postgres
DATABASE_PASSWORD=password
DATABASE_HOST=host.docker.internal # refers to localhost of host
DATABASE_PORT=3000
DATABASE_DB=postgres
```

By default, the Config Database and the Job Database use the same database instance based on the above setting. It is possible, however, to separate the former from the latter by specifying a separate parameters. For example:

```bash
CONFIG_DATABASE_USER=airbyte_config_db_user
CONFIG_DATABASE_PASSWORD=password
CONFIG_DATABASE_PORT=5432
CONFIG_DATABASE_DB=airbyte_config_db
```

Additionally, you must redefine the JDBC URL constructed in the environment variable `DATABASE_URL` to include the correct host, port, and database. If you need to provide extra arguments to the JDBC driver (for example, to handle SSL) you should add it here as well:

```bash
DATABASE_URL=jdbc:postgresql://host.docker.internal:3000/postgres?ssl=true&sslmode=require
```

Same for the config database if it is separate from the job database:

```bash
CONFIG_DATABASE_URL=jdbc:postgresql://<host>:<port>/<database>?<extra-parameters>
```

## Initializing the database

{% hint style="info" %}
This step is only required when you setup Airbyte with a custom database for the first time.
{% endhint %}

If you provide an empty database to Airbyte and start Airbyte up for the first time, the server and scheduler services won't be able to start because there is no data in the database yet.

For the Job Database, you need to make sure that the proper tables have been created by running the init SQL script [here](https://github.com/airbytehq/airbyte/blob/master/airbyte-db/src/main/resources/schema.sql).

You can replace:
- "airbyte" with your actual "DATABASE_DB" value
- "docker" with your actual "DATABASE_USER" value
then run the SQL script to populate the database manually.

For the Config Database, tables will be created automatically. But you should make sure that the database exists, and the user have full access to it.

Now, when you run `docker-compose up`, the Airbyte server and scheduler should connect to the configured database successfully.

## When upgrading Airbyte

When updating Airbyte as described in [the upgrade process docs](upgrading-airbyte.md), scripts are also published in order to handle necessary migrations. These are introduced whenever we make changes to the data model.

Those migration scripts work primarily with an archive file to be updated. Once the archive is ready, the scripts assume that they are being applied on top of an empty database afterward, but with tables already created with the correct schema. They will re-populate and re-import whatever was saved in the upgraded archive back into the database.

Thus, if you deploy Airbyte using an external database, you might need to flush and perform updates to the table schemas by deleting them and re-initializing them as described previously (using the latest `schema.sql` script). This step is implicitly done on the default Docker Postgres database when running `docker-compose down -v` or when deleting Docker volumes).

## Accessing the default database located in docker airbyte-db
In extraordinary circumstances while using the default `airbyte-db` Postgres database, if a developer wants to access the data that tracks jobs, they can do so with the following instructions.

As we've seen previously, the credentials for the database are specified in the `.env` file that is used to run Airbyte. By default, the values are:
```shell
DATABASE_USER=docker
DATABASE_PASSWORD=docker
DATABASE_DB=airbyte
```

If you have overridden these defaults, you will need to substitute them in the instructions below.

The following command will allow you to access the database instance using `psql`. 

```shell
docker exec -ti airbyte-db psql -U docker -d airbyte
```
