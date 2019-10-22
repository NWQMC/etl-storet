# etl\-storet

[![Build Status](https://travis-ci.org/NWQMC/etl-storet.svg?branch=master)](https://travis-ci.org/NWQMC/etl-storet) [![Codacy Badge](https://api.codacy.com/project/badge/Grade/a8a8c5e7c3d2440bb7a16e7c33fa614a)](https://www.codacy.com/app/usgs_wma_dev/etl-storet?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=NWQMC/etl-storet&amp;utm_campaign=Badge_Grade)

ETL Water Quality Data from the Legacy STORETW System

## Development
This is a Spring Batch/Boot project. All of the normal caveats relating to a Spring Batch/Boot application apply.

### Dependencies
This application utilizes a PostgreSQL database. The Docker Hub image usgswma/wqp_db:etl can be used for testing.

### Environment variables
Create an application.yml file in the project directory containing the following (shown are example values - they should match the values you used in creating the etlDB):

```yml
EPA_DATABASE_ADDRESS: <localhost>
EPA_DATABASE_PORT: <5437>
EPA_DATABASE_NAME: <wqp_db>
EPA_SCHEMA_OWNER_USERNAME: <epa_owner>
EPA_SCHEMA_OWNER_PASSWORD: <changeMe>

STORETW_SCHEMA_NAME: <storetw>
STORETW_DUMP_SCHEMA_NAME: <storetw_dump>

ETL_DATA_SOURCE_ID: <3>
ETL_DATA_SOURCE: <STORET>

```

#### Environment variable definitions
##### EPA Schema
*   **EPA_DATABASE_ADDRESS** - Host name or IP address of the PostgreSQL database.
*   **EPA_DATABASE_PORT** - Port the PostgreSQL Database is listening on.
*   **EPA_DATABASE_NAME** - Name of the PostgreSQL database containing the schema.
*   **EPA_SCHEMA_OWNER_USERNAME** - Role which owns the **WQX_SCHEMA_NAME** and **STORETW_SCHEMA_NAME** database objects.
*   **EPA_SCHEMA_OWNER_PASSWORD** - Password for the **EPA_SCHEMA_OWNER_USERNAME** role.
*   **STORETW_SCHEMA_NAME** - Name of the schema holding STORETW database objects.
*   **STORETW_DUMP_SCHEMA_NAME** - Name of the schema holding STORETW_DUMP database objects.

##### Miscellaneous
*   **ETL_DATA_SOURCE_ID** - Database ID of the data_source (data_source_id from the **WQP_SCHEMA_NAME**.data_source table).
*   **ETL_DATA_SOURCE** - Data Source name (text from the **WQP_SCHEMA_NAME**.data_source table).

### Testing
This project contains JUnit tests. Maven can be used to run them (in addition to the capabilities of your IDE).

To run the unit tests of the application use:

```shell
mvn package
```

To additionally start up a Docker database and run the integration tests of the application use:

```shell
mvn verify -DTESTING_DATABASE_PORT=5437 -DTESTING_DATABASE_ADDRESS=localhost -DTESTING_DATABASE_NETWORK=etlStoret
```
