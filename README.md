# etl\-storet

[![Codacy Badge](https://api.codacy.com/project/badge/Grade/5bef6e2972b0416f802869662631c1dd)](https://app.codacy.com/app/usgs_wma_dev/etl-storet?utm_source=github.com&utm_medium=referral&utm_content=NWQMC/etl-storet&utm_campaign=Badge_Grade_Dashboard)

ETL Water Quality Data from the Legacy STORETW System

This ETL is unique in that:

* The transformation SQL in encapsulated in a PL/SQL package.

* Primary key values are manipulated to avoid conflicts with the STORET WQX data.

* The output is fed into another ETL for actual inclusion in the WQP.

These scripts are run by the OWI Jenkins Job Runners. The job name is WQP\_STORETW\_ETL. They follow the general OWI ETL pattern using ant to control the execution of PL/SQL scripts.

The basic flow is:

* Rebuild the ETL PL/SQL Package. (create\_storet\_objects\_stage.sql)

* Copy the data download and cleanup scripts to the database (nolog) server.

* Make sure they have the correct end-of-line characters.

* Download data files as needed. (storet_dump.sh)

* Import the data into the storetw schema of the nolog database using impdp.

* Grant select on the storetw tables to wqp\_core. (storetw\_grants.sql)

* Transform the data into the station\_no\_source, activity\_no\_source, and result\_no\_source tables. (runWeeklyPackageStage.sql)

* Mark the current version of data as processed. (storet\_finish.sh)
