-- Setting up database and schema
CREATE DATABASE NetfliX_db;

CREATE SCHEMA RAW;

-- Granting access to snowflake role 
grant all on warehouse compute_wh to role TRANSFORM;
grant usage on database NetfliX_db to role TRANSFORM;
grant create schema on database NetfliX_db to role TRANSFORM; 
grant usage on schema NetfliX_db.raw to role TRANSFORM;
grant create table on schema NetfliX_db.raw to role TRANSFORM;
grant create view on schema NetfliX_db.raw to role TRANSFORM;
grant usage on future schemas in database NetfliX_db to role TRANSFORM;
grant monitor on future schemas in database NetfliX_db to role TRANSFORM;
grant select on future tables in database NetfliX_db to role TRANSFORM;
grant select on future views in database NetfliX_db to role TRANSFORM;
grant usage on all schemas in database NetfliX_db to role TRANSFORM;
grant monitor on all schemas in database NetfliX_db to role TRANSFORM;
grant select on all tables in database NetfliX_db to role TRANSFORM;
grant select on all views in database NetfliX_db to role TRANSFORM;

-- creating movie table
CREATE OR REPLACE TABLE movies (
    imdb_id varchar(10),
    title varchar, 
    rating int, 
    year date, 
    runtime int,
    top250 int, 
    top250tv int, 
    title_date date 
);


