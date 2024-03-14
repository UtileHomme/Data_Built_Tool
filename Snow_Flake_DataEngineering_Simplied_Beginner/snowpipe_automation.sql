-- https://www.youtube.com/watch?v=PNK49SJvXjE&list=PLba2xJ7yxHB73xHFsyu0YViu3Hi6Ckxzj&index=10
-- https://data-engineering-simplified.medium.com/continuous-data-loading-in-snowflake-bec729ec0e53
drop table if exists my_customer;

create
or replace table my_customer (
    NAME VARCHAR(25),
    ADDRESS VARCHAR(500),
    PHONE VARCHAR(15),
    MARKET_SEGMENT VARCHAR(1000),
    COMMENT VARCHAR(1000)
);

--lets see if it has any record
select
    *
from
    my_customer;

create or replace file format csv_ff type = 'csv' field_optionally_enclosed_by = '\042';

-- before check if stg exist and has data
show stages like '%_010%';

-- let's create it.
create
or replace stage stg_010 file_format = csv_ff comment = 'This is my internal stage for ch-10';

-- let's describe the stage using desc sql command
desc stage stg_010;

list @stg_010/pattern='*.csv';

-- ***********************************
-- Step-05
-- load into table

PUT 'file://C:\Users\jsharma029\OneDrive - pwc\Data_Built_Tool\my_customer_data.csv' @stg_010/history auto_compress=false;


copy into my_customer from @stg_010/my_customer_data.csv;

-- truncate table my_customer;
select count(*) from my_customer;

-- truncate table my_customer;

-- to save the cost, we should remove data from the stage

remove @stg_010/my_customer_data.csv;

-- create a pipe object & understand its construct. 
drop pipe my_pipe_10;

create
or replace pipe my_pipe_10 as copy into my_customer 
from
    @stg_010/;

-- describe the pipe object 
desc pipe my_pipe_10;

list @stg_010;

select * from table(validate_pipe_load(pipe_name => 'my_pipe_10',start_time => dateadd(hour, -1, current_timestamp())));

select pip_usage_history(PIPE_NAME = 'my_pipe_10');

select *
  from table(information_schema.pipe_usage_history(
    date_range_start=>dateadd('day',-1,current_timestamp()),
    pipe_name=>'MY_DB.CH10.my_pipe_10'));

alter pipe my_pipe_10
set
    pipe_execution_paused = false;

    select SYSTEM$PIPE_STATUS( 'my_pipe_10' );

alter pipe my_pipe_10 refresh prefix = '/customer_10*' modified_after = '2021-11-01T13:56:46-07:00';

use role accountadmin;

alter user UTILEHOMME123 set rsa_public_key='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAwTPKpJ85ZPZxfpUvLiyD
WCSlyRi87SI9lOV00ZDWtm/w7ZWtrzm0JUpdQVW6TnEWkYFw0avUbZiitS+o/Lp8
YMmu6jgB512RpNz0mXmdFGdlHXJDC9Z37jrqh+pp4y/kJPa9FoknqRdpi4YKH7ZX
4DpTTvAh84NVpc51utdv2qcNdPaJM9wYj9xPjzF+Q0yTMiQjgRm30W/54HpgGbAc
nnNtN7I5999Pi4E4qczbUfixazo38OzvANvv6H69uzGFdQRiOox/ZCp9+4Y3siEk
rR/EDRd8BFWqQx4CDYdQ+Q/K7eEU+Xhu1BwwhCSmO8rv8LiWeHH5nTvoIlJqZL9e
4wIDAQAB';

alter user UtileHomme123 set DEFAULT_ROLE=ACCOUNTADMIN;

use role accountadmin;

-- select * from "my_db"."account_usage"."copy_history" where pipe_name = "MY_DB.CH10.my_pipe_10";

-- select *
-- from table(information_schema.copy_history(TABLE_NAME=>'MYTABLE', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

-- USE DATABASE mydb;

SELECT table_name, last_load_time
  FROM information_schema.load_history
  WHERE schema_name=current_schema() AND
  table_name='my_customer' AND
  last_load_time > 'Fri, 01 Apr 2016 16:00:00 -0800';