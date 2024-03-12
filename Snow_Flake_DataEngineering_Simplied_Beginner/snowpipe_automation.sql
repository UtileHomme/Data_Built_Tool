-- https://www.youtube.com/watch?v=PNK49SJvXjE&list=PLba2xJ7yxHB73xHFsyu0YViu3Hi6Ckxzj&index=10
-- https://data-engineering-simplified.medium.com/continuous-data-loading-in-snowflake-bec729ec0e53
drop table if exists my_customer;

create
or replace table my_customer (
    NAME VARCHAR(25),
    ADDRESS VARCHAR(500),
    PHONE VARCHAR(15),
    MARKET_SEGMENT VARCHAR(1000),
    COMMENT VARCHAR(117)
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
select * from my_customer;

-- to save the cost, we should remove data from the stage

remove @stg_010/my_customer_data.csv;

-- create a pipe object & understand its construct. 
drop pipe my_pipe_10;

create
or replace pipe my_pipe_10 as copy into my_customer
from
    @stg_010/my_customer_data.csv;

-- describe the pipe object 
desc pipe my_pipe_10;

list @stg_010;

select * from table(validate_pipe_load(pipe_name => 'my_pipe_10',start_time => dateadd(hour, -1, current_timestamp())));

alter pipe my_pipe_10
set
    pipe_execution_paused = false;

alter pipe my_pipe_10 refresh prefix = '/customer_10*' modified_after = '2021-11-01T13:56:46-07:00';