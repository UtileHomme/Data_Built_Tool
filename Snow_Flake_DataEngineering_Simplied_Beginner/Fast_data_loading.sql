-- https://www.youtube.com/watch?v=lI5OQPjuj-8&list=PLba2xJ7yxHB73xHFsyu0YViu3Hi6Ckxzj&index=9


use role sysadmin;
use database my_db;
create schema ch10;

CREATE WAREHOUSE ch10_demo_wh WITH WAREHOUSE_SIZE = 'MEDIUM' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE MIN_CLUSTER_COUNT = 1 MAX_CLUSTER_COUNT = 1 SCALING_POLICY = 'STANDARD' COMMENT = 'this is demo warehouses';
use warehouse ch10_demo_wh;

alter session set query_tag ='chapter-10';

-- you can craete named stages and then list them using list command
    show stages;

-- How to list the data in a particular stage
-- list @STG01;

-- very simple contruct for internal stage
    CREATE STAGE "MY_DB"."CH10".stg03 COMMENT = 'This is my demo internal stage';
    
    -- if you have lot of stages, then you can use like 
    show stages like '%03%';
    
    show stages like '%s3%';
    -- if it has credential, it will show

-- In case we want to search for something particular in a folder or subfolder

    list @~ pattern='.*test.*';
    list @~ pattern='.*.gz';
    list @~ pattern='.*.html';

     show stages like 'TIPS_S3_EXTERNAL_STAGE';
    list @TIPS_S3_EXTERNAL_STAGE;

    drop table customer_parquet_ff;
    create or replace table customer_parquet_ff(
        my_data variant
    ) 
    STAGE_FILE_FORMAT = (TYPE = PARQUET);

    list @%customer_parquet/;
    -- now lets query the data using $ notation
    select 
        metadata$filename, 
        metadata$file_row_number,
        $1:CUSTOMER_KEY::varchar,
        $1:NAME::varchar,
        $1:ADDRESS::varchar,
        $1:COUNTRY_KEY::varchar,
        $1:PHONE::varchar,
        $1:ACCT_BAL::decimal(10,2),
        $1:MKT_SEGMENT::varchar,
        $1:COMMENT::varchar
        from @%customer_parquet_ff ;

copy into customer_parquet_ff from @%customer_parquet_ff/customer.snappy.parquet;
      
      select * from  customer_parquet_ff;

copy into customer_parquet_ff 
        from @%customer_parquet_ff/customer.snappy.parquet
        force=true;



