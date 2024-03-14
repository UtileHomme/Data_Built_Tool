-- https://www.youtube.com/watch?v=w9BQsOlJc5s&list=PLba2xJ7yxHB73xHFsyu0YViu3Hi6Ckxzj&index=11&ab_channel=DataEngineeringSimplified

-- https://data-engineering-simplified.medium.com/work-with-external-table-in-snowflake-b86714f5a6df


    use role accountadmin;



    use database my_db;

    create schema ch11;

    use schema ch11;

    use warehouse CH10_DEMO_WH;

    -- lets create external stage in s3
    drop stage s3_customer_csv;
    create or replace stage s3_customer_csv 
        url = 's3://snowflake-s3-ch11-jatin/customer/csv/' 
        comment = 'this customer parquet data';
        
    -- desc the stage and validate the definition
    -- and on_error parameters
    desc stage s3_customer_csv;
    
    -- lets list the stage to see all the files and path 
    list @s3_customer_csv;
    
    -- create a csv file format
    create file format csv_ff 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 0 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');
        
        
     -- select external stage data using $ notation without creating a table to load data
     select t.$1, t.$2, t.$3,t.$4, t.$5, t.$6 , t.$7, t.$8 
        from 
     @s3_customer_csv (file_format => 'csv_ff') t; 
        
    -- what if you give one. extra colum in your $ notation
    select t.$1, t.$2, t.$3,t.$4, t.$5, t.$6 , t.$7, t.$8, t.$9 
        from 
    @s3_customer_csv (file_format => 'csv_ff') t; 

    // it will appear null, so be careful with such column.
     
drop table customer_csv_et;
create or replace external TABLE customer_csv_et (
        CUST_KEY varchar AS (value:c1::varchar),
        NAME varchar AS (value:c2::varchar),
        ADDRESS varchar AS (value:c3::varchar),
        NATION_KEY varchar AS (value:c4::varchar),
        PHONE varchar AS (value:c5::varchar),
        ACCOUNT_BALANCE varchar AS (value:c6::varchar),
        MARKET_SEGMENT varchar AS (value:c7::varchar),
        COMMENT varchar AS (value:c8::varchar)
    )
with location=@s3_customer_csv
auto_refresh = false
file_format = (format_name = csv_ff)
;

-- lets see the extra value column and it is available in json format.
select count(*) from customer_csv_et;


create or replace external TABLE customer_csv_et_dummy (
    )
with location=@s3_customer_csv
auto_refresh = false
file_format = (format_name = csv_ff);

-- select the table
select * from customer_csv_et;

-- fetch the value column (json for each row) and metadata column (filename from which data was loaded)
select value, metadata$filename from customer_csv_et;

-- select clause
select value, metadata$filename from customer_csv_et where metadata$filename ='customer/csv/customer_001.csv';

-- if you are not sure about the column names etc, then you can simply add a dummy column and check all values
create or replace external TABLE customer_csv_et_dummy (
      col1 varchar AS (value:c1::varchar)
    )
    with location=@s3_customer_csv
    auto_refresh = false
    file_format = (format_name = csv_ff);

    select * from customer_csv_et_dummy;


-- shows the description of each column
desc external table customer_csv_et type = 'column';

-- gives all the properties associated with the stage in which this table is present
desc external table customer_csv_et type = 'stage';








