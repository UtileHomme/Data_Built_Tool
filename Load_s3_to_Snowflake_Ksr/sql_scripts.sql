-- https://www.youtube.com/watch?v=roXlSAvx6Kg&ab_channel=KSRDatavizon

-- How to create Storage Integration

Create storage integration AWS_S3_Integration
type = external_stage
enabled = true
storage_provider = s3
storage_allowed_locations = ('s3://snowflake-s3-ch11-jatin/customer/csv/')
storage_aws_role_arn = 'arn:aws:iam::218876340004:role/s3_ksr_snowflake_role';

desc storage integration AWS_S3_Integration;

create stage aws_stage
url = 's3://snowflake-s3-ch11-jatin/customer/csv/'
storage_integration = AWS_S3_Integration;

ls @aws_stage;

select t.$1, t.$2, t.$3,t.$4, t.$5, t.$6 , t.$7, t.$8 from @aws_stage t;

   create file format csv_ff 
        type = 'csv' 
        compression = 'auto' 
        field_delimiter = ',' 
        record_delimiter = '\n' 
        skip_header = 0 
        field_optionally_enclosed_by = '\042' 
        null_if = ('\\N');

drop table customer_csv_s3;

create or replace TABLE customer_csv_s3 (
        CUST_KEY varchar,
        NAME varchar ,
        ADDRESS varchar,
        NATION_KEY varchar ,
        PHONE varchar ,
        ACCOUNT_BALANCE varchar ,
        MARKET_SEGMENT varchar ,
        COMMENT varchar 
    );
-- with location=@aws_stage
-- auto_refresh = false
-- file_format = (format_name = csv_ff)


select * from customer_csv_s3;

copy into customer_csv_s3
from @aws_stage
file_format = csv_ff;