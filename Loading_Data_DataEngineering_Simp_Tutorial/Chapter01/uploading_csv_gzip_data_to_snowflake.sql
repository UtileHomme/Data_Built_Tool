
-- https://www.youtube.com/watch?v=ygZ04M2YcKs&list=PLba2xJ7yxHB6NPEv8pp7j3zWibLZzwjvO&index=2

-- https://gitlab.com/toppertips/snowflake-work/-/tree/main/loading-data-into-snowflake-playlist/track-01-ch-01?utm_source=youtube&utm_medium=desc-click&utm_campaign=internal-promotion

use role sysadmin;

use database my_db;

use schema my_schema;


create or replace transient table customer_csv (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

-- Creating file format
create or replace file format customer_csv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    skip_header = 1 ;

select current_version(), current_region();

create or replace file format customer_csv_ff2
    type = 'csv' 
    compression = 'none' 
    field_delimiter = ','
    -- handles the double quotes in data scenario due to which the data is not loaded into snowflake
    field_optionally_enclosed_by = '\042'
    skip_header = 1 ;

select * from my_db.information_schema.load_history;

-- truncate table customer_csv;

select * from customer_csv;

create or replace transient table customer_tsv (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

create or replace file format customer_tsv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = '\t'
    field_optionally_enclosed_by = '\042'
    skip_header = 1 ;

select * from customer_tsv;

create or replace transient table customer_psv (
	customer_pk number(38,0),
	salutation varchar(10),
	first_name varchar(20),
	last_name varchar(30),
	gender varchar(1),
	marital_status varchar(1),
	day_of_birth date,
	birth_country varchar(60),
	email_address varchar(50),
	city_name varchar(60),
	zip_code varchar(10),
	country_name varchar(20),
	gmt_timezone_offset number(10,2),
	preferred_cust_flag boolean,
	registration_time timestamp_ltz(9)
);

create or replace file format customer_psv_ff 
    type = 'csv' 
    compression = 'none' 
    field_delimiter = '|'
    field_optionally_enclosed_by = '\042'
    skip_header = 1 ;

select * from customer_psv limit 10;

-- How to put different actions when copy command is running

-- Option-1
-- On Error => Skip The file

COPY INTO "TTIPS"."CH01"."CUSTOMER_CSV" 
    FROM @/ui1664948747495 
    FILE_FORMAT = '"TTIPS"."CH01"."CUSTOMER_CSV_FF"' 
    ON_ERROR = 'SKIP_FILE' 
    PURGE = TRUE;
  
-- Option-2
-- On Error => Abort Statement

COPY INTO "TTIPS"."CH01"."CUSTOMER_CSV" 
    FROM @/ui1664948747495 
    FILE_FORMAT = '"TTIPS"."CH01"."CUSTOMER_CSV_FF"' 
    ON_ERROR = 'ABORT_STATEMENT' 
    PURGE = TRUE;

-- Option-3
-- On Error => Skip upto 10 error records
COPY INTO "TTIPS"."CH01"."CUSTOMER_CSV" 
    FROM @/ui1664948747495 
    FILE_FORMAT = '"TTIPS"."CH01"."CUSTOMER_CSV_FF"' 
    ON_ERROR = 'SKIP_FILE_10' 
    PURGE = TRUE;
    
-- Option-4
-- On Error => Continue even if there is an error
COPY INTO "TTIPS"."CH01"."CUSTOMER_CSV" 
    FROM @/ui1664948747495 
    FILE_FORMAT = '"TTIPS"."CH01"."CUSTOMER_CSV_FF"' 
    ON_ERROR = 'CONTINUE' 
    PURGE = TRUE;









