## How to connect to Snowsql

`snowsql -o log_level=DEBUG`

## How to put file to stage

`PUT 'file://C:/Users/pksk6/OneDrive_Latest/OneDrive/Study_Related/Data_Build_Tool(DBT)_and_Snowflake/Snow_Flake_DataEngineering_Simplied_Beginner/ch07.csv' @MY_STG;`

## How to list all files present in a stage

`list @ch7_stg;`

## How to create file format for data from a stage

`create or replace file format my_format type = 'csv' field_delimiter = ',';`

## How to view data from stage

`select t.$1, t.$2, t.$3,t.$4, t.$5, t.$6 from @my_stg (file_format => 'my_format') t;`

## How to copy data from stage to table

<!-- https://youtu.be/Pi3z1NyBd-Y?si=O2wa71gDvAi8LSct&t=1650 -->

`copy into my_stg_table from @my_stg;`

## How to create an external table (for loading data from outside locations like s3 etc.)

`create or replace external table json_weather_data_et (
        time varchar AS (value:c1::varchar), 
        .... 
    )
    with location=@nyc_weather
    auto_refresh = false
    file_format = (format_name = file_format)`

 



