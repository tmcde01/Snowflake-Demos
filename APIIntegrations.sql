-- THIS IS AN EXAMPLE OF CONNECTING TO AN API DIRECTLY WITH SNOWFLAKE
-- THE API IS THEN CALLED VIA PYTHON UDF AND THE DATA STORED TO A TABLE
-- FROM THERE THE DATA IS OFFLOADED FROM A TABLE TO A PERMANENT FILE VIA COPY INTO AND A STAGE RUN VIA AN ANONYMOUS BLOCK

use database sandbox;
use role sysadmin;

-- drop schema json_data;
create schema if not exists json_data;
use schema json_data;
                
use role accountadmin;

-- drop network rule nasa_sfauth;
create network rule if not exists nasa_sfauth
    mode = egress
    type = host_port
    value_list = ('tle.ivanstanojevic.me', 'api.nasa.gov', 'sfc-endpoint-login.snowflakecomputing.app');

create secret if not exists nasa_api_key 
    type = generic_string
    secret_string = 'REMOVED';
grant read on secret nasa_api_key to role sysadmin;

-- drop external access integration nasa_api_integration; 
create external access integration if not exists nasa_api_integration
    allowed_network_rules = (nasa_sfauth)
    allowed_authentication_secrets = (nasa_api_key)
    enabled = true;
grant usage on integration nasa_api_integration to role sysadmin;


use role sysadmin;

create or replace function get_nasa_neo_data(start_date date, end_date date)
returns string
language python
runtime_version = 3.11
handler = 'get_nasa_api_data'
external_access_integrations = (nasa_api_integration)
packages = ('snowflake-snowpark-python','requests')
secrets = ('api_key' = nasa_api_key)
AS
$$

import _snowflake
import requests
import json

session = requests.Session()

def get_nasa_api_data(start_date, end_date):
    api_key = _snowflake.get_generic_secret_string('api_key')
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    params = {'start_date': start_date,
              'end_date': end_date,
              'api_key': api_key}
    response = requests.get(url, params=params)
    return response.json()

$$;


create or replace function get_nasa_tle_data()
returns string
language python
runtime_version = 3.11
handler = 'get_nasa_api_data'
external_access_integrations = (nasa_api_integration)
packages = ('snowflake-snowpark-python','requests')
AS
$$

import _snowflake
import requests
import json

session = requests.Session()

def get_nasa_api_data():
    url = "https://tle.ivanstanojevic.me/api/tle/"
    headers = {"User-Agent": "curl/7.68.0"}
    response = requests.get(url, headers=headers)
    return response.json()

$$;


--drop table json_responses;
create table if not exists json_responses(
    api_name varchar,
    json_response_date date,
    json_response variant);

select * from json_responses;

insert into json_responses
    select
        'neo_data' as api_name,
        current_date() as json_response_date,  
        parse_json(json_data) as json_response
    from (select get_nasa_neo_data(current_date, current_date) as json_data);


insert into json_responses
    select
        'tle_data' as api_name,
        current_date() as json_response_date,  
        parse_json(json_data) as json_response
    from (select get_nasa_tle_data() as json_data);

select * from json_data.json_responses;

-- drop stage json_data.nasa_api_data;
create stage if not exists json_data.nasa_api_data
    directory = (enable = true
                 refresh_on_create = true);

-- remove@json_data.nasa_api_data/;
list@json_data.nasa_api_data;

-- drop file format json_data.create_file_from_response;
create file format if not exists json_data.create_file_from_response
    type = json
    compression = auto
    file_extension = '.json';

    
declare
    api_array array default array_construct('neo_data', 'tle_data');
    api_ string;
    copy_into_statement string;
    result_ string default 'Objects loaded: ';
begin
    for index_ in 0 to array_size(api_array) - 1 do
        api_ := api_array[index_];
        copy_into_statement := 'copy into @json_data.nasa_api_data/' || :api_ || '_' || to_varchar(current_date(), 'YYYYMMDD') || '.json.gz' || '  
                                    from (select json_response from json_responses where api_name = ''' || :api_ || ''')
                                    file_format = (format_name = ''json_data.create_file_from_response''
                                                   compression = auto)
                                    overwrite = true
                                    single = true
                                    max_file_size = 16777216;';
        execute immediate copy_into_statement;
        result_ := result_ || '\n\t--'|| :api_ || ': '|| (select to_varchar(object_construct(*)) from table(result_scan(last_query_id()))) || ',';
    end for;
    result_ := rtrim(result_, ',');
    return result_;
end;

list@json_data.nasa_api_data;