select * from json_ingestion_dev.b_raw_json_data.raw_neo_data src; 


select 
    data.*
    -- regexp_substr(data.path, 
    --               '^near_earth_objects\\[''2026-06-02''\\]\\[([0-9]+)\\]', 1, 1, 'e', 1)::int as data_object,
    -- row_number() over (partition by data_object order by data.path) as data_object_seq,
    -- data.path as data_path,    
    -- data.key as data_key,
    -- data.value as data_value
from json_ingestion_dev.b_raw_json_data.raw_neo_data src,
lateral flatten(input => src.source_file_object, recursive => true) data
order by path;
-- where typeof(data_value) not in ('OBJECT', 'ARRAY')
-- order by data_object nulls first, data_object_seq;





select metadata$filename as file_name,
       metadata$file_row_number as file_row,
       $1, $2, $3, $4, $5
from @csv_ingestion_dev.landing.cleaned_files/customers_     -- target one file by path
    (file_format => 'csv_ingestion_dev.landing.consume_csv')
order by file_name, file_row;


select * from table(infer_schema(
    location => '@csv_ingestion_dev.landing.cleaned_files/customers_',  
    file_format => 'csv_ingestion_dev.landing.infer_schema'))
order by order_id;


declare

    db varchar default 'SANDBOX';
    schma varchar default 'TEST_SCRIPTS';
    tbl varchar default 'CUSTOMERS';
    
    query_rs resultset;
    column_name varchar;
    column_datatype varchar;
    column_ordinal_position number;

    table_column_ddl varchar default '';
    merge_column_dml varchar default '';
    select_source_dml varchar default '';
    merge_into_source_column_dml varchar default '';
    merge_into_target_column_dml varchar default '';
        
    create_table_statement varchar;
    merge_into_statement varchar;

    error_message array;

begin

    -- select 12 / 0;
    
    use database identifier(:db);
    create schema if not exists identifier(:schma);
    use schema identifier(:schma);
    
    select * from table(infer_schema(
        location => '@csv_ingestion_dev.landing.cleaned_files/customers_',  
        file_format => 'csv_ingestion_dev.landing.infer_schema'))
    order by order_id;

    query_rs := (select * from table(result_scan(last_query_id())));
    -- return table(query_rs);

    for record in query_rs do
        column_name := '"' || record.column_name || '"';
        column_datatype := record.type;
        column_ordinal_position := (record.order_id + 1);

        if (regexp_like(record.type, 'NUMBER\\(\\d{1,2},\\d{1,2}\\)')) then
            column_datatype := regexp_replace(record.type, 'NUMBER\\(\\d{1,2}, (\\d{1,2})\\)$', 'NUMBER(38, \\1)');
        elseif (record.type = 'TEXT') then column_datatype := 'VARCHAR';
        elseif (record.type = 'DATE') then null;
        elseif (record.type = 'BOOLEAN') then null;
        else null;
        end if;

        table_column_ddl := :table_column_ddl || :column_name || ' ' || :column_datatype || ',' || '\n';
        select_source_dml := :select_source_dml || '$' || :column_ordinal_position || ',' || '\n';
        merge_into_source_column_dml := :merge_into_source_column_dml || 's.$' || :column_ordinal_position || '::' || :column_datatype || ',' || '\n';
        merge_into_target_column_dml := :merge_into_target_column_dml || 't.' || :column_name || ',' || '\n';
        
    
    end for;

    table_column_ddl := rtrim(:table_column_ddl, ',\n');
    merge_into_target_column_dml := rtrim(:merge_into_target_column_dml, ',\n');
    merge_into_source_column_dml := rtrim(:merge_into_source_column_dml, ',\n');

    create_table_statement := 'create table if not exists ' || :db || '.' || :schma || '.' || :tbl || ' (
                                    loaded_at timestamp_tz,
                                    file_name varchar,
                                    file_row_number integer,
                                    file_date date,
                                    file_record_unique_key varchar, ' || '\n'
                                    || :table_column_ddl || ')
                                enable_schema_evolution = true';
    -- return create_table_statement;
    execute immediate create_table_statement;
    -- return (select object_construct(*) from table(result_scan(last_query_id())));

    merge_into_statement := 'merge into ' || :schma || '.' || :tbl || ' t
                             using (select 
                                       current_timestamp() as loaded_at,
                                       metadata$filename as file_name,
                                       metadata$file_row_number as file_row_number,
                                       to_date(regexp_substr(metadata$filename, ''customers_(\\\\d{8})[.]csv[.]gz$'', 1, 1, ''e'', 1), ''YYYYMMDD'') 
                                            as file_date,
                                       md5(file_name || file_row_number) as file_record_unique_key, ' || '\n'
                                       || :select_source_dml || '
                                    from @csv_ingestion_dev.landing.cleaned_files/customers_   
                                        (file_format => ''csv_ingestion_dev.landing.consume_csv'')
                                    order by file_name, file_row_number ) s                             
                             on (s.file_record_unique_key = t.file_record_unique_key)
                             when not matched then
                                insert(t.loaded_at,
                                       t.file_name,
                                       t.file_row_number,
                                       t.file_date,
                                       t.file_record_unique_key,' || '\n'
                                       || :merge_into_target_column_dml || ')
                                values(s.loaded_at,
                                       s.file_name,
                                       s.file_row_number,
                                       s.file_date,
                                       s.file_record_unique_key,' || '\n'
                                       || :merge_into_source_column_dml || ')';
    -- return merge_into_statement;
    execute immediate merge_into_statement;
    return (select object_construct(*) from table(result_scan(last_query_id())));


exception 
    when other then
        error_message := array_construct('error_type: <procedure name> other type exception',
                                         'at_utc: ' || convert_timezone('UTC', current_timestamp())::varchar,
                                         'sql_code: ' || sqlcode,
                                         'sql_state: ' || sqlstate,
                                         'sql_errm: ' || sqlerrm,
                                         'logging_table: <logging location>');
        return error_message;
end;


                                    
select * from sandbox.test_scripts.customers
order by file_name, file_row_number;

drop table sandbox.test_scripts.customers;


[
  "error_type: <procedure name> other type exception",
  "at_utc: 2026-11-03 05:47:56.427 Z",
  "sql_code: 2043",
  "sql_state: 02000",
  "sql_errm: SQL compilation error:\nObject does not exist, or operation cannot be performed.",
  "logging_table: <logging location>"
]


merge into TEST_SCRIPTS.CUSTOMERS t
                             using (select 
                                       current_timestamp() as loaded_at,
                                       metadata$filename as file_name,
                                       metadata$file_row_number as file_row_number,
                                       to_date(regexp_substr(metadata$filename, 'customers_(\\d{8})[.]csv[.]gz$', 1, 1, 'e', 1), 'YYYYMMDD') 
                                            as file_date,
                                       md5(file_name || file_row_number) as file_record_unique_key, 
$1,
$2,
$3,
$4,
$5,
$6,
$7,
$8,

                                    from @csv_ingestion_dev.landing.cleaned_files/customers_   
                                        (file_format => 'csv_ingestion_dev.landing.consume_csv')
                                    order by file_name, file_row_number ) s                             
                             on (s.file_record_unique_key = t.file_record_unique_key)
                             when not matched then
                                insert(t.loaded_at,
                                       t.file_name,
                                       t.file_row_number,
                                       t.file_date,
                                       t.file_record_unique_key,
t."C_CUSTKEY",
t."C_NAME",
t."C_ADDRESS",
t."C_NATIONKEY",
t."C_PHONE",
t."C_ACCTBAL",
t."C_MKTSEGMENT",
t."C_COMMENT")
                                values(s.loaded_at,
                                       s.file_name,
                                       s.file_row_number,
                                       s.file_date,
                                       s.file_record_unique_key,
s.$1::NUMBER(6, 0),
s.$2::VARCHAR,
s.$3::VARCHAR,
s.$4::NUMBER(2, 0),
s.$5::VARCHAR,
s.$6::NUMBER(6, 2),
s.$7::VARCHAR,
s.$8::VARCHAR)