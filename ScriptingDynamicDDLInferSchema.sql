-- BELOW IS A PROCEDURE USED TO CREATE A PATIENT-DATA SPECIFIC SCHEMA AND BUILD TABLES FROM PATIENT DATA FILES
-- THE SCHEMA_ARG IS USED TO SET THE WORKING SCHEMA


-- DROP PROCEDURE GENERAL_HOSPITAL_PATIENTS.CREATE_SCHEMA_AND_TABLES(VARCHAR);
CREATE PROCEDURE IF NOT EXISTS GENERAL_HOSPITAL_PATIENTS.CREATE_SCHEMA_AND_TABLES(SCHEMA_ARG VARCHAR)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    schema_ STRING DEFAULT SCHEMA_ARG;
    file_list RESULTSET;
    file_ STRING;  
    table_ STRING;
    table_ddl STRING;
    get_column_metadata STRING;
    column_metadata RESULTSET;
    column_name STRING;
    column_datatype STRING;
    column_ddl STRING;
    create_table_result STRING;
    
    sql_error_message STRING;
    build_tables_report STRING;

    --Inner loop exceptions:
    OTHER_FILE_ITERATION_EXCEPTION EXCEPTION (-20001, 'ERROR --Other-type failure:  Stopping current iteration. ');

    --Outer scope exceptions:
    --None except OTHER

BEGIN
    build_tables_report := 'Beginning dynamic generation of tables in schema: ' || schema_ || '\n\n';
    file_list := (SELECT FILE_NAME AS FILE_NAME
                      FROM (SELECT 
                                METADATA$FILENAME AS FILE_NAME
                            FROM @GENERAL_HOSPITAL_PATIENTS.INBOUND_HEALTHCARE_FILES
                            WHERE 
                                METADATA$FILE_ROW_NUMBER = 1
                                --AND
                                --NOT REGEXP_LIKE(METADATA$FILENAME, '^surgical_encounters_\\d{14}_UTC.csv.gz$')
                            ORDER BY FILE_NAME));
    
    FOR record in file_list DO
    BEGIN        
        file_ := record.file_name;
        table_:= UPPER(REGEXP_SUBSTR(file_, '^(.*)_\\d{14}_UTC.csv.gz', 1, 1, 'e', 1)); 

        build_tables_report := build_tables_report || '\t' || '--Beginning table creation for file: ' || file_ || '...' || '\n';

        table_ddl := 'CREATE TABLE IF NOT EXISTS ' || :schema_ || '.' || :table_ || ' (
                        SOURCE_FILE_LOADED_AT_UTC TIMESTAMP_TZ,
                        SOURCE_FILE_DATE_UTC DATE,
                        SOURCE_FILE_NAME VARCHAR,
                        SOURCE_FILE_UNIQUE_KEY VARCHAR,
                        SOURCE_FILE_ROW_NUMBER NUMBER(38,0),
                        SOURCE_FILE_RECORD_UNIQUE_KEY VARCHAR,' || '\n';
        
        get_column_metadata := 'SELECT 
                                    COLUMN_NAME, TYPE, ORDER_ID
                                FROM TABLE(INFER_SCHEMA(
                                LOCATION => ''@GENERAL_HOSPITAL_PATIENTS.INBOUND_HEALTHCARE_FILES'',
                                FILE_FORMAT => ''GENERAL_HOSPITAL_PATIENTS.INFER_SCHEMA_PATIENT_FILES'',
                                FILES => ''' || :file_ || ''',
                                IGNORE_CASE => FALSE
                                --MAX_FILE_COUNT => <num>
                                --MAX_RECORDS_PER_FILE => <num>
                                ));';
        column_metadata := (EXECUTE IMMEDIATE get_column_metadata);
        
        column_ddl := '';
        FOR record in column_metadata DO
            column_name := record.column_name;
            column_datatype := record.type;

            IF (REGEXP_LIKE(column_datatype, '^(TEXT|DATE)$')) THEN
                column_datatype := 'VARCHAR';
            ELSEIF (REGEXP_LIKE(column_datatype, '^NUMBER\\(.*$')) THEN
                column_datatype := REGEXP_REPLACE(column_datatype, 'NUMBER\\(\\d+,', 'NUMBER(38,');
            ELSE
                column_datatype := column_datatype;
            END IF;
            
            column_ddl := column_ddl || REPEAT('\t ', 8) || UPPER(column_name) || ' ' || column_datatype || ',' || '\n';
            
        END FOR;

        column_ddl := RTRIM(column_ddl, ',\n');
        table_ddl := table_ddl || column_ddl || '\n' || REPEAT('\t ', 8) || ');';
        EXECUTE IMMEDIATE table_ddl;
        create_table_result := (SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
        build_tables_report := build_tables_report || '\t\t' || '...create table result: ' || create_table_result || '\n';

        EXCEPTION 
            WHEN OTHER_FILE_ITERATION_EXCEPTION THEN
                sql_error_message := SQLERRM;
                build_tables_report := build_tables_report || '\t' || '--Other-type failure for file iteration: ' || sql_error_message || '\n'
                                                           || '\t\t' || '--Create table from file data: FAILURE.' || '\n'
                                                           || '\t\t' || '--Stopping iteration for current file and moving to next.' || '\n\n';
                INSERT INTO GENERAL_HOSPITAL_PATIENTS.ADMIN_CREATE_OBJECTS_RUN_REPORT (RUN_REPORT_RESULTS) VALUES (:build_tables_report);
                CONTINUE;
                
    END;                 
    
    END FOR;
        
    build_tables_report := build_tables_report || '\n' || '...dynamic creation of tables completed.' || '\n\n'
                                                       || 'Procedure run success: TRUE.';
    INSERT INTO GENERAL_HOSPITAL_PATIENTS.ADMIN_CREATE_OBJECTS_RUN_REPORT (RUN_REPORT_RESULTS) VALUES (:build_tables_report);

    RETURN 'SUCCESS -- CREATE_SCHEMA_AND_TABLES() procedure complete.  Check the log at GENERAL_HOSPITAL_PATIENTS.ADMIN_CREATE_OBJECTS_RUN_REPORT';

    EXCEPTION
        WHEN OTHER THEN
            sql_error_message := SQLERRM;
            build_tables_report := build_tables_report || '\t' || '--General failure of CREATE_SCHEMA_AND_TABLES() procedure: ' || sql_error_message || '\n'
                                                       || '\t\t' || '--Procedure will terminate.' || '\n'
                                                       || '\t\t' || '--Procedure run success: FALSE' || '\n\n';
            INSERT INTO GENERAL_HOSPITAL_PATIENTS.ADMIN_CREATE_OBJECTS_RUN_REPORT (RUN_REPORT_RESULTS) VALUES (:build_tables_report);
            
            RETURN 'ERROR -- CREATE_SCHEMA_AND_TABLES() procedure did not complete.  Check the log at GENERAL_HOSPITAL_PATIENTS.ADMIN_CREATE_OBJECTS_RUN_REPORT';

END;

$$;

CALL GENERAL_HOSPITAL_PATIENTS.CREATE_SCHEMA_AND_TABLES('GENERAL_HOSPITAL_PATIENTS');