-- BELOW IS A PYTHON UDF FOR CLEANING CSV FILES
    -- IN SOME CASES THIS CAN BE REPLACED WITH "REPLACE INVALID CHARACTERS" IN THE COPY INTO FILE FORMAT
    -- THEN THE CHARACTER '�' CAN BE REPLACED AT THE MERGE STEP


create procedure if not exists utilities.prep_csv_files()
    returns varchar
    language python
    runtime_version = '3.12'
    packages = ('snowflake-snowpark-python', 'pandas')
    handler = 'main'
    execute as caller
as 
$$

import snowflake.snowpark as snowpark
import gzip
import io
import pandas as pd
import json

def transform_files(session):

    source_stage = '@utilities.raw_files'
    target_stage = '@utilities.prepped_files'

    session.sql(f'alter stage {source_stage.replace("@", "")} refresh').collect()
    file_list = session.sql(f'select relative_path from directory({source_stage}) order by relative_path').collect()

    if not file_list:
        output_summary = json.dumps({"No files processed": "No files found in source stage"})
        return output_summary
    else:
        processed_files = []
        skipped_files = []
        for row in file_list:
            file_name = row['RELATIVE_PATH']
            source_file_path = f'{source_stage}/{file_name}'            
            try:
                file_stream = session.file.get_stream(source_file_path)
                with gzip.GzipFile(fileobj=io.BytesIO(file_stream.read()), mode='rb') as file_content:
                    df = pd.read_csv(file_content, sep=',', dtype=str)

            except Exception as e:
                skipped_files.append({"skipped file": source_file_path, "reason": str(e)})
                continue

            df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
            df = df.replace(['', 'NaN', 'nan', 'None', 'null'], None)
            df = df.mask(df.isna(), None)

            gz_buffer = io.BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode='wb') as gz:
                df.to_csv(gz, index=False, encoding='utf-8', errors='replace')
            gz_buffer.seek(0)

            target_file_path = f'{target_stage}/{file_name}'            
            session.file.put_stream(input_stream=gz_buffer,
                                    stage_location=target_file_path,
                                    auto_compress=False,
                                    overwrite=False)
    
            session.sql(f'remove {source_file_path}').collect()
            processed_files.append({"source file": source_file_path, 
                                    "target file": target_file_path})
    
        output_summary = json.dumps({"processed_files": processed_files, 
                                     "skipped_files": skipped_files
                                     },
                                     indent=2, 
                                     sort_keys=True)
                                     
        session.sql(f'alter stage {target_stage.replace("@", "")} refresh').collect()
        
        return output_summary
        
def main(session):
    output_summary = transform_files(session)
    return f'File processing complete.  Output summary: {output_summary}'

$$;