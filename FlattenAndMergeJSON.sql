-- THE BELOW IS A BRIEF EXAMPLE OF FLATTENING JSON OBJECTS STORED AS A SINGLE COLUMN VARIANT TO A TEMP TABLE
-- FOLLOWING THAT THE RESULTS ARE MERGED TO A RAW TABLE

create or replace temporary table json_data.temp_load_neo_data as
    with flattened_data as (
        select 
            ld.*,
            ldf.value as data_objects
        from landing_neo_data as ld,
        lateral flatten(input => source_file_object:near_earth_objects) ldf
        ),
    flattened_objects as (
        select 
            fd.source_file_loaded_at_utc,
            fd.source_file_date_utc,
            fd.source_file_name,
            fd.source_file_object_unique_key,
            fd.source_file_object,
            row_number() over(order by fdo.value:id) as source_file_row_number,
            fdo.value:links.self::string as link_self,
            fdo.value:id::string as id,
            fdo.value:neo_reference_id::string as neo_reference_id,
            fdo.value:name::string as name,
            fdo.value:absolute_magnitude_h::float as absolute_magnitude_h,
            fdo.value:is_potentially_hazardous_asteroid::boolean as is_hazardous,
            fdo.value:close_approach_data[0].close_approach_date::string as close_approach_date,
            fdo.value:close_approach_data[0].relative_velocity.kilometers_per_second::float as velocity_kps,
            fdo.value:close_approach_data[0].miss_distance.kilometers::float as miss_distance_km,
            fdo.value:close_approach_data[0].orbiting_body::string as orbiting_body
        from flattened_data as fd,
        lateral flatten(input => data_objects) as fdo
        )
    select * from flattened_objects;

select * from json_data.temp_load_neo_data;

merge into json_data.raw_neo_data t
    using 
        (select * from json_data.temp_load_neo_data) s
    on 
        s.source_file_object_unique_key = t.source_file_object_unique_key
    when not matched then 
        insert (
            t.source_file_loaded_at_utc,
            t.source_file_date_utc,
            t.source_file_name,
            t.source_file_object_unique_key,
            t.source_file_object,
            t.source_file_row_number,
            t.link_self,
            t.id,
            t.neo_reference_id,
            t.name,
            t.absolute_magnitude_h,
            t.is_hazardous,
            t.close_approach_date,
            t.velocity_kps,
            t.miss_distance_km,
            t.orbiting_body
            )
        values(
            s.source_file_loaded_at_utc,
            s.source_file_date_utc,
            s.source_file_name,
            s.source_file_object_unique_key,
            s.source_file_object,
            s.source_file_row_number,
            s.link_self,
            s.id,
            s.neo_reference_id,
            s.name,
            s.absolute_magnitude_h,
            s.is_hazardous,
            s.close_approach_date,
            s.velocity_kps,
            s.miss_distance_km,
            s.orbiting_body
            );

select * from json_data.raw_neo_data
order by  source_file_date_utc, source_file_row_number;