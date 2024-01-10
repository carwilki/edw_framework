--before we insert the parameters into the database we need 
--to remove all the other parameters that could be there 
--from previous parameter iterations
delete from dev_raw.parameter_config
where
    parameter_file_name = 'wf_store_data';

--insert the source bucket and archive bucket into
--the parameter config table

insert into
    qa_raw.parameter_config (
        parameter_file_name,
        parameter_section,
        parameter_key,
        parameter_value
    )
values
    (
        'wf_store_data',
        'm_store_data',
        'source_bucket',
        'gs://petm-bdpl-prod-prep-p1-gcs-gbl/nas/masterdata/store_data/'
    ),
    (
        'wf_store_data',
        'm_store_data',
        'archive_bucket',
        'gs://petm-bdpl-qa-raw-p1-gcs-gbl/nas/masterdata/store_data'
    );