
-- day 1 assignment
-- 1. create a file format for csv
 create or replace file format ingestion.temp.test_csv_format 
 type = 'csv'
 Field_Delimiter=','
 skip_header = 1 
 FIELD_OPTIONALLY_ENCLOSED_BY='"';

 -- 3 -- create an external stage on s3 bucket
 create or replace stage ingestion.temp.test_ext_stg_csv 
url= 's3://inovalon-more2-deidentified-dataregistry-bucket-588713642076/test/patient_records.csv'
file_format = ingestion.temp.test_csv_format 
ENCRYPTION = (TYPE = 'AWS_SSE_KMS' KMS_KEY_ID = 'fd70af15-fe8e-44c2-b4e1-39ae6de6bf2d') STORAGE_INTEGRATION=INOVALON_MORE2_DEV;

 select count(*) from @ingestion.temp.test_ext_stg_csv;

 create or replace table ingestion.temp.patients(
  memberid int,
  name varchar(50),
    age varchar(4),
    medication varchar(50)
 );

copy into ingestion.temp.patients from @ingestion.temp.test_ext_stg_csv;

select * from ingestion.temp.patients;
 
 --  create a materialized view
 CREATE MATERIALIZED VIEW ingestion.temp.avgAgeByMedication AS
 SELECT medication, AVG(age) AS
 AvgAge
 FROM patients
 GROUP BY medication;

 -- Query the materialized view:
select * from ingestion.temp.avgAgeByMedication;