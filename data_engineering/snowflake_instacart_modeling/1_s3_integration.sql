-- Authentication 

CREATE STAGE my_stage 
URL = ${S3_URL} 
CREDENTIALS = (AWS_KEY_ID = ${AWS_KEY_ID} AWS_SECRET_KEY = ${AWS_SECRET_KEY});

-- Format creation 

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';
