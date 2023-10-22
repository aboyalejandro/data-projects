CREATE DATABASE RAW_SNOWFLAKE;

DROP SCHEMA external_stages;
DROP SCHEMA piwik_pro;

CREATE OR REPLACE SCHEMA external_stages;

CREATE OR REPLACE SCHEMA piwik_pro;

USE SCHEMA piwik_pro;

-- CREATING FILE FORMAT 

CREATE OR REPLACE FILE FORMAT PARQUET
TYPE = PARQUET 
COMPRESSION = AUTO
COMMENT = 'default s3 file format for parquet files';

CREATE OR REPLACE FILE FORMAT CSV
TYPE = CSV,
FIELD_DELIMITER = ","
SKIP_HEADER = 1;

USE SCHEMA external_stages;

-- CREATE STORAGE INTEGRATION

CREATE OR REPLACE STORAGE INTEGRATION s3_connection
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '_'
  STORAGE_ALLOWED_LOCATIONS = ('s3://piwik-pro-analytics/');

DESC STORAGE INTEGRATION s3_connection;

-- CREATING STAGES

CREATE OR REPLACE STAGE piwik_sessions 
URL = 's3://piwik-pro-analytics/sessions/'
CREDENTIALS = (AWS_KEY_ID = '_' AWS_SECRET_KEY = '_')
FILE_FORMAT = RAW_SNOWFLAKE.piwik_pro.csv;

LIST @piwik_sessions;
DESC STAGE piwik_sessions;


CREATE OR REPLACE STAGE piwik_events
URL = 's3://piwik-pro-analytics/events/'
CREDENTIALS = (AWS_KEY_ID = '_' AWS_SECRET_KEY = '_')
FILE_FORMAT = RAW_SNOWFLAKE.piwik_pro.csv;

LIST @piwik_events;
DESC STAGE piwik_events;

CREATE OR REPLACE STAGE piwik_analytics
URL = 's3://piwik-pro-analytics/analytics/'
CREDENTIALS = (AWS_KEY_ID = '_' AWS_SECRET_KEY = '_')
FILE_FORMAT = RAW_SNOWFLAKE.piwik_pro.csv;

LIST @piwik_analytics;
DESC STAGE piwik_analytics;

-- CREATING TABLES

USE SCHEMA piwik_pro;

CREATE OR REPLACE TABLE SESSIONS (
                  session_id STRING,
                  visitor_id STRING,
                  timestamp STRING,
                  visitor_session_number STRING,
                  visitor_returning STRING,
                  source_medium STRING,
                  campaign_name STRING,
                  session_total_page_views STRING,
                  session_total_events STRING,
                  visitor_days_since_last_session STRING,
                  visitor_days_since_first_session STRING,
                  location_country_name STRING,
                  date_inserted STRING
);

CREATE OR REPLACE TABLE ANALYTICS (
                timestamp__to_date STRING,
                referrer_type STRING,
                source_medium STRING,
                campaign_name STRING,
                campaign_content STRING,
                session_entry_url STRING,
                visitor_returning STRING,
                location_country_name STRING,
                operating_system STRING,
                device_type STRING,
                events STRING,
                visitors STRING,
                sessions STRING,
                page_views STRING,
                ecommerce_conversions STRING,
                cart_additions STRING,
                ecommerce_abandoned_carts STRING,
                consents_none STRING,
                consents_full STRING,
                date_inserted STRING
);

CREATE OR REPLACE TABLE EVENTS (
                  session_id STRING,
                  event_id STRING,
                  visitor_id STRING,
                  timestamp STRING,
                  event_index STRING,
                  page_view_index STRING,
                  custom_event_category STRING,
                  custom_event_action STRING,
                  custom_event_name STRING,
                  event_url STRING,
                  source_medium STRING,
                  campaign_name STRING,
                  goal_id STRING,
                  date_inserted STRING
);

-- COPY STAGES FILES

USE SCHEMA piwik_pro;

COPY INTO RAW_SNOWFLAKE.piwik_pro.sessions
FROM @external_stages.piwik_sessions
FILE_FORMAT = (FORMAT_NAME = RAW_SNOWFLAKE.piwik_pro.CSV);

COPY INTO RAW_SNOWFLAKE.piwik_pro.events
FROM @external_stages.piwik_events
FILE_FORMAT = (FORMAT_NAME = RAW_SNOWFLAKE.piwik_pro.CSV);

COPY INTO RAW_SNOWFLAKE.piwik_pro.analytics
FROM @external_stages.piwik_analytics
FILE_FORMAT = (FORMAT_NAME = RAW_SNOWFLAKE.piwik_pro.CSV);

SELECT * from RAW_SNOWFLAKE.piwik_pro.sessions;
SELECT * from RAW_SNOWFLAKE.piwik_pro.events;
SELECT * from RAW_SNOWFLAKE.piwik_pro.analytics;

-- CREATING PIPES

CREATE OR REPLACE TABLE streaming (
                  session_id STRING,
                  event_id STRING,
                  visitor_id STRING,
                  timestamp STRING,
                  event_index STRING,
                  page_view_index STRING,
                  custom_event_category STRING,
                  custom_event_action STRING,
                  custom_event_name STRING,
                  event_url STRING,
                  source_medium STRING,
                  campaign_name STRING,
                  goal_id STRING,
                  date_inserted STRING
);

CREATE OR REPLACE pipe RAW_SNOWFLAKE.piwik_pro.streaming
auto_ingest = TRUE
AS
COPY INTO RAW_SNOWFLAKE.piwik_pro.streaming
FROM @external_stages.piwik_events ;

DESC pipe streaming;

SELECT * FROM RAW_SNOWFLAKE.piwik_pro.streaming;