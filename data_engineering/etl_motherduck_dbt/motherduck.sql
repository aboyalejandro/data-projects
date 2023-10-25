CREATE OR REPLACE TABLE PIWIK.SESSIONS (
                  session_id bigint,
                  visitor_id STRING,
                  timestamp TIMESTAMP,
                  visitor_session_number INT,
                  visitor_returning STRING,
                  source_medium STRING,
                  campaign_name STRING,
                  session_total_page_views INT,
                  session_total_events INT,
                  visitor_days_since_last_session INT,
                  visitor_days_since_first_session INT,
                  location_country_name STRING,
                  date_inserted TIMESTAMP,
                  PRIMARY KEY(session_id)
);

CREATE OR REPLACE TABLE PIWIK.ANALYTICS (
                timestamp__to_date DATE,
                referrer_type STRING,
                source_medium STRING,
                campaign_name STRING,
                campaign_content STRING,
                session_entry_url STRING,
                visitor_returning STRING,
                location_country_name STRING,
                operating_system STRING,
                device_type STRING,
                events INT,
                visitors INT,
                sessions INT,
                page_views INT,
                ecommerce_conversions INT,
                cart_additions INT,
                ecommerce_abandoned_carts INT,
                consents_none INT,
                consents_full INT,
                date_inserted TIMESTAMP
);

CREATE OR REPLACE TABLE PIWIK.STREAMING (
                  session_id bigint,
                  event_id bigint,
                  visitor_id STRING,
                  timestamp TIMESTAMP,
                  event_index INT,
                  page_view_index INT,
                  custom_event_category STRING,
                  custom_event_action STRING,
                  custom_event_name STRING,
                  event_url STRING,
                  source_medium STRING,
                  campaign_name STRING,
                  goal_id INT,
                  date_inserted TIMESTAMP,
                  PRIMARY KEY(event_id)
);

---

INSERT INTO PIWIK.ANALYTICS
SELECT *
FROM read_csv_auto('s3://piwik-pro-analytics/analytics/*');

INSERT INTO PIWIK.SESSIONS
SELECT replace(replace(session_id, 'e+', ''),'.','')::bigint as session_id,
       * EXCLUDE(session_id)
FROM read_csv_auto('s3://piwik-pro-analytics/sessions/*');

INSERT INTO PIWIK.STREAMING
select  replace(replace(session_id, 'e+', ''),'.','')::bigint as session_id,
        replace(replace(event_id, 'e+', ''),'.','')::bigint as event_id,
        * EXCLUDE (session_id,event_id)
FROM read_csv_auto('s3://piwik-pro-analytics/events/*')