
  
  create view "my_db"."main"."stg_events__dbt_tmp" as (
    select *
from "raw"."piwik"."streaming"
  );
