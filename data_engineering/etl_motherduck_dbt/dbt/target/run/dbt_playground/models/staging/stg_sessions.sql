
  
  create view "my_db"."main"."stg_sessions__dbt_tmp" as (
    select *
from "raw"."piwik"."sessions"
  );
