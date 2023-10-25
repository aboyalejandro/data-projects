
  
  create view "my_db"."main"."stg_analytics__dbt_tmp" as (
    select *
from "raw"."piwik"."analytics"
  );
