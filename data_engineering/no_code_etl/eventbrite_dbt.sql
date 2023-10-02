{{
    config
    (
        materialized='incremental',
        unique_key = ['id'],
        on_schema_change = 'sync_all_columns'
    )

}} 

with salesforce as (select distinct salesforce_lead_id,
	   email,
	   created_date, 
	   converted_date,
	   closed_date,
	   channel,
    from {{ref('salesforce')}})

-- getting eventbrite data from google sheets, need to apply qualify to avoid any duplicated lead getting more than one registration for a single event
-- they could be getting a ticket for another friend, but that edge will be removed for the sake of scalability 
,eventbrite as (select ticket_date, 
      event_date,
	   SHA2(email) as email,
	   organizer, 
	   event_id, 
	   event_name, 
	   city,
	   country, 
	   affiliate, 
	   quantity as event_registrations
    from  {{ source('raw_gsheets','eventbrite')}}
    Qualify ROW_NUMBER() over (partition by ticket_date,
                                        email,
                                        event_id,
                                        organizer,
                                        event_name, 
                                        city,
                                        country, 
                                        affiliate
                                        order by ticket_date desc) = 1)

select eventbrite.email,
       salesforce_lead_id, 
	   ticket_date, 
	   event_date, 
	   created_date, 
	   converted_date, 
       closed_date,
       organizer,
	   event_id,
	   event_name,
	   country,
	   city,
	   affiliate,
       channel, 
	   event_registrations,
       -- checking for email presence inside SF to confirm if there is a lead inside the CRM
	   COALESCE(CASE WHEN (case when salesforce.email is null then false else true end)::boolean THEN 1 ELSE 0 END,0) as is_in_salesforce,
       -- generating an unique id for potential incremental models 
       {{ dbt_utils.generate_surrogate_key(['eventbrite.email', 'ticket_date','event_id']) }} as id,
	   current_timestamp as date_inserted
from eventbrite 
left join salesforce 
	on eventbrite.email = salesforce.email 
-- building a one-day batch incremental model
{% if is_incremental() %}
  and ticket_date > (select max(ticket_date) from {{ this }})
{% endif %}