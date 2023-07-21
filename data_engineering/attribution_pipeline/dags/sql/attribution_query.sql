select prs.visitor_id,
            prs.session_id, 
            pre.event_id,
            pre.timestamp,
            pre.event_url,
            pre.custom_event_category,
            pre.custom_event_action,
            pre.custom_event_name,
            prs.source_medium,
            prs.campaign_name,
            pre.event_index,
            pre.page_view_index,
            prs.visitor_session_number as touchpoint,
            pre.date_inserted 
        from piwik_pro_sessions_airflow prs 
        left join piwik_pro_events_airflow  pre 
            on prs.visitor_id  = pre.visitor_id