
--Create campaign_member table
        DROP TABLE IF EXISTS prod_db.work.campaign_member;
        CREATE TEMPORARY TABLE prod_db.work.campaign_member AS (  
                SELECT DISTINCT cm.type
                        ,cm.FCRM_FCR_Response_Status_c
                        ,cm.lead_or_contact_id
                        ,CASE WHEN cm.type = 'Lead' THEN cm.lead_id WHEN cm.type = 'Contact' AND cm.lead_id IS NOT NULL THEN cm.lead_id WHEN cm.type = 'Contact' AND cm.lead_id IS NULL THEN llu.id END AS lead_id
                        ,cm.contact_id
                        ,CASE WHEN cm.type = 'Lead' THEN l.converted_contact_id WHEN cm.type = 'Contact' THEN llu.converted_contact_id END AS converted_contact_id
                        ,COALESCE(c.account_id,llu.converted_account_id) AS account_id 
                        ,ucm.name AS lead_or_contact_owner
                        ,CASE WHEN cm.type = 'Lead' THEN l.assigned_c WHEN cm.type = 'Contact' THEN llu.assigned_c END AS lead_assigned
                        ,CASE WHEN cm.type = 'Lead' THEN l.is_unread_by_owner WHEN cm.type = 'Contact' THEN llu.is_unread_by_owner END AS is_unread_by_owner   
                        ,CASE WHEN cm.type = 'Lead' THEN l.status WHEN cm.type = 'Contact' THEN llu.status END AS lead_status
                        ,c.fcrm_fcr_status_c AS contact_status
                        ,l.disqualification_reason_c AS disqual_reason
                        ,CASE WHEN cm.type = 'Lead' THEN l.is_converted WHEN cm.type = 'Contact' THEN llu.is_converted END AS is_converted       
                        ,CASE WHEN cm.type = 'Lead' THEN l.converted_opportunity_id WHEN cm.type = 'Contact' THEN llu.converted_opportunity_id END AS converted_opp_id                
                        ,CASE WHEN cm.type = 'Lead' THEN l.created_date WHEN cm.type = 'Contact' THEN llu.created_date END AS lead_created_date  
                        ,c.created_date AS contact_created_date
                        ,CASE WHEN cm.type = 'Lead' THEN l.assigned_date_time_c WHEN cm.type = 'Contact' THEN llu.assigned_date_time_c END AS lead_assigned_date_time
                        ,CASE WHEN cm.type = 'Lead' THEN l.converted_date WHEN cm.type = 'Contact' THEN llu.converted_date END AS converted_date      
                        ,CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj                              
                        ,cm.created_date AS cm_created_date
                        ,cm.FCRM_FCR_Response_Date_c
                        ,cm.FCRM_FCR_QR_date_c
                        ,cm.FCRM_FCR_SAR_date_c
                        ,cm.FCRM_FCR_SQR_date_c   
                        ,cmp.name AS campaign_name
                        ,cmp.type AS campaign_type
                        ,cmp.id AS campaign_ID
                        ,cmp.subtype_c AS campaign_subtype
                        ,cm.FCRM_FCR_Inquiry_Target_c
                        ,cm.FCRM_FCR_QR_c
                        ,cm.FCRM_FCR_SAR_c
                        ,cm.FCRM_FCR_SQR_c
                        ,cm.FCRM_FCR_SQR_Won_c
                        ,cm.fcrm_fcr_opportunity_c
--                        ,cm.FCRM_FCR_first_queue_assigned_c
--                        ,cm.FCRM_FCR_first_owner_assigned_c
--                        ,cm.FCRM_FCR_opportunity_created_by_c
--                        ,cm.FCRM_FCR_first_owner_worked_c
--                        ,cm.FCRM_FCR_sar_owner_c
                FROM fivetran_db.salesforce.campaign_member cm
                LEFT JOIN fivetran_db.salesforce.lead l ON cm.lead_id = l.id
                LEFT JOIN fivetran_db.salesforce.contact c ON cm.contact_id = c.id
                LEFT JOIN fivetran_db.salesforce.lead llu ON c.id = llu.converted_contact_id
                LEFT JOIN fivetran_db.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN fivetran_db.salesforce.account a ON l.converted_account_id = a.id
                LEFT JOIN fivetran_db.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id  
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND ucm.name <> 'Stephen Ernest'
                        AND response_date_adj >= '2016-11-01'
        );
        
        SELECT * FROM prod_db.work.campaign_member;
        

--Create First Sales Rep Activity on LEAD
        DROP TABLE IF EXISTS prod_db.work.first_SR_activity_lead;
        CREATE TEMPORARY TABLE prod_db.work.first_SR_activity_lead AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.lead_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM prod_db.work.salesforce_completed_activity a
                        LEFT JOIN prod_db.work.campaign_member cm ON a.lead_id = cm.lead_id
                        LEFT JOIN fivetran_db.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (((a.type IN ('Call','Webinar','Phone','Inmail','Closing Call') OR a.type IS NULL) OR a.sub_type LIKE '%Presentation%') OR ((email_direction = 'OB' AND outreach_flag = 0) OR (email_direction = 'IB' AND outreach_flag = 1)))
                ) WHERE rn = 1
        );
        
        SELECT * FROM prod_db.work.first_SR_activity_lead;


--Create First Sales Rep Activity on CONTACT
        DROP TABLE IF EXISTS prod_db.work.first_SR_activity_contact;
        CREATE TEMPORARY TABLE prod_db.work.first_SR_activity_contact AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.contact_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM prod_db.work.salesforce_completed_activity a
                        LEFT JOIN prod_db.work.campaign_member cm ON a.contact_id = cm.contact_id
                        LEFT JOIN fivetran_db.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (((a.type IN ('Call','Webinar','Phone','Inmail','Closing Call') OR a.type IS NULL) OR a.sub_type LIKE '%Presentation%') OR ((email_direction = 'OB' AND outreach_flag = 0) OR (email_direction = 'IB' AND outreach_flag = 1)))
                ) WHERE rn = 1
        );
        
        SELECT * FROM prod_db.work.first_SR_activity_contact;
        
--        SELECT * FROM prod_db.work.salesforce_completed_activity
--        where activity_id = '00T1a00000jWQymEAG'

--        SELECT COUNT(*)
--        SELECT *
--        FROM fivetran_db.salesforce.task
----        WHERE is_closed = 'True'
----                AND completed_date_time_c IS NULL
--        where id = '00T1a00000jWQymEAG'
--            --who last modified these task? is it support?
--            
--            select sum(case when subject like '%Email: Case%' or subject like '%[ ref:%'  then 1 else 0 end) as count_case
--                ,count(distinct subject)

--            select sum(case when subject like '%Email: Case%' or subject like '%[ ref:%'  then 1 else 0 end) as count_case
--                ,count(*)
--                select *
--             FROM fivetran_db.salesforce.task
--            where 1=1
--                and completed_date_time_c is null 
--                and is_closed = 'True'--50851
--                and created_date > '2016-03-31'
--                group by task_subtype

          
--Create First Outreach Activity on LEAD
        DROP TABLE IF EXISTS prod_db.work.first_OR_activity_lead;
        CREATE TEMPORARY TABLE prod_db.work.first_OR_activity_lead AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.lead_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM prod_db.work.salesforce_completed_activity a
                        LEFT JOIN prod_db.work.campaign_member cm ON a.lead_id = cm.lead_id
                        LEFT JOIN fivetran_db.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND a.outreach_flag = 1
                                AND (email_direction = 'OB' AND outreach_flag = 1)
                ) WHERE rn = 1
        );
        
        SELECT * FROM prod_db.work.first_OR_activity_lead;


--Create First Outreach Activity on CONTACT
        DROP TABLE IF EXISTS prod_db.work.first_OR_activity_contact;
        CREATE TEMPORARY TABLE prod_db.work.first_OR_activity_contact AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.contact_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM prod_db.work.salesforce_completed_activity a
                        LEFT JOIN prod_db.work.campaign_member cm ON a.contact_id = cm.contact_id
                        LEFT JOIN fivetran_db.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (email_direction = 'OB' AND outreach_flag = 1)
                ) WHERE rn = 1
        );
        
        SELECT * FROM prod_db.work.first_OR_activity_contact;  


--Create Opportunities
        DROP TABLE IF EXISTS prod_db.work.opps;
        CREATE TEMPORARY TABLE prod_db.work.opps AS (  
              
                SELECT * FROM 
                (SELECT DISTINCT o.account_id, o.id opp_id, o.stage_name, o.type, o.close_date, o.forecast_amount_c, o.created_date, csr.completed_date_ts, csr.response_date_adj
                        ,row_number() OVER (PARTITION BY o.account_id ORDER BY o.created_date ASC) rn
                FROM fivetran_db.salesforce.opportunity o
                LEFT JOIN prod_db.work.first_SR_activity_contact csr ON o.account_id = csr.account_id
                WHERE 1=1
                        AND o.name NOT LIKE '%Orphan%'
                        AND o.name NOT LIKE '%Forecast%'
                        AND o.is_deleted = 'False'
                        AND ((o.created_date BETWEEN csr.completed_date_ts AND DATEADD(month,6,csr.completed_date_ts)) OR (o.created_date BETWEEN DATEADD(day,-1,csr.completed_date_ts) AND csr.completed_date_ts)) --add time condition to be an opp created up to 24 hours before SR activity or any opp created within 6 months post activity
                        )
                        --AND forecast_amount_c > 0)
                WHERE rn = 1 --139
                
                UNION 
                
                SELECT * FROM 
                (SELECT DISTINCT o.account_id, o.id opp_id, o.stage_name, o.type, o.close_date, o.forecast_amount_c, o.created_date, csr.completed_date_ts, csr.response_date_adj
                        ,row_number() OVER (PARTITION BY o.account_id ORDER BY o.created_date ASC) rn
                FROM fivetran_db.salesforce.opportunity o
                LEFT JOIN prod_db.work.campaign_member cm ON o.account_id = cm.account_id
                LEFT JOIN prod_db.work.first_SR_activity_contact csr ON o.account_id = csr.account_id
                WHERE 1=1
                        AND o.name NOT LIKE '%Orphan%'
                        AND o.name NOT LIKE '%Forecast%'
                        AND o.is_deleted = 'False'
                        AND o.created_date > cm.response_date_adj
                        )
                        --AND forecast_amount_c > 0)
                WHERE rn = 1 --164
                
       );
        
        SELECT * FROM prod_db.work.opps;


--Create summary
        ;WITH cte_summary AS (
         SELECT cm.*
                ,CASE WHEN cm.type = 'Lead' THEN lor.completed_date_ts WHEN cm.type = 'Contact' THEN cor.completed_date_ts END AS First_OR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lor.subject WHEN cm.type = 'Contact' THEN cor.subject END AS First_OR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lor.activity_id WHEN cm.type = 'Contact' THEN cor.activity_id END AS First_OR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.completed_date_ts WHEN cm.type = 'Contact' THEN csr.completed_date_ts END AS First_SR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lsr.subject WHEN cm.type = 'Contact' THEN csr.subject END AS First_SR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lsr.activity_id WHEN cm.type = 'Contact' THEN csr.activity_id END AS First_SR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.sales_rep WHEN cm.type = 'Contact' THEN csr.sales_rep END AS First_SR_Activity_Name
                ,op.created_date AS First_Opp_Created_Date
                ,op.close_date AS First_Opp_Close_Date
                ,op.opp_id AS First_Opp_ID
                ,op.stage_name AS First_Opp_Stage
                ,op.type AS First_Opp_Type
                ,op.forecast_amount_c AS First_Opp_Booking_Amt
        FROM prod_db.work.campaign_member cm         
        LEFT JOIN prod_db.work.first_SR_activity_lead lsr ON cm.lead_id = lsr.lead_id AND cm.campaign_id = lsr.campaign_id
        LEFT JOIN prod_db.work.first_SR_activity_contact csr ON cm.contact_id = csr.contact_id AND cm.campaign_id = csr.campaign_id
        LEFT JOIN prod_db.work.first_OR_activity_lead lor ON cm.lead_id = lor.lead_id AND cm.campaign_id = lor.campaign_id
        LEFT JOIN prod_db.work.first_OR_activity_contact cor ON cm.contact_id = cor.contact_id AND cm.campaign_id = cor.campaign_id                                                            
        LEFT JOIN prod_db.work.opps op ON cm.account_id = op.account_id
        )
        
        --SELECT * FROM cte_summary s
        
        SELECT * FROM (
        SELECT s.*
                ,datediff(day,lead_created_date,lead_assigned_date_time) AS lead_assignment_d
                ,datediff(min,lead_created_date,contact_created_date) AS lead_conversion_m
                ,datediff(hour,lead_created_date,contact_created_date) AS lead_conversion_h
                ,datediff(day,lead_created_date,contact_created_date) AS lead_conversion_d
                ,datediff(min,response_date_adj,first_OR_activity_date) AS action_to_OR_m
                ,datediff(hour,response_date_adj,first_OR_activity_date) AS action_to_OR_h
                ,datediff(day,response_date_adj,first_OR_activity_date) AS action_to_OR_d
                ,datediff(min,response_date_adj,first_SR_activity_date) AS action_to_SR_m
                ,datediff(hour,response_date_adj,first_SR_activity_date) AS action_to_SR_h
                ,datediff(day,response_date_adj,first_SR_activity_date) AS action_to_SR_d
                ,datediff(min,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_m
                ,datediff(hour,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_h
                ,datediff(day,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_d
                ,datediff(min,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_m
                ,datediff(hour,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_h
                ,datediff(day,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_d
                ,datediff(min,response_date_adj,first_opp_created_date) AS action_to_Opp_m
                ,datediff(hour,response_date_adj,first_opp_created_date) AS action_to_Opp_h
                ,datediff(day,response_date_adj,first_opp_created_date) AS action_to_Opp_d
                ,CASE WHEN first_opp_id IS NOT NULL THEN row_number() OVER (PARTITION BY first_opp_id ORDER BY response_date_adj ASC) ELSE NULL END AS mkt_cmp_rn
        FROM cte_summary s
        ) WHERE (mkt_cmp_rn = 1 OR mkt_cmp_rn IS NULL)