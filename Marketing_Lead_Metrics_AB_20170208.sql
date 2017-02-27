/*

Bartos
Created: 20170130
Updated: 20170222

Sales to Marketing Metrics

NOTES:
        Lead_or_contact_id (Campaign Member)
        Company_or_Account (Campaign Member)
        Type (Campaign Member)
        Lead_or_contact_owner_id (Campaign Member)
        Lead ID (if applicable - a prospect could just be a contact)
        Lead Created Date
        Lead Owner ID (will be 'Marketing Integration' if not yet assigned)
        Assigned_C (T/F)
        Assigned_Date_Time_C
        Is_Unread_By_Owner
        Status (of lead)
        Is_Converted
        Lead Converted_Date (will always match Contact Created Date)
        First_Action_Date_Time_C (on lead)
        Converted Contact ID
        Contact Created Date
        fcrm_fcr_created_by_lead_conversion_c (from Contact)
        Contact Owner ID (most often a rep; SAJ admin = unassigned; SnapLogic = eComm; SF Admin - Kristie)
        Contact Converted_Date (from lead to contact) - will match Created Date if converted from lead; some instances of NULLs (unclear why_
        Converted Account ID
        Account Created Date - will be same as Contact Created Date if brand new account
        Converted Opportunity ID
        Opportunity Created Date
        Campaign_Source_C - field in both lead and contact tables identifying associated campaign
        Campaign Name - ties to campaign_source_c
        Campaign Type - ties to campaign_source_c
        Campaign Sub-type - ties to campaign_source_c'
        fcrm_fcr_response_status_c (Campaign Member)
        Salesforce_Activity fields
        Associated bookings data (if the prospect made it that far)
        
        first opportunity created after rep touched the contact
        opp status (stage), expected close date, amt (forecast_amt_c - opp)
        time from contact to opp created
        time from campaign to opp created
        total activities after first touch within 14 days
        first Outreach touch and first non Outreach touch
        Outreach should be happening immediately
        time to assign/notify
        time to contact Out/not Out
        time next Opp created on this account***
        closed, stage, amt
        count activities
        contact vs account comparison
        check that what is on the lead, is also on the contact*
        is assigned field what we want for letting rep know?
        SLA time

       --start with leads, regardless of if they have a marketing campaign
       --how is date of response time created? when a prospect does an inbound action (request demo) - is that date auto-generated at time of action or do we update it?
       --dates in CM - how created
       --check with demos
       
        --limit opp from X days from when lead was created
        --one lead to one opp/interaction
        --should be following up on the first
        --converted contact ID
        --activity on the account ID - pull in account ID (coalesce for converted account id)
        --only look at opps with X time of SR activity
        --closed and won vs lost bookings
        --care about 1st marketing interaction, not second within X days (45 days) 
        --total campaign interactions vs distinct to lead contact
        --multiple contacts on same account - does this happen?
        --lead disqualification_reason_c
                --research to see if they truly should be disqualified
        --summary pivot on Q4 data:
                --total total prospects, total qual, total disqual, total touched by SR, total touched by rep with OR, total touched excl OR, opps created, bookings won, lost, outstanding (forecast amt where expected close dat in fture and stage name not lost)
                --first activity within 24 hours, 72 hours, week
        --separate: only accts we contacted (SR activitY), where created opp, turnaround time
        --2 pops (opp vs no opp) - median time b/w campaign creation and OB SR activity
                --where we had an opp - won vs lost
        --cumulative distrib of hours
       
*/

----------------------------------------------------------------MARKETING LEAD METRICS - VERSION 3----------------------------------------------------------------

--Create campaign_member table
        DROP TABLE IF EXISTS work_revopt.campaign_member;
        CREATE TEMPORARY TABLE work_revopt.campaign_member AS (  
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
                FROM prod_saj_share.salesforce.campaign_member cm
                LEFT JOIN prod_saj_share.salesforce.lead l ON cm.lead_id = l.id
                LEFT JOIN prod_saj_share.salesforce.contact c ON cm.contact_id = c.id
                LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id
                LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.account a ON l.converted_account_id = a.id
                LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id  
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND ucm.name <> 'Stephen Ernest'
                        AND response_date_adj >= '2016-11-01'
        );
        
        SELECT * FROM work_revopt.campaign_member;
        
        SELECT first_action_date_time_c
        FROM prod_saj_share.salesforce.lead
        where id = '00Q1a00000bYh7TEAS'
        

--Create First Sales Rep Activity on LEAD
        DROP TABLE IF EXISTS work_revopt.first_SR_activity_lead;
        CREATE TEMPORARY TABLE work_revopt.first_SR_activity_lead AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.lead_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM work_revopt.salesforce_completed_activity a
                        LEFT JOIN work_revopt.campaign_member cm ON a.lead_id = cm.lead_id
                        LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (((a.type IN ('Call','Webinar','Phone','Inmail','Closing Call') OR a.type IS NULL) OR a.sub_type LIKE '%Presentation%') OR ((email_direction = 'OB' AND outreach_flag = 0) OR (email_direction = 'IB' AND outreach_flag = 1)))
                ) WHERE rn = 1
        );
        
        SELECT * FROM work_revopt.first_SR_activity_lead;


--Create First Sales Rep Activity on CONTACT
        DROP TABLE IF EXISTS work_revopt.first_SR_activity_contact;
        CREATE TEMPORARY TABLE work_revopt.first_SR_activity_contact AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.contact_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM work_revopt.salesforce_completed_activity a
                        LEFT JOIN work_revopt.campaign_member cm ON a.contact_id = cm.contact_id
                        LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (((a.type IN ('Call','Webinar','Phone','Inmail','Closing Call') OR a.type IS NULL) OR a.sub_type LIKE '%Presentation%') OR ((email_direction = 'OB' AND outreach_flag = 0) OR (email_direction = 'IB' AND outreach_flag = 1)))
                ) WHERE rn = 1
        );
        
        SELECT * FROM work_revopt.first_SR_activity_contact;
        
--        SELECT * FROM work_revopt.salesforce_completed_activity
--        where activity_id = '00T1a00000jWQymEAG'

--        SELECT COUNT(*)
--        SELECT *
--        FROM prod_saj_share.salesforce.task
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
--             FROM prod_saj_share.salesforce.task
--            where 1=1
--                and completed_date_time_c is null 
--                and is_closed = 'True'--50851
--                and created_date > '2016-03-31'
--                group by task_subtype

          
--Create First Outreach Activity on LEAD
        DROP TABLE IF EXISTS work_revopt.first_OR_activity_lead;
        CREATE TEMPORARY TABLE work_revopt.first_OR_activity_lead AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.lead_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM work_revopt.salesforce_completed_activity a
                        LEFT JOIN work_revopt.campaign_member cm ON a.lead_id = cm.lead_id
                        LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND a.outreach_flag = 1
                                AND (email_direction = 'OB' AND outreach_flag = 1)
                ) WHERE rn = 1
        );
        
        SELECT * FROM work_revopt.first_OR_activity_lead;


--Create First Outreach Activity on CONTACT
        DROP TABLE IF EXISTS work_revopt.first_OR_activity_contact;
        CREATE TEMPORARY TABLE work_revopt.first_OR_activity_contact AS (  
                SELECT * FROM
                        (SELECT cm.campaign_id, cm.campaign_name, a.contact_id, a.account_id, u.name sales_rep, a.activity_id, a.subject, a.completed_date_ts, cm.response_date_adj, fcrm_fcr_response_date_c, cm.cm_created_date
                                ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                        FROM work_revopt.salesforce_completed_activity a
                        LEFT JOIN work_revopt.campaign_member cm ON a.contact_id = cm.contact_id
                        LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                        WHERE 1=1
                                AND a.completed_date_ts >= cm.response_date_adj
                                AND (email_direction = 'OB' AND outreach_flag = 1)
                ) WHERE rn = 1
        );
        
        SELECT * FROM work_revopt.first_OR_activity_contact;  


--Create Opportunities
        DROP TABLE IF EXISTS work_revopt.opps;
        CREATE TEMPORARY TABLE work_revopt.opps AS (  
              
                SELECT * FROM 
                (SELECT DISTINCT o.account_id, o.id opp_id, o.stage_name, o.type, o.close_date, o.forecast_amount_c, o.created_date, csr.completed_date_ts, csr.response_date_adj
                        ,row_number() OVER (PARTITION BY o.account_id ORDER BY o.created_date ASC) rn
                FROM prod_saj_share.salesforce.opportunity o
                LEFT JOIN work_revopt.first_SR_activity_contact csr ON o.account_id = csr.account_id
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
                FROM prod_saj_share.salesforce.opportunity o
                LEFT JOIN work_revopt.campaign_member cm ON o.account_id = cm.account_id
                LEFT JOIN work_revopt.first_SR_activity_contact csr ON o.account_id = csr.account_id
                WHERE 1=1
                        AND o.name NOT LIKE '%Orphan%'
                        AND o.name NOT LIKE '%Forecast%'
                        AND o.is_deleted = 'False'
                        AND o.created_date > cm.response_date_adj
                        )
                        --AND forecast_amount_c > 0)
                WHERE rn = 1 --164
                
       );
        
        SELECT * FROM work_revopt.opps;


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
        FROM work_revopt.campaign_member cm         
        LEFT JOIN work_revopt.first_SR_activity_lead lsr ON cm.lead_id = lsr.lead_id AND cm.campaign_id = lsr.campaign_id
        LEFT JOIN work_revopt.first_SR_activity_contact csr ON cm.contact_id = csr.contact_id AND cm.campaign_id = csr.campaign_id
        LEFT JOIN work_revopt.first_OR_activity_lead lor ON cm.lead_id = lor.lead_id AND cm.campaign_id = lor.campaign_id
        LEFT JOIN work_revopt.first_OR_activity_contact cor ON cm.contact_id = cor.contact_id AND cm.campaign_id = cor.campaign_id                                                            
        LEFT JOIN work_revopt.opps op ON cm.account_id = op.account_id
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








----------------------------------------------------------------MARKETING LEAD METRICS - VERSION 2----------------------------------------------------------------

        ;WITH cte_summary AS (
         SELECT DISTINCT cm.type
                ,cm.FCRM_FCR_Response_Status_c
                ,cm.lead_or_contact_id
                ,CASE WHEN cm.type = 'Lead' THEN cm.lead_id WHEN cm.type = 'Contact' AND cm.lead_id IS NOT NULL THEN cm.lead_id WHEN cm.type = 'Contact' AND cm.lead_id IS NULL THEN llu.id END AS lead_id
                ,cm.contact_id
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_contact_id WHEN cm.type = 'Contact' THEN llu.converted_contact_id END AS converted_contact_id
                ,COALESCE(c.account_id,llu.converted_account_id) AS account_id 
                ,ucm.name AS lead_or_contact_owner
--                ,CASE WHEN cm.type = 'Lead' THEN l.assigned_c WHEN cm.type = 'Contact' THEN llu.assigned_c END AS lead_assigned
--                ,CASE WHEN cm.type = 'Lead' THEN l.is_unread_by_owner WHEN cm.type = 'Contact' THEN llu.is_unread_by_owner END AS is_unread_by_owner   
                ,CASE WHEN cm.type = 'Lead' THEN l.status WHEN cm.type = 'Contact' THEN llu.status END AS lead_status
                ,c.fcrm_fcr_status_c AS contact_status
                ,l.disqualification_reason_c AS disqual_reason
                ,CASE WHEN cm.type = 'Lead' THEN l.is_converted WHEN cm.type = 'Contact' THEN llu.is_converted END AS is_converted       
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_opportunity_id WHEN cm.type = 'Contact' THEN llu.converted_opportunity_id END AS converted_opp_id                
                ,CASE WHEN cm.type = 'Lead' THEN l.created_date WHEN cm.type = 'Contact' THEN llu.created_date END AS lead_created_date  
                ,c.created_date AS contact_created_date
                ,cm.response_date_adj                               
                ,cm.created_date AS cm_created_date
                ,cm.fcrm_fcr_inquiry_target_date_c
                ,cm.FCRM_FCR_Response_Date_c
                ,cm.FCRM_FCR_QR_date_c
                ,cm.FCRM_FCR_SAR_date_c
                ,cm.FCRM_FCR_SQR_date_c
                ,CASE WHEN cm.type = 'Lead' THEN l.assigned_date_time_c WHEN cm.type = 'Contact' THEN llu.assigned_date_time_c END AS lead_assigned_date_time
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_date WHEN cm.type = 'Contact' THEN llu.converted_date END AS converted_date      
                ,cmp.name AS campaign_name
                ,cmp.type AS campaign_type
                ,cmp.id AS campaign_ID
                ,cmp.subtype_c AS campaign_subtype
                ,CASE WHEN cm.type = 'Lead' THEN lor.completed_date_ts WHEN cm.type = 'Contact' THEN cor.completed_date_ts END AS First_OR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lor.subject WHEN cm.type = 'Contact' THEN cor.subject END AS First_OR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lor.activity_id WHEN cm.type = 'Contact' THEN cor.activity_id END AS First_OR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.completed_date_ts WHEN cm.type = 'Contact' THEN csr.completed_date_ts END AS First_SR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lsr.subject WHEN cm.type = 'Contact' THEN csr.subject END AS First_SR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lsr.activity_id WHEN cm.type = 'Contact' THEN csr.activity_id END AS First_SR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.name WHEN cm.type = 'Contact' THEN csr.name END AS First_SR_Activity_Name
                ,op.created_date AS First_Opp_Created_Date
                ,op.close_date AS First_Opp_Close_Date
                ,op.opp_id AS First_Opp_ID
                ,cm.fcrm_fcr_opportunity_c
                ,op.stage_name AS First_Opp_Stage
                ,op.type AS First_Opp_Type
                ,op.forecast_amount_c AS First_Opp_Booking_Amt
                ,cm.FCRM_FCR_Inquiry_Target_c
                ,cm.FCRM_FCR_QR_c
                ,cm.FCRM_FCR_SAR_c
                ,cm.FCRM_FCR_SQR_c
                ,cm.FCRM_FCR_SQR_Won_c
                ,cm.FCRM_FCR_first_queue_assigned_c
                ,cm.FCRM_FCR_first_owner_assigned_c
                ,cm.FCRM_FCR_opportunity_created_by_c
                ,cm.FCRM_FCR_dated_opportunity_amount_c
                ,cm.FCRM_FCR_first_owner_worked_c
                ,cm.FCRM_FCR_sar_owner_c
        FROM (SELECT CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24)
                                THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                ,cm.*
                FROM prod_saj_share.salesforce.campaign_member cm) cm
        LEFT JOIN prod_saj_share.salesforce.lead l ON cm.lead_id = l.id
        LEFT JOIN prod_saj_share.salesforce.contact c ON cm.contact_id = c.id
        LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id
        LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
        LEFT JOIN prod_saj_share.salesforce.account a ON l.converted_account_id = a.id
        LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id   
        
        LEFT JOIN (SELECT * FROM
                (SELECT cm.campaign_id, cmp.name, a.lead_id, u.name user_name, a.activity_id, a.subject, a.completed_date_ts , response_date_adj ,fcrm_fcr_response_date_c, cm.created_date
                        ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN (
                        SELECT CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                                ,cm.*
                        FROM prod_saj_share.salesforce.campaign_member cm) cm on a.lead_id = cm.lead_id
                LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND a.completed_date_ts >= cm.response_date_adj
                        AND outreach_flag = 0
                        AND response_date_adj >= '2016-11-01'                    
                        ) WHERE rn = 1) lsr ON cm.lead_id = lsr.lead_id AND cm.campaign_id = lsr.campaign_id
        
        LEFT JOIN (SELECT * FROM
                (SELECT cm.campaign_id, cmp.name, a.contact_id, u.name user_name, a.activity_id, a.subject, a.completed_date_ts , response_date_adj ,fcrm_fcr_response_date_c, cm.created_date
                        ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN (
                        SELECT CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                                ,cm.*
                        FROM prod_saj_share.salesforce.campaign_member cm) cm on a.contact_id = cm.contact_id
                LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND a.completed_date_ts >= cm.response_date_adj
                        AND outreach_flag = 0
                        AND response_date_adj >= '2016-11-01'
--                        AND cm.contact_id = '0031a00000c5zN4AAI'                       
                        ) WHERE rn = 1) csr ON cm.contact_id = csr.contact_id AND cm.campaign_id = csr.campaign_id
                        
        LEFT JOIN (SELECT * FROM
                (SELECT cm.campaign_id, cmp.name, a.lead_id, u.name user_name, a.activity_id, a.subject, a.completed_date_ts , response_date_adj ,fcrm_fcr_response_date_c, cm.created_date
                        ,row_number() OVER (PARTITION BY cm.lead_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN (
                        SELECT CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                                ,cm.*
                        FROM prod_saj_share.salesforce.campaign_member cm) cm on a.lead_id = cm.lead_id
                LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND a.completed_date_ts >= cm.response_date_adj
                        AND outreach_flag = 1
                        AND response_date_adj >= '2016-11-01'                    
                        ) WHERE rn = 1) lor ON cm.lead_id = lor.lead_id AND cm.campaign_id = lor.campaign_id
        
        LEFT JOIN (SELECT * FROM
                (SELECT cm.campaign_id, cmp.name, a.contact_id, u.name user_name, a.activity_id, a.subject, a.completed_date_ts , response_date_adj ,fcrm_fcr_response_date_c, cm.created_date
                        ,row_number() OVER (PARTITION BY cm.contact_id, cm.campaign_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN (
                        SELECT CASE WHEN (datediff(hour,cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                                ,cm.*
                        FROM prod_saj_share.salesforce.campaign_member cm) cm on a.contact_id = cm.contact_id
                LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND a.completed_date_ts >= cm.response_date_adj
                        AND outreach_flag = 1
                        AND response_date_adj >= '2016-11-01'                      
                        ) WHERE rn = 1) cor ON cm.contact_id = cor.contact_id AND cm.campaign_id = cor.campaign_id                        
                        
        LEFT JOIN (SELECT * FROM 
                (SELECT account_id, id opp_id, stage_name, type, close_date ,forecast_amount_c ,created_date
                        ,row_number() OVER (PARTITION BY account_id ORDER BY created_date ASC) rn
                FROM prod_saj_share.salesforce.opportunity o
                WHERE 1=1
                        AND name NOT LIKE '%Orphan%'
                        AND name NOT LIKE '%Forecast%'
                        AND account_id = '0011a00000WSDVrAAP'
                        AND forecast_amount_c > 0)
                WHERE rn = 1) op ON COALESCE(c.account_id,llu.converted_account_id) = op.account_id --30 day limitation
       
        WHERE 1=1
                AND cm.is_deleted = 'False'
                AND cm.has_responded = 'True'
                AND cmp.type = 'Website'
                AND cmp.subtype_c <> 'Newslettter Sign Up'
                AND ucm.name <> 'Stephen Ernest'
                AND cm.response_date_adj >= '2016-11-01'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53
        )
        
        SELECT * FROM cte_summary s
        
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
                --AND lead_source = 'Contact Us/Request a Demo'
                --AND first_opp_booking_amt IS NOT NULL
        ) WHERE (mkt_cmp_rn = 1 OR mkt_cmp_rn IS NULL)
      
      
        
        SELECT *
        FROM prod_saj_share.salesforce.campaign_member cm
        LEFT JOIN prod_saj_share.salesforce.contact c on cm.contact_id = c.id
        LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id
        WHERE llu.converted_account_id = '0011a00000Z7uHTAAZ'
        

                SELECT CASE WHEN (datediff(hour, cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                        ,cm.*
                SELECT CASE WHEN (datediff(hour, cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) THEN dateadd(hour,6,fcrm_fcr_response_date_c) ELSE cm.created_date END AS response_date_adj
                          ,cm.campaign_id
                        ,uc.name
                        ,cm.lead_or_contact_id
                        ,cm.created_date
                        ,fcrm_fcr_response_date_c
                        --,date_part(hour,fcrm_fcr_response_date_c)
        --                ,datediff(hour, cm.fcrm_fcr_response_date_c,cm.created_date)
                        ,datediff(hour, cm.created_date, cm.fcrm_fcr_response_date_c)
        --        SELECT *
                FROM prod_saj_share.salesforce.campaign_member cm
                LEFT JOIN prod_saj_share.salesforce.campaign cmp on cm.campaign_id = cmp.id
                LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id   
                LEFT JOIN prod_saj_share.salesforce.user uc ON cm.created_by_id = uc.id   
                WHERE 1=1
                        AND cm.is_deleted = 'False'
                        AND cm.has_responded = 'True'
                        --AND uc.name NOT IN ('Full Circle Integration','Marketing Integration','Ema Gantcheva','Jen Dugosh')
                        AND cmp.type = 'Website'
                        AND cmp.subtype_c <> 'Newslettter Sign Up'
                        AND ucm.name <> 'Stephen Ernest'
        --                AND uc.name = 'Full Circle Integration'
                        --AND cm.created_date < cm.fcrm_fcr_response_date_c
        --                AND datediff(hour, cm.fcrm_fcr_response_date_c,cm.created_date) > 24
        --                AND (datediff(hour, cm.created_date, cm.fcrm_fcr_response_date_c) NOT BETWEEN -24 AND 24) -- USE RESPONSE DATE FOR THESE SITUATIONS
        --                AND (datediff(hour, cm.created_date, cm.fcrm_fcr_response_date_c) BETWEEN -24 AND 24) -- USE CREATED DATE FOR THESE SITUATIONS
                        AND cm.created_date >= '2016-11-01'


        SELECT *
        FROM prod_saj_share.salesforce.contact
        WHERE email LIKE '%@snagajob.com%' and is_deleted = 'False'
----------------------------------------------------------------MARKETING LEAD METRICS - VERSION 1----------------------------------------------------------------

        ;WITH cte_summary AS (
         SELECT DISTINCT cm.type
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
                ,cm.created_date AS cm_created_date
                ,cm.first_responded_date
                ,cm.FCRM_FCR_Response_Date_c
                ,CASE WHEN cm.type = 'Lead' THEN l.assigned_date_time_c WHEN cm.type = 'Contact' THEN llu.assigned_date_time_c END AS lead_assigned_date_time
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_date WHEN cm.type = 'Contact' THEN llu.converted_date END AS converted_date      
                ,cmp.name AS campaign_name
                ,cmp.type AS campaign_type
                ,cmp.id AS campaign_ID
                ,cmp.subtype_c AS campaign_subtype
                ,CASE WHEN cm.type = 'Lead' THEN lor.completed_date_ts WHEN cm.type = 'Contact' THEN cor.completed_date_ts END AS First_OR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lor.subject WHEN cm.type = 'Contact' THEN cor.subject END AS First_OR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lor.activity_id WHEN cm.type = 'Contact' THEN cor.activity_id END AS First_OR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.completed_date_ts WHEN cm.type = 'Contact' THEN csr.completed_date_ts END AS First_SR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lsr.subject WHEN cm.type = 'Contact' THEN csr.subject END AS First_SR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lsr.activity_id WHEN cm.type = 'Contact' THEN csr.activity_id END AS First_SR_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.name WHEN cm.type = 'Contact' THEN csr.name END AS First_SR_Activity_Name
                ,op.created_date AS First_Opp_Created_Date
                ,op.close_date AS First_Opp_Close_Date
                ,op.opp_id AS First_Opp_ID
                ,op.stage_name AS First_Opp_Stage
                ,op.type AS First_Opp_Type
                ,op.forecast_amount_c AS First_Opp_Booking_Amt
                ,cm.FCRM_FCR_Inquiry_Target_c
                ,cm.FCRM_FCR_QR_c
                ,cm.FCRM_FCR_SAR_c
                ,cm.FCRM_FCR_SQR_c
                ,cm.FCRM_FCR_SQR_Won_c
                ,cm.FCRM_FCR_Response_Status_c
                ,cm.FCRM_FCR_first_queue_assigned_c
                ,cm.FCRM_FCR_first_owner_assigned_c
                ,cm.FCRM_FCR_opportunity_created_by_c
                ,cm.FCRM_FCR_dated_opportunity_amount_c
                ,cm.FCRM_FCR_opportunity_c
                ,cm.FCRM_FCR_first_owner_worked_c
                ,cm.FCRM_FCR_sar_owner_c
        FROM prod_saj_share.salesforce.campaign_member cm
        LEFT JOIN prod_saj_share.salesforce.lead l ON cm.lead_id = l.id
        LEFT JOIN prod_saj_share.salesforce.contact c ON cm.contact_id = c.id
        LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id
        LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
        LEFT JOIN prod_saj_share.salesforce.account a ON l.converted_account_id = a.id
        LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id   
        LEFT JOIN (SELECT * FROM
                (SELECT a.lead_id, a.person_id, a.activity_id, a.subject, a.completed_date_ts 
                        ,row_number() OVER (PARTITION BY a.lead_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN prod_saj_share.salesforce.campaign_member cm on a.lead_id = cm.lead_id
                WHERE 1=1
                        AND a.completed_date_ts > cm.created_date
                        AND a.outreach_flag = 1 AND a.email_direction = 'OB') WHERE rn = 1) lor ON cm.lead_id = lor.lead_id
        LEFT JOIN (SELECT * FROM
                (SELECT a.contact_id, a.person_id, a.activity_id, a.subject, a.completed_date_ts 
                        ,row_number() OVER (PARTITION BY a.contact_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN prod_saj_share.salesforce.campaign_member cm on a.contact_id = cm.contact_id
                WHERE 1=1
                        AND a.completed_date_ts > cm.created_date
                        AND a.outreach_flag = 1 AND a.email_direction = 'OB') WHERE rn = 1) cor ON cm.contact_id = cor.contact_id
        LEFT JOIN (SELECT * FROM
                (SELECT a.lead_id, a.person_id, a.activity_id, a.subject, a.completed_date_ts, u.name 
                        ,row_number() OVER (PARTITION BY a.lead_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN prod_saj_share.salesforce.campaign_member cm on a.lead_id = cm.lead_id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND a.completed_date_ts > cm.created_date
                        AND a.outreach_flag = 0) WHERE rn = 1) lsr ON cm.lead_id = lsr.lead_id
        LEFT JOIN (SELECT * FROM
                (SELECT a.contact_id, a.person_id, a.activity_id, a.subject, a.completed_date_ts , u.name ,cm.created_date
                        ,row_number() OVER (PARTITION BY a.contact_id ORDER BY a.completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity a
                LEFT JOIN prod_saj_share.salesforce.campaign_member cm on a.contact_id = cm.contact_id
                LEFT JOIN prod_saj_share.salesforce.user u on a.person_id = u.id
                WHERE 1=1
                        AND a.completed_date_ts > cm.created_date  
                        AND outreach_flag = 0) WHERE rn = 1) csr ON cm.contact_id = csr.contact_id
        LEFT JOIN (SELECT * FROM 
                (SELECT account_id, id opp_id, stage_name, type, close_date ,forecast_amount_c ,created_date
                        ,row_number() OVER (PARTITION BY account_id ORDER BY created_date ASC) rn
                FROM prod_saj_share.salesforce.opportunity
                WHERE name NOT LIKE '%Orphan%' AND name NOT LIKE '%Forecast%' AND forecast_amount_c > 0)
                where rn = 1) op ON c.account_id = op.account_id AND op.created_date BETWEEN csr.completed_date_ts AND DATEADD(day,30,csr.completed_date_ts) --30 day limitation
        WHERE 1=1
                AND cm.is_deleted = 'False'
                AND cm.has_responded = 'True'
                AND cmp.type = 'Website'
                AND cmp.subtype_c <> 'Newslettter Sign Up'
                AND ucm.name <> 'Stephen Ernest'
                AND cm.created_date BETWEEN '2017-01-01' AND '2017-01-31'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51
        )
        
        SELECT * FROM (
        SELECT s.*
                ,datediff(day,lead_created_date,lead_assigned_date_time) AS lead_assignment_d
                ,datediff(min,lead_created_date,contact_created_date) AS lead_conversion_m
                ,datediff(hour,lead_created_date,contact_created_date) AS lead_conversion_h
                ,datediff(day,lead_created_date,contact_created_date) AS lead_conversion_d
                ,datediff(min,cm_created_date,first_OR_activity_date) AS action_to_OR_m
                ,datediff(hour,cm_created_date,first_OR_activity_date) AS action_to_OR_h
                ,datediff(day,cm_created_date,first_OR_activity_date) AS action_to_OR_d
                ,datediff(min,cm_created_date,first_SR_activity_date) AS action_to_SR_m
                ,datediff(hour,cm_created_date,first_SR_activity_date) AS action_to_SR_h
                ,datediff(day,cm_created_date,first_SR_activity_date) AS action_to_SR_d
                ,datediff(min,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_m
                ,datediff(hour,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_h
                ,datediff(day,first_OR_activity_date,first_SR_activity_date) AS OR_to_SR_d
                ,datediff(min,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_m
                ,datediff(hour,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_h
                ,datediff(day,first_SR_activity_date,first_opp_created_date) AS SR_to_Opp_d
                ,datediff(min,cm_created_date,first_opp_created_date) AS action_to_Opp_m
                ,datediff(hour,cm_created_date,first_opp_created_date) AS action_to_Opp_h
                ,datediff(day,cm_created_date,first_opp_created_date) AS action_to_Opp_d
                ,CASE WHEN first_opp_id IS NOT NULL THEN row_number() OVER (PARTITION BY first_opp_id ORDER BY cm_created_date ASC) ELSE NULL END AS mkt_cmp_rn
        FROM cte_summary s
                --AND lead_source = 'Contact Us/Request a Demo'
                --AND first_opp_booking_amt IS NOT NULL
        ) WHERE (mkt_cmp_rn = 1 OR mkt_cmp_rn IS NULL)
        
        
        SELECT created_date
                ,first_responded_date
                ,fcrm_fcr_response_date_c
                SELECT *
        FROM prod_saj_share.salesforce.campaign_member
        WHERE 1=1
                AND contact_id = '0031a00000MIeAMAA1'
                AND created_date BETWEEN '2017-01-01' AND '2017-01-31'
        
                
        SELECT * FROM work_revopt.salesforce_completed_activity
        WHERE contact_id ='0031a00000KlyUeAAJ'
        
-------------------------------------------------------------------EXPLORATION-------------------------------------------------------------------        
        
        SELECT DISTINCT fcrm_fcr_status_c
        FROM prod_saj_share.salesforce.contact;
        
        SELECT DISTINCT status
        FROM prod_saj_share.salesforce.lead
        
        
        SELECT *
        FROM prod_saj_share.salesforce.campaign_member
        WHERE contact_id IS NULL

        SELECT DISTINCT lead_source
                ,COUNT(*)
        FROM prod_saj_share.salesforce.campaign_member
        GROUP BY lead_source


        SELECT DISTINCT status
                ,COUNT(*)
        FROM prod_saj_share.salesforce.lead
        GROUP BY status


;WITH cte_summary AS(
        SELECT DISTINCT cm.created_date AS cm_created_
                ,l.created_date as l_created
                ,datediff(min,l.created_date,cm.created_date) AS timediff_min
                ,datediff(hour,l.created_date,cm.created_date) AS timediff_hour
                ,datediff(day,l.created_date,cm.created_date) AS timediff_day
        FROM prod_saj_share.salesforce.campaign_member cm
        JOIN prod_saj_share.salesforce.lead l on cm.lead_id = l.id
        WHERE 1=1
                --AND lead_or_contact_id = '0031a00000JW6IjAAL'
                AND cm.is_deleted = 'False'
        )
        SELECT MIN(timediff_min) AS min_min
                ,MIN(timediff_hour) AS min_hour
                ,MIN(timediff_day) AS min_day
                ,max(timediff_max) AS max_min
                ,max(timediff_hour) AS max_hour
                ,max(timediff_day) AS max_day
        
        SELECT created_date
        FROM prod_saj_share.salesforce.lead
        WHERE id = '00Q1a00000GpzONEAZ';
        
        SELECT created_date
        FROM prod_saj_share.salesforce.contact
        WHERE id = '0031a00000JW6IjAAL';


;WITH cte_summary AS(        
        SELECT created_date
                ,first_responded_date
                ,first_responded_date::timestamp
                ,datediff(hour,first_responded_date::timestamp,created_date) AS timediff_hour
                ,datediff(day,first_responded_date::timestamp,created_date) AS timediff_day
                ,lead_source
                ,has_responded
                ,status         
        FROM prod_saj_share.salesforce.campaign_member cm
        WHERE has_responded = 'True'
                and is_deleted = 'False'
        )
        SELECT *
        FROM cte_summary
        WHERE timediff_hour BETWEEN 0 AND 24
        
        --75896
        --75895



        SELECT DISTINCT converted_date_c
                ,count(*)
        FROM prod_saj_share.salesforce.contact
--        where type = 'Contact'
        GROUP BY converted_date_c
        
        SELECT *
        FROM prod_saj_share.salesforce.lead
        WHERE id = '00Q1a00000a4iyKEAQ';
        
        SELECT DISTINCT sfacco.*,sfacc.*
        FROM prod_saj_share.salesforce.contact c
        LEFT JOIN prod_saj_share.salesforce.campaign_member cm on c.id = cm.contact_id
        LEFT JOIN prod_saj_share.salesforce.lead llu on c.id = llu.converted_contact_id
        LEFT JOIN work_revopt.salesforce_completed_activity sfacc ON cm.lead_or_contact_owner_id = sfacc.person_id AND cm.contact_id = sfacc.contact_id AND sfacc.outreach_flag = 0 AND sfacc.email_direction = 'OB' --AND sfacc.completed_date_ts > l.assigned_date_time_c
        LEFT JOIN work_revopt.salesforce_completed_activity sfacco ON cm.lead_or_contact_owner_id = sfacco.person_id AND cm.contact_id = sfacco.contact_id AND sfacco.outreach_flag = 1 AND sfacco.email_direction = 'OB' --AND sfacc.completed_date_ts > l.assigned_date_time_c
        WHERE c.id = '0031a00000c80QcAAI'
        
        SELECT DISTINCT account_lead_source_c
                ,COUNT(*)
        FROM prod_saj_share.salesforce.lead
        GROUP BY account_lead_source_c;
        
        SELECT DISTINCT lead_source
                ,COUNT(*)
        FROM prod_saj_share.salesforce.lead
        GROUP BY lead_source
             
        select 
        contact_id
        ,subject
        ,completed_date_ts
        
        from 
        work_revopt.salesforce_completed_activity 
        where contact_id = '0031a00000WlbFtAAJ'
        and completed_date_ts = (select min(completed_date_ts) from work_revopt.salesforce_completed_activity where contact_id = '0031a00000WlbFtAAJ')    

        SELECT DISTINCT type
        FROM prod_saj_share.salesforce.opportunity





----------------------------------------------------------------MARKETING LEAD METRICS - VERSION 2----------------------------------------------------------------

;WITH cte_summary AS (
         SELECT DISTINCT cm.type
                ,CASE WHEN cm.type = 'Lead' THEN cm.lead_id WHEN cm.type = 'Contact' THEN llu.id END AS lead_id
                ,cm.contact_id AS contact_id
                ,cm.lead_or_contact_id
                ,ucm.name AS lead_or_contact_owner
                ,CASE WHEN cm.type = 'Lead' THEN l.created_date WHEN cm.type = 'Contact' THEN llu.created_date END AS lead_created_date
                ,CASE WHEN cm.type = 'Lead' THEN l.status WHEN cm.type = 'Contact' THEN llu.status END AS lead_status
                ,CASE WHEN cm.type = 'Lead' THEN l.lead_source WHEN cm.type = 'Contact' THEN llu.lead_source END AS lead_source   
                --ASSIGNMENT/NOTIFICATION DATE - need to figure out what to do in different scenarios
                        ,CASE WHEN cm.type = 'Lead' THEN l.assigned_c WHEN cm.type = 'Contact' THEN llu.assigned_c END AS lead_assigned
                        ,CASE WHEN cm.type = 'Lead' THEN l.assigned_date_time_c WHEN cm.type = 'Contact' THEN llu.assigned_date_time_c END AS lead_assigned_date_time
                        ,CASE WHEN cm.type = 'Lead' THEN l.is_unread_by_owner WHEN cm.type = 'Contact' THEN llu.is_unread_by_owner END AS is_unread_by_owner               
                ,CASE WHEN cm.type = 'Lead' THEN l.is_converted WHEN cm.type = 'Contact' THEN llu.is_converted END AS is_converted
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_date WHEN cm.type = 'Contact' THEN llu.converted_date END AS converted_date
                ,c.created_date AS contact_created_date
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_contact_id WHEN cm.type = 'Contact' THEN llu.converted_contact_id END AS converted_contact_id
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_account_id WHEN cm.type = 'Contact' THEN llu.converted_account_id END AS converted_account_id
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_opportunity_id WHEN cm.type = 'Contact' THEN llu.converted_opportunity_id END AS converted_opportunity_id
                ,cmp.name AS campaign_name
                ,cmp.type AS campaign_type
                ,cmp.id AS campaign_ID
                ,cmp.subtype_c AS campaign_subtype
                ,cmp.start_date AS campaign_start_date
                ,cmp.end_date AS campaign_end_date
                ,CASE WHEN cm.type = 'Lead' THEN lor.completed_date_ts WHEN cm.type = 'Contact' THEN cor.completed_date_ts END AS First_Outreach_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lor.subject WHEN cm.type = 'Contact' THEN cor.subject END AS First_Outreach_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lor.activity_id WHEN cm.type = 'Contact' THEN cor.activity_id END AS First_Outreach_Activity_ID
                ,CASE WHEN cm.type = 'Lead' THEN lsr.completed_date_ts WHEN cm.type = 'Contact' THEN csr.completed_date_ts END AS First_SR_Activity_Date
                ,CASE WHEN cm.type = 'Lead' THEN lsr.subject WHEN cm.type = 'Contact' THEN csr.subject END AS First_SR_Activity_Subject
                ,CASE WHEN cm.type = 'Lead' THEN lsr.activity_id WHEN cm.type = 'Contact' THEN csr.activity_id END AS First_SR_Activity_ID
                --,Total_SR_Acts_14day_Creation --total activities after first touch within 14 days
                ,op.close_date AS First_Opp_Close_Date
                ,op.opp_id AS First_Opp_ID
                ,op.stage_name AS First_Opp_Stage
                ,op.type AS First_Opp_Type
                ,op.forecast_amount_c AS First_Opp_Booking_Amt
--                ,CASE WHEN opa.type IN ('New Logo','New') THEN SUM(opa.forecast_amount_c) ELSE NULL END AS Total_New_Logo_Opp_Bookings
--                ,CASE WHEN opa.type IN ('Upsell New Logo','Upsell') THEN SUM(opa.forecast_amount_c) ELSE NULL END AS Total_Upsell_Opp_Bookings

        FROM prod_saj_share.salesforce.campaign_member cm
        LEFT JOIN prod_saj_share.salesforce.lead l ON cm.lead_id = l.id
        LEFT JOIN prod_saj_share.salesforce.contact c ON cm.contact_id = c.id
        LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id
        LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
        LEFT JOIN prod_saj_share.salesforce.account a ON l.converted_account_id = a.id
        LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id   


        LEFT JOIN (SELECT * FROM
                (SELECT lead_id, person_id, activity_id, subject, completed_date_ts 
                        ,row_number() OVER (PARTITION BY lead_id ORDER BY completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity
                WHERE outreach_flag = 1 AND email_direction = 'OB') WHERE rn = 1) lor ON cm.lead_id = lor.lead_id AND cm.lead_or_contact_owner_id = lor.person_id
        LEFT JOIN (SELECT * FROM
                (SELECT contact_id, person_id, activity_id, subject, completed_date_ts 
                        ,row_number() OVER (PARTITION BY contact_id ORDER BY completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity
                WHERE outreach_flag = 1 AND email_direction = 'OB') WHERE rn = 1) cor ON cm.contact_id = cor.contact_id AND cm.lead_or_contact_owner_id = cor.person_id
        LEFT JOIN (SELECT * FROM
                (SELECT lead_id, person_id, activity_id, subject, completed_date_ts 
                        ,row_number() OVER (PARTITION BY lead_id ORDER BY completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity
                WHERE outreach_flag = 0 AND email_direction = 'OB') WHERE rn = 1) lsr ON cm.lead_id = lsr.lead_id AND cm.lead_or_contact_owner_id = lsr.person_id
        LEFT JOIN (SELECT * FROM
                (SELECT contact_id, person_id, activity_id, subject, completed_date_ts 
                        ,row_number() OVER (PARTITION BY contact_id ORDER BY completed_date_ts ASC) rn
                FROM work_revopt.salesforce_completed_activity
                WHERE outreach_flag = 0 AND email_direction = 'OB') WHERE rn = 1) csr ON cm.contact_id = csr.contact_id AND cm.lead_or_contact_owner_id = csr.person_id
           
           
        LEFT JOIN (SELECT * FROM 
                (SELECT account_id, id opp_id, stage_name, type, close_date ,forecast_amount_c
                        ,row_number() OVER (PARTITION BY account_id ORDER BY close_date ASC) rn
                FROM prod_saj_share.salesforce.opportunity
                WHERE name NOT LIKE '%Orphan%')
                where rn = 1) op ON c.account_id = op.account_id AND op.close_date > csr.completed_date_ts
                
--        LEFT JOIN (SELECT * FROM 
--                (SELECT account_id, id opp_id, stage_name, type, close_date ,forecast_amount_c
--                        ,row_number() OVER (PARTITION BY account_id ORDER BY close_date ASC) rn
--                FROM prod_saj_share.salesforce.opportunity
--                WHERE name NOT LIKE '%Orphan%' AND type IN ('Upsell','Upsell New Logo') AND stage_name NOT LIKE '%Lost%'
--                ) where rn = 1) opu ON c.account_id = opu.account_id AND opu.close_date > csr.completed_date_ts
--        LEFT JOIN (
--                SELECT account_id, id opp_id, stage_name, type, close_date, forecast_amount_c
--                FROM prod_saj_share.salesforce.opportunity
--                WHERE name NOT LIKE '%Orphan%' AND type IN ('New Logo','New','Upsell','Upsell New Logo') AND stage_name NOT LIKE '%Lost%'
--                ) opa ON c.account_id = opa.account_id AND opa.close_date > csr.completed_date_ts           

        WHERE 1=1
                --AND c.id = '0031a00000c80QcAAI'
                --AND l.converted_opportunity_ID IS NOT NULL
                AND cm.is_deleted = 'False'
                AND cm.has_responded = 'True'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34
        )
        
        SELECT *
        FROM cte_summary
        WHERE 1=1
--                AND is_converted = 'True'
--                AND first_sr_activity_date IS NOT NULL
--                AND first_new_logo_opp_booking_amt IS NOT NULL
                AND contact_id = '0031a00000JW6IjAAL'


------------------------------------------------------------MARKETING LEAD METRICS - VERSION 1-----------------------------------------------------------

;WITH cte_summary AS(
        SELECT DISTINCT cm.type
                ,cm.lead_id AS lead_id_cm
                ,CASE WHEN cm.type = 'Lead' THEN l.id WHEN cm.type = 'Contact' THEN llu.id END AS lead_id_l
                ,cm.contact_id AS contact_id_cm
                ,c.id AS contact_id_c
                ,cm.lead_or_contact_id
                ,cm.lead_or_contact_owner_id
                ,ucm.name AS lead_or_contact_owner_ucm
                ,ul.name AS lead_owner_l
                ,uc.name AS contact_owner_c
                ,CASE WHEN cm.type = 'Lead' THEN l.created_date WHEN cm.type = 'Contact' THEN llu.created_date END AS lead_created_date
                ,c.created_date AS contact_created_date
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_contact_id WHEN cm.type = 'Contact' THEN llu.converted_contact_id END AS converted_contact_id
                ,a.created_date AS account_created_date
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_account_id WHEN cm.type = 'Contact' THEN llu.converted_account_id END AS converted_account_id
                ,op.created_date AS opportunity_created_date
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_opportunity_id WHEN cm.type = 'Contact' THEN llu.converted_opportunity_id END AS converted_opportunity_id
                ,CASE WHEN cm.type = 'Lead' THEN l.assigned_c WHEN cm.type = 'Contact' THEN llu.assigned_c END AS assigned_c 
                ,CASE WHEN cm.type = 'Lead' THEN l.assigned_date_time_c WHEN cm.type = 'Contact' THEN llu.assigned_date_time_c END AS assigned_date_time_c
                ,CASE WHEN cm.type = 'Lead' THEN l.is_unread_by_owner WHEN cm.type = 'Contact' THEN llu.is_unread_by_owner END AS is_unread_by_owner
                ,CASE WHEN cm.type = 'Lead' THEN l.status WHEN cm.type = 'Contact' THEN llu.status END AS lead_status_l
                ,CASE WHEN cm.type = 'Lead' THEN l.is_converted WHEN cm.type = 'Contact' THEN llu.is_converted END AS is_converted
                ,CASE WHEN cm.type = 'Lead' THEN l.converted_date WHEN cm.type = 'Contact' THEN llu.converted_date END AS converted_date_l 
                ,c.converted_date_c AS converted_date_c
                ,c.fcrm_fcr_created_by_lead_conversion_c
                ,CASE WHEN cm.type = 'Lead' THEN l.first_action_date_time_c WHEN cm.type = 'Contact' THEN llu.first_action_date_time_c END AS first_action_date_time_c
                ,CASE WHEN cm.type = 'Lead' THEN l.campaign_source_c WHEN cm.type = 'Contact' THEN llu.campaign_source_c END AS lead_campaign_source
                ,c.campaign_source_c AS contact_campaign_source
                ,cmp.name AS campaign_name
                ,cmp.type AS campaign_type
                ,cmp.subtype_c AS campaign_subtype
                ,cm.fcrm_fcr_response_status_c
                --,CASE WHEN cm.lead_ID IS NOT NULL THEN MIN(sfacl.completed_date_ts) WHEN cm.contact_ID IS NOT NULL AND llu.id IS NULL THEN MIN(sfacc.completed_date_ts) ELSE NULL END AS First_SR_Activity_Date
                --,CASE WHEN cm.lead_ID IS NOT NULL THEN MIN(sfacl.completed_date_ts) WHEN cm.contact_ID IS NOT NULL THEN MIN(sfacc.completed_date_ts) ELSE NULL END AS First_SR_Activity_Date
                ,MIN(sfacl.completed_date_ts) AS First_SR_Activity_Date_Lead
                ,MIN(sfacc.completed_date_ts) AS First_SR_Activity_Date_Contact
        FROM prod_saj_share.salesforce.campaign_member cm
        LEFT JOIN prod_saj_share.salesforce.lead l ON cm.lead_id = l.id
        LEFT JOIN prod_saj_share.salesforce.contact c ON cm.contact_id = c.id
        LEFT JOIN prod_saj_share.salesforce.lead llu ON c.id = llu.converted_contact_id --AND llu.is_converted = 'True' --is_converted not necessary (picks up based on converted_contact_id)
        LEFT JOIN prod_saj_share.salesforce.campaign cmp ON cm.campaign_id = cmp.id
        LEFT JOIN prod_saj_share.salesforce.account a ON l.converted_account_id = a.id
        LEFT JOIN prod_saj_share.salesforce.opportunity op ON l.converted_opportunity_id = op.id
        LEFT JOIN prod_saj_share.salesforce.user ul ON l.owner_id = ul.id
        LEFT JOIN prod_saj_share.salesforce.user uc ON c.owner_id = uc.id
        LEFT JOIN prod_saj_share.salesforce.user ucm ON cm.lead_or_contact_owner_id = ucm.id
        LEFT JOIN work_revopt.salesforce_completed_activity sfacl ON cm.lead_or_contact_owner_id = sfacl.person_id AND l.id = sfacl.lead_id AND sfacl.outreach_flag = 0 AND sfacl.completed_date_ts > l.assigned_date_time_c
        LEFT JOIN work_revopt.salesforce_completed_activity sfacc ON cm.lead_or_contact_owner_id = sfacc.person_id AND c.id = sfacc.contact_id AND sfacc.outreach_flag = 0 AND sfacc.completed_date_ts > llu.assigned_date_time_c
        WHERE 1=1
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32
        )

        SELECT s.*
                ,sfca.type
                ,sfca.sub_type
                ,sfca.subject
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(minute,lead_created_date,assigned_date_time_c)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(minute,contact_created_date,assigned_date_time_c) END AS create_to_assign_min
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(hour,lead_created_date,assigned_date_time_c)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(hour,contact_created_date,assigned_date_time_c) END AS create_to_assign_hour
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(day,lead_created_date,assigned_date_time_c)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(day,contact_created_date,assigned_date_time_c) END AS create_to_assign_day                                                

                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(minute,assigned_date_time_c,first_sr_activity_date_lead)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(minute,assigned_date_time_c,first_sr_activity_date_contact) END AS assign_to_activity_min
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(hour,assigned_date_time_c,first_sr_activity_date_lead)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(hour,assigned_date_time_c,first_sr_activity_date_contact) END AS assign_to_activity_hour               
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(day,assigned_date_time_c,first_sr_activity_date_lead)
                        WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(day,assigned_date_time_c,first_sr_activity_date_contact) END AS assign_to_activity_day
                        
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(minute,lead_created_date,first_sr_activity_date_lead) 
                         WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(minute,contact_created_date,first_sr_activity_date_contact) END AS full_create_to_activity_min
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(hour,lead_created_date,first_sr_activity_date_lead) 
                         WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(hour,contact_created_date,first_sr_activity_date_contact) END AS full_create_to_activity_hour
                ,CASE WHEN (s.type = 'Lead' OR (s.type = 'Contact' AND s.lead_id_l IS NOT NULL)) THEN datediff(day,lead_created_date,first_sr_activity_date_lead) 
                         WHEN s.type = 'Contact' AND s.lead_id_l IS NULL THEN datediff(day,contact_created_date,first_sr_activity_date_contact) END AS full_create_to_activity_day
                
                ,datediff(minute,lead_created_date,contact_created_date) AS lead_to_contact_min
                ,datediff(hour,lead_created_date,contact_created_date) AS lead_to_contact_hour
                ,datediff(day,lead_created_date,contact_created_date) AS lead_to_contact_day
                
--                ,((datediff(minute,lead_created_date,assigned_date_time_c))/NULLIF((datediff(minute,lead_created_date,first_sr_activity_date_lead)),0)*100) AS percent_due_to_assignment
--                ,((datediff(minute,assigned_date_time_c,first_sr_activity_date_lead))/NULLIF((datediff(minute,lead_created_date,first_sr_activity_date_lead)),0)*100) AS percent_due_to_SR
        FROM cte_summary s
        LEFT JOIN work_revopt.salesforce_completed_activity sfca ON s.lead_or_contact_owner_id = sfca.person_id AND s.first_sr_activity_date_lead = sfca.completed_date_ts
        WHERE 1=1
                AND first_sr_activity_date_lead IS NOT NULL
                AND assigned_date_time_c IS NOT NULL
        


-------------------------------------------------------------------EXPLORATION-------------------------------------------------------------------

--campaign and campaign member exploration
        SELECT DISTINCT name
                ,type
                ,subtype_c
                ,SUM(number_of_leads) leads
                ,SUM(number_of_converted_leads) converted_leads
                ,SUM(number_of_contacts) contacts
                ,SUM(number_of_responses) responses
                ,SUM(number_of_opportunities) opportunities
                ,SUM(amount_all_opportunities) amount_opps
                ,SUM(amount_won_opportunities) amount_opps_won
        FROM prod_saj_share.salesforce.campaign
        WHERE 1=1
        GROUP BY name,type, subtype_c
        ORDER BY 8 DESC
        
        SELECT *
        FROM prod_saj_share.salesforce.lead
        WHERE is_converted = 'true' limit 100
        
        SELECT DISTINCT c.name, c.type, c. subtype_c, l.lead_source, l.id
        FROM prod_saj_share.salesforce.lead l
        LEFT JOIN prod_saj_share.salesforce.campaign c on l.campaign_source_c = c.id
        WHERE l.campaign_source_c = '7011a000000B4exAAC'
        
        SELECT COUNT(*)
        --        DISTINCT u.name
        --        ,l.*
        FROM prod_saj_share.salesforce.lead l
        LEFT JOIN prod_saj_share.salesforce.user u on l.owner_id = u.id
        --210002
        
        SELECT DISTINCT u.name
                ,l.owner_id
               ,COUNT(*)
--        FROM prod_saj_share.salesforce.contact c
        FROM prod_saj_share.salesforce.lead l
        LEFT JOIN prod_saj_share.salesforce.user u on l.owner_id = u.id
        WHERE u.name IN ('Salesforce Administrator','Snagajob Admin','SnapLogic Integration', 'Marketing Integration')
        GROUP BY u.name, l.owner_id
        --2

        SELECT *
        FROM prod_saj_share.salesforce.lead l
        LEFT JOIN prod_saj_share.salesforce.contact c on l.converted_contact_id = c.id
        WHERE 1=1
--        AND converted_date IS NOT NULL
--        AND is_converted = 'True' 
        AND converted_contact_id = '0031a00000c82cPAAQ'
        
        SELECT DISTINCT c.converted_date_c contact_converted_date
                ,l.converted_date lead_converted_date
                ,c.created_date contact_created_date
                ,c.id
                ,l.converted_contact_id
                ,l.*
                ,c.*
--                SELECT DISTINCT l.status
--                        ,COUNT(*)
        FROM prod_saj_share.salesforce.contact c
        LEFT JOIN prod_saj_share.salesforce.lead l on c.id = l.converted_contact_id
        WHERE l.is_converted = 'True'
--                AND c.converted_date_c IS NOT NULL
                AND c.created_date is null
        GROUP BY l.status
        LIMIT 100
        
        --lead source, status = qualified
        --is unread = false
        
        SELECT distinct assigned_c
                ,count(*)
        FROM prod_saj_share.salesforce.lead
--        where assigned_c = 'True'
        group by assigned_c
        
        SELECT DISTINCT assigned_c
                --,owner_id
                ,COUNT(*)
        FROM prod_saj_share.salesforce.lead
        WHERE 1=1
--                AND assigned_c = 'True'
                --AND assigned_date_time_c IS NULL
                AND owner_id <> '0051a0000019Te0AAE'
        GROUP BY assigned_c--, owner_id
      
--share with Ema!        
        SELECT DISTINCT owner_id
                ,u.name
                ,COUNT(*)
        FROM prod_saj_share.salesforce.lead l
        LEFT JOIN prod_saj_share.salesforce.user u on l.owner_id = u.id
        WHERE 1=1
                AND assigned_c = 'False'
                AND first_action_date_time_c IS NOT NULL
                AND owner_id NOT IN ('0051a000001Pt0vAAC'
                        ,'0051a000001ZopLAAS'
                        ,'0051a000001ZoMdAAK'
                        ,'0051a0000019Te0AAE')
        GROUP BY owner_id
                ,u.name

        SELECT DISTINCT FCRM_FCR_DATED_OPPORTUNITY_AMOUNT_C
                ,COUNT(*)
        FROM prod_saj_share.salesforce.campaign_member
        GROUP BY FCRM_FCR_DATED_OPPORTUNITY_AMOUNT_C


 select distinct assigned_c
                ,count(*)
        from prod_saj_share.salesforce.lead
        --WHERE assigned_date_time_c IS NULL
        group by assigned_c
        
        --confirm how accurate the is_converted flag is!!! (in lead object)
                SELECT l.id
                        ,c.id
                        ,l.is_converted
                        ,l.converted_account_id
                        ,l.converted_contact_id
                FROM prod_saj_share.salesforce.lead l
                JOIN prod_saj_share.salesforce.contact c ON c.id = l.converted_contact_id AND l.is_converted = 'True'
                WHERE is_converted = 'False'