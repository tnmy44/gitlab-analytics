{{ config(materialized='table') }}

With ExpandedGroupMembers AS (
    -- Recursive CTE to expand group memberships
    SELECT 
        gm.user_or_group_id,
        gm.group_id
    FROM 
        {{ ref('sfdc_group_member_source') }} as gm
    WHERE 
        NOT EXISTS (SELECT 1 FROM {{ ref('sfdc_group_source') }} WHERE group_id = gm.user_or_group_id)
    UNION ALL
    SELECT 
        gm.user_or_group_id,
        egm.group_id
    FROM 
        {{ ref('sfdc_group_member_source') }} as gm
    JOIN 
        ExpandedGroupMembers as egm ON gm.group_id = egm.user_or_group_id
    WHERE 
        EXISTS (SELECT 1 FROM {{ ref('sfdc_group_source') }} WHERE group_id = gm.user_or_group_id)
),
UserRolesHierarchiesTerritoriesProfiles AS (
    -- CTE for user roles, hierarchies, territories, and profiles
    SELECT 
        u.user_id,
        u.Profile_Id,
        u.User_Role_Id,
        r.Name AS Role_Name,
        u.Manager_Id,
        ut.Territory2Id,
        p.is_permissions_view_all_data AS CanViewAllData,
        p.is_permissions_modify_all_data AS CanModifyAllData
    FROM 
        {{ ref('sfdc_users_source') }} as u
    LEFT JOIN 
        {{ ref('sfdc_user_roles_source') }} r ON u.User_Role_Id = r.Id
    LEFT JOIN 
         {{ ref('sfdc_profile_source') }} p ON u.Profile_Id = p.ID
    LEFT JOIN 
        raw.salesforce_v2_stitch.USERTERRITORY2ASSOCIATION ut ON u.Id = ut.UserId
    WHERE 
        u.Is_Active = true
)
SELECT 
    u.Id, 
    u.User_name,
    u.Profile_Id,
    a.Account_Id,
    -- Apply profile-based permissions to determine the effective access level
    CASE
        WHEN urht.CanModifyAllData = true THEN '1'
        WHEN urht.CanViewAllData = true THEN '1'
        ELSE a.AccountAccessLevel
    END AS EffectiveAccountAccessLevel,
    urht.Territory2Id
FROM 
    {{ ref('sfdc_users_source') }} as u
JOIN 
    {{ ref('wk_prep_crm_account_share_active') }} a ON u.Id = a.User_Or_Group_Id OR a.User_Or_Group_Id IN (SELECT USERORGROUPID FROM ExpandedGroupMembers)
LEFT JOIN 
    UserRolesHierarchiesTerritoriesProfiles urht ON u.Id = urht.User_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.User_name, a.Account_Id ORDER BY a.Account_Access_Level DESC) = 1;