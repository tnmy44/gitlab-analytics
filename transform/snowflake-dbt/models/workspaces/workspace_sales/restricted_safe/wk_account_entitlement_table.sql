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
        u.profile_Id,
        u.user_role_Id,
        r.name AS role_Name,
        u.manager_Id,
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
    u.user_id, 
    u.user_name,
    u.profile_Id,
    a.account_Id,
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
    {{ ref('wk_prep_crm_account_share_active') }} a ON u.Id = a.user_Or_group_Id OR a.user_Or_group_Id IN (SELECT user_or_group_id FROM ExpandedGroupMembers)
LEFT JOIN 
    UserRolesHierarchiesTerritoriesProfiles urht ON u.user_id = urht.user_Id
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.user_name, a.account_Id ORDER BY a.account_access_Level DESC) = 1;