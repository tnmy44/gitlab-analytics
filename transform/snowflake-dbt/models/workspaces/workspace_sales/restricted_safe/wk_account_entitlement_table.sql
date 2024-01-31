{{ config(materialized='view') }}

With ExpandedGroupMembers AS (
    -- Recursive CTE to expand group memberships
    SELECT 
        gm.USERORGROUPID,
        gm.GROUPID
    FROM 
        {{ ref('sfdc_group_member_source') }} as gm
    WHERE 
        NOT EXISTS (SELECT 1 FROM {{ ref('sfdc_group_source') }} WHERE id = gm.USERORGROUPID)
    UNION ALL
    SELECT 
        gm.USERORGROUPID,
        egm.GROUPID
    FROM 
        {{ ref('sfdc_group_member_source') }} as gm
    JOIN 
        ExpandedGroupMembers egm ON gm.GROUPID = egm.USERORGROUPID
    WHERE 
        EXISTS (SELECT 1 FROM {{ ref('sfdc_group_source') }} WHERE ID = gm.USERORGROUPID)
),
UserRolesHierarchiesTerritoriesProfiles AS (
    -- CTE for user roles, hierarchies, territories, and profiles
    SELECT 
        u.Id AS UserId,
        u.ProfileId,
        u.UserRoleId,
        r.Name AS RoleName,
        u.ManagerId,
        ut.Territory2Id,
        p.PERMISSIONSVIEWALLDATA AS CanViewAllData,
        p.PERMISSIONSMODIFYALLDATA AS CanModifyAllData
    FROM 
        {{ ref('sfdc_users_source') }} as u
    LEFT JOIN 
        {{ ref('sfdc_user_roles_source') }} r ON u.UserRoleId = r.Id
    LEFT JOIN 
         {{ ref('sfdc_profile_source') }} p ON u.ProfileId = p.ID
    LEFT JOIN 
        raw.salesforce_v2_stitch.USERTERRITORY2ASSOCIATION ut ON u.Id = ut.UserId
    WHERE 
        u.IsActive = true
)
SELECT 
    u.Id, 
    u.Username,
    u.ProfileId,
    a.AccountId,
    -- Apply profile-based permissions to determine the effective access level
    CASE
        WHEN urht.CanModifyAllData = true THEN '1'
        WHEN urht.CanViewAllData = true THEN '1'
        ELSE a.AccountAccessLevel
    END AS EffectiveAccountAccessLevel,
    urht.Territory2Id
FROM 
    raw.salesforce_v2_stitch.USER u
JOIN 
    accountshare a ON u.Id = a.UserOrGroupId OR a.UserOrGroupId IN (SELECT USERORGROUPID FROM ExpandedGroupMembers)
LEFT JOIN 
    UserRolesHierarchiesTerritoriesProfiles urht ON u.Id = urht.UserId
QUALIFY ROW_NUMBER() OVER (PARTITION BY u.Username, a.AccountId ORDER BY a.AccountAccessLevel DESC) = 1;