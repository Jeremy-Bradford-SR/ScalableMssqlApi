-- Create Unified Events View
CREATE OR ALTER VIEW vw_AllEvents AS
SELECT 
    'AR' as SourceType,
    id as SourceId,
    event_time as EventTime,
    location as Location,
    charge as Description,
    name as PrimaryPerson,
    lat,
    lon
FROM dbo.DailyBulletinArrests
WHERE [key] = 'AR'

UNION ALL

SELECT 
    'CAD' as SourceType,
    CAST(id AS VARCHAR(50)) as SourceId,
    starttime as EventTime,
    address as Location,
    nature as Description,
    NULL as PrimaryPerson,
    lat,
    lon
FROM dbo.cadHandler;

GO

-- Full Text Search Setup (Idempotent)
IF NOT EXISTS (SELECT * FROM sys.fulltext_catalogs WHERE name = 'P2CCatalog')
BEGIN
    CREATE FULLTEXT CATALOG P2CCatalog AS DEFAULT;
END

-- Note: FTS requires unique index on key column. Assuming 'id' and 'IncidentNumber' are PKs.
-- Skipping FTS creation in script as it requires knowing exact PK names and might fail if they don't exist.

-- Create Violators with Jail Info View
CREATE OR ALTER VIEW vw_ViolatorsWithJailInfo AS
SELECT
    A.name AS ArrestRecordName,
    A.firstname AS FirstName,
    A.lastname AS LastName,
    A.charge AS ArrestCharge,
    A.event_time AS ArrestDate,
    A.location AS ArrestLocation,
    
    -- Offender Summary Info
    S.OffenderNumber AS DocOffenderNumber,
    S.Gender AS DocGender,
    S.Age AS DocAge,
    
    -- Aggregated Offenses (from Offender_Detail)
    (SELECT 
        STRING_AGG(T3.Offense, ', ') WITHIN GROUP (ORDER BY T3.Offense)
     FROM 
        dbo.Offender_Summary AS T1
     JOIN 
        (SELECT DISTINCT OffenderNumber, Offense FROM dbo.Offender_Detail WHERE Offense IS NOT NULL) AS T3 
        ON T1.OffenderNumber = T3.OffenderNumber
     WHERE 
        T1.Name = S.Name
    ) AS OriginalOffenses,

    -- Jail Info (Subquery to get latest booking to avoid duplicates or left join on current)
    -- Using OUTER APPLY to get the MOST RECENT jail booking for this person
    J.book_id AS JailBookId,
    J.arrest_date AS JailArrestDate,
    J.released_date AS JailReleasedDate,
    J.total_bond_amount AS JailBondAmount,
    (SELECT STRING_AGG(charge_description, ', ') FROM jail_charges WHERE book_id = J.book_id) AS JailCharges

FROM
    dbo.DailyBulletinArrests AS A
INNER JOIN
    dbo.Offender_Summary AS S ON S.Name = CONCAT_WS(' ', A.firstname, A.middlename, A.lastname)
OUTER APPLY (
    SELECT TOP 1 *
    FROM dbo.jail_inmates AS Ji
    WHERE Ji.firstname = A.firstname AND Ji.lastname = A.lastname
    ORDER BY Ji.arrest_date DESC
) AS J;
GO
