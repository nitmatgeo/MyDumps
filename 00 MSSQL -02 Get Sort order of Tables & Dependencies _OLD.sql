/************************************************************************************************************************************
    Created By  :   Nitin
    Created On  :   SEP'24
    Version     :   1.3
    Description :   Identify the FK-PK dependencies iteratively across any table to idenfity the Loador Deletion  Order.
************************************************************************************************************************************/
-- Define the desired schema name and table names
DECLARE @SchemaName NVARCHAR(128) = 'Landing';
DECLARE @TableNames NVARCHAR(MAX) = NULL --'Organisation_Telephone_Type,Organisation_Type,Organisation,Organisation_Trading_Style'; -- Comma-separated list of table names

-- Convert the comma-separated list of table names into a table variable
DECLARE @TableList TABLE (TableName NVARCHAR(128));
IF @TableNames IS NOT NULL
BEGIN
    INSERT INTO @TableList (TableName)
    SELECT REPLACE(REPLACE(value, '[', ''), ']', '') FROM STRING_SPLIT(@TableNames, ',');
END

-- Step 1: Create a temporary table to store dependencies
IF OBJECT_ID('tempdb..#dependencies') IS NOT NULL DROP TABLE #dependencies;
SELECT DISTINCT
	FK.TABLE_SCHEMA COLLATE SQL_Latin1_General_CP1_CI_AS AS SchemaName,
    FK.TABLE_NAME COLLATE SQL_Latin1_General_CP1_CI_AS AS TableName,
    PK.TABLE_NAME COLLATE SQL_Latin1_General_CP1_CI_AS AS DependsOn
INTO #dependencies
FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS C
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS FK
	ON C.CONSTRAINT_NAME = FK.CONSTRAINT_NAME
INNER JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS PK 
	ON C.UNIQUE_CONSTRAINT_NAME = PK.CONSTRAINT_NAME
WHERE 1 = 1 
	AND FK.TABLE_SCHEMA = @SchemaName
    AND ((@TableNames IS NULL 
		OR FK.TABLE_NAME COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TableName COLLATE SQL_Latin1_General_CP1_CI_AS FROM @TableList))
	OR (@TableNames IS NULL 
		OR PK.TABLE_NAME COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TableName COLLATE SQL_Latin1_General_CP1_CI_AS FROM @TableList)));
 
--select * from #dependencies

-- Step 2: Create a temporary table to store the load and delete order
IF OBJECT_ID('tempdb..#order') IS NOT NULL DROP TABLE #order;
SELECT 
	SCHEMA_NAME(schema_id) COLLATE SQL_Latin1_General_CP1_CI_AS AS SchemaName,
    name COLLATE SQL_Latin1_General_CP1_CI_AS AS TableName,
    CAST('' AS NVARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS AS DependsOn,
    0 AS Level
INTO #order
FROM sys.objects
WHERE 1 = 1 
    AND type = 'U' -- Only user tables
	AND name COLLATE SQL_Latin1_General_CP1_CI_AS NOT IN (SELECT DISTINCT TableName FROM #dependencies)
	AND SCHEMA_NAME(schema_id) = @SchemaName
    AND (@TableNames IS NULL 
		OR name COLLATE SQL_Latin1_General_CP1_CI_AS IN (SELECT TableName COLLATE SQL_Latin1_General_CP1_CI_AS FROM @TableList));
--select * from #order

-- Step 4: Iteratively populate the load and delete order table based on dependencies
DECLARE @Level INT = 0;

WHILE EXISTS (SELECT 1 FROM #dependencies)
BEGIN
    SET @Level = @Level + 1;
	PRINT 'Iteration: ' + LTRIM(@Level)

    INSERT INTO #order (SchemaName, TableName, DependsOn, Level)
    SELECT 
		d.SchemaName,
        d.TableName,
        CAST(o.DependsOn + ' > ' + d.DependsOn AS NVARCHAR(MAX)) COLLATE SQL_Latin1_General_CP1_CI_AS AS DependsOn,
        @Level AS Level
    FROM #dependencies d
    LEFT JOIN #order o 
		ON d.DependsOn COLLATE SQL_Latin1_General_CP1_CI_AS = o.TableName COLLATE SQL_Latin1_General_CP1_CI_AS
    WHERE 1 = 1
        AND (o.Level = @Level - 1 OR d.TableName = d.DependsOn); -- Handle self-referencing tables

    DELETE FROM #dependencies
    WHERE TableName IN (SELECT TableName FROM #order WHERE Level = @Level);
END

-- Step 5: Select the final result
SELECT DISTINCT
    SCHEMA_NAME(o.schema_id) AS TableSchema,
    r.DependsOn,
    r.TableName,
    r.Level
FROM #order r
INNER JOIN sys.objects o 
	ON r.TableName COLLATE SQL_Latin1_General_CP1_CI_AS = o.name COLLATE SQL_Latin1_General_CP1_CI_AS
	AND SCHEMA_NAME(o.schema_id) = r.SchemaName
ORDER BY r.Level, r.TableName;

---- Clean up temporary tables
--DROP TABLE #dependencies;
--DROP TABLE #order;

--select * from  #dependencies;
--select * from #order where TableName = 'Responsible_Inspectorate_Type';