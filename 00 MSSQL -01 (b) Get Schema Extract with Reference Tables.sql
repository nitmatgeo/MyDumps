SELECT
    @@SERVERNAME AS server_name,
    SCHEMA_NAME(tab.schema_id) AS schema_name,
    tab.name AS table_name,
    col.name AS column_name,
    t.name AS data_type,
    CASE 
        WHEN t.name IN ('char', 'varchar', 'nchar', 'nvarchar') THEN t.name + '(' + CASE WHEN col.max_length = -1 THEN 'max' ELSE CONVERT(VARCHAR(10), col.max_length) END + ')'
        WHEN t.name IN ('decimal', 'numeric') THEN t.name + '(' + CONVERT(VARCHAR(10), col.precision) + ',' + CONVERT(VARCHAR(10), col.scale) + ')'
        ELSE t.name
        END AS data_type_with_size,
    CASE WHEN col.is_nullable = 1 THEN 'YES' ELSE 'NO' END AS is_nullable,
    CASE WHEN idx.index_id IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_unique,
    GETDATE() AS last_accessed_timestamp,
    CASE WHEN tab.type = '1' THEN 'SYSTEM_TABLE'
         WHEN tab.type = 'U' THEN 'USER_TABLE'
         WHEN tab.type = 'V' THEN 'VIEW'
         ELSE tab.type + '- is UNKNOWN'
        END AS table_type,
    CASE WHEN pk.index_id IS NOT NULL THEN '<Primary Key>' ELSE SCHEMA_NAME(ref_tab.schema_id) END AS referenced_schema_name,
    CASE WHEN pk.index_id IS NOT NULL THEN '<Primary Key>' ELSE ref_tab.name END AS referenced_table_name,
    CASE WHEN pk.index_id IS NOT NULL THEN '<Primary Key>' ELSE ref_col.name END AS referenced_column_name
FROM sys.tables tab
INNER JOIN sys.columns col
    ON tab.object_id = col.object_id
INNER JOIN sys.types t 
    ON col.system_type_id = t.system_type_id 
    AND col.user_type_id = t.user_type_id
LEFT JOIN (
    SELECT object_id, index_id
    FROM sys.indexes
    WHERE is_primary_key = 1
) idx 
    ON tab.object_id = idx.object_id
    AND col.column_id IN (
        SELECT column_id 
        FROM sys.index_columns
        WHERE object_id = idx.object_id AND index_id = idx.index_id
    )
LEFT JOIN sys.foreign_key_columns fk
    ON col.object_id = fk.parent_object_id 
    AND col.column_id = fk.parent_column_id
LEFT JOIN sys.tables ref_tab
    ON fk.referenced_object_id = ref_tab.object_id
LEFT JOIN sys.columns ref_col
    ON fk.referenced_object_id = ref_col.object_id 
    AND fk.referenced_column_id = ref_col.column_id
LEFT JOIN (
    SELECT object_id, index_id
    FROM sys.indexes
    WHERE is_primary_key = 1
) pk
    ON tab.object_id = pk.object_id
    AND col.column_id IN (
        SELECT column_id 
        FROM sys.index_columns
        WHERE object_id = pk.object_id AND index_id = pk.index_id
    )
WHERE 1 = 1
    -- AND SCHEMA_NAME(tab.schema_id) IN ('')
    -- AND tab.name IN ('')
    -- AND col.name IN ('')
ORDER BY schema_name, table_name, col.column_id;