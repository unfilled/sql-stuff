USE devops;
GO

IF OBJECT_ID ('sp_object_sizes') IS NULL
    EXEC ('CREATE PROCEDURE sp_object_sizes AS SELECT 1 AS C;');

GO

ALTER PROCEDURE sp_object_sizes
    @dbname     varchar(max)    = 'all',
    @indexes    bit             = 0
AS 
BEGIN

DECLARE @dbs AS TABLE (name sysname);
DECLARE @sql AS nvarchar(max) = N'';
DECLARE @template AS nvarchar(max) = N'
SELECT
    ''[db]'' COLLATE Cyrillic_General_CI_AS AS database_name,
    s.Name + N''.'' + t.NAME COLLATE Cyrillic_General_CI_AS AS table_name,
    i.Name COLLATE Cyrillic_General_CI_AS AS index_name,
    p.rows,
    CAST(SUM(a.total_pages) * 8.0 / NULLIF(p.rows, 0) AS NUMERIC(36, 2)) AS avg_KB_per_row,
    CAST(ROUND(((SUM(a.total_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS total_MB,
    CAST(ROUND(((SUM(a.used_pages) * 8) / 1024.00), 2) AS NUMERIC(36, 2)) AS used_MB
FROM [db].sys.tables t
INNER JOIN [db].sys.indexes i 
    ON t.OBJECT_ID = i.object_id
INNER JOIN [db].sys.partitions p 
    ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
INNER JOIN [db].sys.allocation_units a 
    ON p.partition_id = a.container_id
LEFT JOIN [db].sys.schemas s 
    ON t.schema_id = s.schema_id
WHERE
    t.is_ms_shipped = 0
    AND i.object_id > 255
GROUP BY
    t.Name, s.Name, p.Rows, i.Name
';

IF @indexes = 0
    SELECT @template = REPLACE(REPLACE(@template, 'i.Name COLLATE Cyrillic_General_CI_AS AS index_name,', ''), ', i.Name', '')

IF @dbname = 'all'
BEGIN
    INSERT INTO @dbs (name)
    SELECT name
    FROM sys.databases
    WHERE database_id > 4;
END;
ELSE 
BEGIN
    SELECT @dbname = '[' + REPLACE(REPLACE(@dbname, ' ', ''), ',', '],[') + ']';

    INSERT INTO @dbs (name)
    SELECT name
    FROM sys.databases
    WHERE CHARINDEX(QUOTENAME(name), @dbname) > 0;

END;

DECLARE @name AS sysname;

DECLARE cur CURSOR
FOR SELECT name FROM @dbs;

OPEN cur;

FETCH NEXT FROM cur INTO @name
WHILE @@FETCH_STATUS = 0 
BEGIN
    IF LEN(@sql)>0
        SET @sql += N'
        UNION ALL
        ';

    SET @sql += REPLACE(@template, N'[db]', @name);

    FETCH NEXT FROM cur INTO @name;
END

CLOSE cur;
DEALLOCATE cur;

SET @sql += N'
ORDER BY 
    database_name ASC, total_MB DESC;
';

EXEC sp_executesql @sql;
    
END;

GO

EXEC sp_object_sizes  @dbname = 'stackoverflow2013, devops,    aaa, chartest', @indexes = 1