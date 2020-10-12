SELECT  
    s.name + '.' + o.name AS table_name,
    i.name AS index_name,
    ius.user_seeks,
    ius.user_scans,
    ius.user_lookups,
    ius.user_updates    
FROM sys.dm_db_index_usage_stats ius
JOIN sys.indexes i ON ius.index_id = i.index_id AND ius.object_id = i.object_id
JOIN sys.objects o ON i.object_id = o.object_id
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE ius.database_id = DB_ID();