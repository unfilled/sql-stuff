--overall execution statisctics
SELECT TOP 100
    d.name AS database_name,
    st.text AS query_text,
    qp.query_plan,
    execution_count,             
    (total_worker_time - max_worker_time - min_worker_time)   --exclude extremums
        / 1000 
        / NULLIF((execution_count - 2), 0) 
    AS avg_time_ms,
    min_worker_time / 1000 AS min_time_ms,
    max_worker_time / 1000 AS max_time_ms,
    total_worker_time / 1000 AS total_time_ms,     
    (total_logical_reads - min_logical_reads - max_logical_reads)   --exclude extremums 
        / NULLIF((execution_count - 2), 0) AS avg_logical_reads,
    min_logical_reads,
    max_logical_reads,
    total_logical_reads,
    CAST (total_grant_MB / 1.0 / execution_count AS decimal (18, 4)) AS avg_grant_MB,
    min_grant_MB,
    max_grant_MB,
    total_spills,
    --in case we have different plans in cache for this query, there is a query to get them all
    --in other case we have nothing here
    CASE WHEN CONVERT (nvarchar(max), plan_handle, 2) = CONVERT (nvarchar(max), other_plan, 2) THEN N'there is only one plan in cache, you see everything'
    ELSE 
        N'
    SELECT 
        st.text, 
        qp.query_plan, 
        qs.execution_count, 
        qs.total_worker_time / 1000 AS total_worker_time,
        qs.total_logical_reads,
        qs.total_grant_kb / 1024 AS total_grant_MB,
        qs.total_spills
    FROM sys.dm_exec_query_stats qs
    OUTER APPLY sys.dm_exec_sql_text (qs.sql_handle) st
    OUTER APPLY sys.dm_exec_query_plan (qs.plan_handle) qp
    WHERE qs.query_hash = 0x' + CONVERT (nvarchar(max), query_hash, 2) 
    END AS diagnosis_query
FROM (
    SELECT 
        SUM (qs.execution_count) AS execution_count,
        SUM (qs.total_worker_time) AS total_worker_time,
        MAX (qs.max_worker_time) AS max_worker_time,
        MIN (qs.min_worker_time) AS min_worker_time,
        SUM (qs.total_logical_reads) AS total_logical_reads,
        MIN (qs.min_logical_reads) AS min_logical_reads,
        MAX (qs.max_logical_reads) AS max_logical_reads,
        CAST (SUM(qs.total_grant_kb) / 1024.0 AS decimal (18, 4)) AS total_grant_MB,
        CAST (MIN(qs.min_grant_kb) / 1024.0 AS decimal (18, 4)) AS min_grant_MB,
        CAST (MAX(qs.max_grant_kb) / 1024.0 AS decimal (18, 4)) AS max_grant_MB,
        SUM(qs.total_spills) AS total_spills,        
        qs.query_hash,
        qs.sql_handle AS sql_handle,            --i need to test it, maybe there is i need random handle too
        MAX (qs.plan_handle) AS plan_handle,    --take a random plan, just in case.. 
        MIN (qs.plan_handle) AS other_plan
    FROM sys.dm_exec_query_stats qs
    GROUP BY qs.query_hash, qs.sql_handle
) stat
OUTER APPLY sys.dm_exec_sql_text (stat.sql_handle) st
OUTER APPLY sys.dm_exec_query_plan (stat.plan_handle) qp
LEFT JOIN sys.databases d ON d.database_id = COALESCE (st.dbid, qp.dbid)
ORDER BY stat.execution_count DESC



