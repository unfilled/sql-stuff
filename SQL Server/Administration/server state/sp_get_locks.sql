IF OBJECT_ID ('sp_get_locks')  IS NULL
    EXEC ('CREATE PROCEDURE sp_get_locks AS SELECT 1 C;');
GO

ALTER PROCEDURE sp_get_locks (@session_id int)
AS
BEGIN

    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

    DECLARE @blocked_session_id int;
    DECLARE @blocked_count bigint;

    DECLARE @db_id INT;
    DECLARE @db_name sysname;

    DECLARE @tmplt nvarchar(max) = N'
    SELECT 
        dtl.request_mode,
        dtl.request_status,
        CASE dtl.resource_type 
            WHEN N''OBJECT'' 
                THEN OBJECT_NAME(dtl.resource_associated_entity_id, {dbid}) 
    	    ELSE OBJECT_NAME(p.object_id, {dbid}) 
    	END AS locked_object,
        dtl.resource_type,
        COUNT_BIG(*) AS total_locks
    FROM sys.dm_tran_locks AS dtl
    LEFT JOIN [{dbname}].sys.partitions AS p ON p.hobt_id = dtl.resource_associated_entity_id
    WHERE 
        dtl.request_session_id = {sessionid} AND 
        dtl.resource_type <> N''DATABASE''
    GROUP BY 
        CASE dtl.resource_type 
            WHEN N''OBJECT'' 
                THEN OBJECT_NAME(dtl.resource_associated_entity_id, {dbid}) 
    	    ELSE OBJECT_NAME(p.object_id, {dbid}) 
    	END, 
    	dtl.resource_type, 
        dtl.request_status,
    	dtl.request_mode
    ORDER BY 
        CASE 
            WHEN dtl.request_status IN (N''WAIT'', N''CONVERT'') 
                THEN 1
            ELSE 2 
        END, 
        locked_object,
        dtl.resource_type;';
    DECLARE @sql nvarchar(max);

    /* get random blocked spid and count of blocked spid, blocked by @session_id */
    SELECT @blocked_session_id = MIN(session_id), @blocked_count = COUNT_BIG(*)
    FROM sys.dm_exec_requests
    WHERE blocking_session_id = @session_id;

    /* get database_id and database_name from sys.dm_exec_sessions */
    SELECT @db_id = database_id, @db_name = name
    FROM sys.databases
    WHERE database_id = (SELECT database_id FROM sys.dm_exec_sessions WHERE session_id = @session_id);

    IF @blocked_session_id IS NOT NULL
        SELECT @session_id AS session_id, @blocked_count AS blocked_sessions_count, @blocked_session_id AS min_blocked_session_id;

    SET @sql = REPLACE(
                    REPLACE (
                        REPLACE(@tmplt, N'{dbid}',      CAST(@db_id AS nvarchar(50))),
                                        N'{dbname}',    @db_name), 
                                        N'{sessionid}', CAST(@session_id AS nvarchar(50)));
    EXEC (@sql);

    IF @blocked_session_id IS NOT NULL
    BEGIN
        SET @sql = REPLACE(
                        REPLACE (
                            REPLACE(@tmplt, N'{dbid}', CAST(@db_id AS nvarchar(50))),
                                            N'{dbname}', @db_name),
                                            N'{sessionid}', CAST(@blocked_session_id AS nvarchar(50)));
        
        EXEC (@sql);

    END;

END;

GO

EXEC sp_get_locks 76

--SELECT * FROM sys.dm_tran_locks where request_session_id = 76
