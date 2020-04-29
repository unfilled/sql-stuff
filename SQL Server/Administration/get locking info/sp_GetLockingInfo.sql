IF OBJECT_ID('sp_GetLockingInfo', 'P') IS NOT NULL
    DROP PROC sp_GetLockingInfo;
GO

--TODO: добавить везде MAXDOP 1 V
--TODO: сделать возможным запуск процедуры из нескольких сессий? Либо переход на локальные вр. таблицы, либо добавить executor_spid в snapshot
--TODO: Добавить sys.dm_os_waiting_tasks
--TODO: Добавить информацию о потреблённых ресурсах и ожиданиях в детальном отчёте

--TODO: blocking_chains + новый параметр V
--TODO: по blocking_session_id можно построить, кроме цепочки блокировок, очередь, чтобы сразу видеть кто в каком порядке будет выполняться
--возможно - этоо не имеет смысла, другие сессии, стоящие в очереди позже, могут иметь блокировки, которые потребуются тем. кто в очереди раньше
--такая штука имеет смысл в разрезе объекта, да и то не факт ---

--TODO: добавить информацию о схеме, которой принадлежит объект



--TODO: в параметры стоит добавить имя объекта?
CREATE PROCEDURE sp_GetLockingInfo (            --Если параметр NULL, отбор не используется
      @db_name nvarchar(200)    = NULL          --имя БД, в которой анализируются блокировки
    , @db_id int                = NULL          --ИЛИ её database_id
    , @SPID int                 = NULL          --session_id, информация по которому будет выводиться
    , @login nvarchar(200)      = NULL          --ИЛИ логин
    , @clear_data bit           = 1             --Если 1, перед окончанием выполнения, временные таблиц будут удалены, если 0 - останутся
    , @refill_data bit          = 1             --Если 0, при наличии данных во временных таблицах, они не будут перезаполняться, если 1 - в любом случае будут
    , @mode nvarchar(200)       = N'OVERVIEW'   --'DETAILED' - вывод всех резалтсетов, 'OVERVIEW' - Только "суммарных" по БД и сессиям
    , @system_dbs_info bit      = 0             --Включать в snapshot блокировки в системных БД? 1 - да, 0 - нет
    , @system_spids_info bit    = 0             --Включать в snapshot блокировки системных процессов? 1 - да, 0 - нет
    , @check_my_locks bit       = 0             --учитывать блокировки, наложенные сессией, запускающей процедуру
    , @use_blocking_SPID bit    = 1             --1 - при выводе 1-го резалтсета, учитывается blocking_session_id; 0 - учитывается только заблокированный ресурс (в качестве блокирующих показываются все сессии с granted блокировкой на нужном ресурсе)
    , @blocking_chains bit      = 0             --показывать цепочки блокировок (может выполняться медленно) 
)
AS    
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;    
    
    DECLARE @dbg_info AS nvarchar(255) = N'Started at: ' + CONVERT(nvarchar(50), CURRENT_TIMESTAMP, 120);
    RAISERROR (@dbg_info, 1, 1) WITH NOWAIT;

    DECLARE @sql AS nvarchar(max);      --переменная для dynamic sql
    
    --Проверка параметров
    -------------------------------------------------------------------------------------------------------------------
    
    --имя/идентификатор БД
    IF NOT EXISTS (
                    SELECT 1/0 
                    FROM sys.databases 
                    WHERE ([name] = @db_name OR @db_name IS NULL)
                        AND (database_id = @db_id OR @db_id IS NULL)
                    )
        BEGIN
            RAISERROR ('Некорректно заполнены имя/идентификатор БД', 16, 1) WITH NOWAIT;
            RETURN;
        END

    IF @db_id IS NOT NULL
        SET @db_name = (SELECT TOP 1 name FROM sys.databases WHERE database_id = @db_id)
    
    --режим просмотра
    IF @mode NOT IN (N'DETAILED', N'OVERVIEW')
        BEGIN
            RAISERROR ('Некорректно заполнен параметр @mode, будет использовано значение OVERVIEW', 16, 1) WITH NOWAIT;
            SET @mode = N'OVERVIEW';
        END

    --требуется очистка "snapshot" блокировок?
    IF @refill_data = 1
        BEGIN
            PRINT ('"snapshot" блокировок будет обновлён');
            
            IF OBJECT_ID('tempdb..##locks_snapshot', 'U') IS NOT NULL
                DROP TABLE ##locks_snapshot;
        
        END

    PRINT @mode;

    -------------------------------------------------------------------------------------------------------------------

    --Если временных таблиц нет, нужно создать
    -------------------------------------------------------------------------------------------------------------------

    IF OBJECT_ID('tempdb..##locks_snapshot', 'U') IS NULL
        BEGIN

            --таблица со snapshot'ом всех блокировок
            CREATE TABLE ##locks_snapshot (
                [db_name]                   sysname
                , resource_type             nvarchar(60)
                , resource_description      nvarchar(60)
                , hobt_id                   bigint
                , [object_name]             nvarchar(128)
                , request_type              nvarchar(60)
                , request_status            nvarchar(60)
                , request_mode              nvarchar(60)
                , session_id                smallint
                , login_name                nvarchar(128)
                , [host_name]               nvarchar(128)
                , [status]                  nvarchar(30)
                , open_transaction_count    int
                , blocking_session_id       smallint
                , transaction_il            smallint
                , [text]                    nvarchar(MAX)
            );

            CREATE CLUSTERED INDEX #locks_snapshot_clustered ON ##locks_snapshot (db_name, hobt_id);

            CREATE NONCLUSTERED INDEX #locks_snapshot_object_name ON ##locks_snapshot (object_name);

        END

    --таблица для временного хранения идентификаторов объектов, для которых сразу не получилось узнать имя
    IF OBJECT_ID('tempdb..##orphaned_objects', 'U') IS NOT NULL
        DROP TABLE ##orphaned_objects;
        
    CREATE TABLE ##orphaned_objects (
         db_name        sysname
         , hobt_id      bigint
         , is_object    bit
    );

    --таблица для хранения информации об ожиданиях на блокировках - тут конкретные ресурсы, 
    --которые кем-то заблокированы и кому-то нужны при этом
    IF OBJECT_ID('tempdb..##locked_objects', 'U') IS NOT NULL
        DROP TABLE ##locked_objects;

    CREATE TABLE ##locked_objects (
        db_name                     sysname
        , resource_type             nvarchar(60)
        , locked_object_name        nvarchar(128)
        , [resource]                nvarchar(60)
        , lock_mode                 nvarchar(60)
        , lock_status               nvarchar(60)
        , granted_SPID              smallint
        , [login]                   nvarchar(128)
        , [host]                    nvarchar(128)
        , locking_query             nvarchar(max)
        , locking_session_status    nvarchar(30)
        , tran_cnt                  int
        , waiting_lock_mode         nvarchar(60)
        , waiting_SPID              smallint
        , waiting_login             nvarchar(128)
        , waiting_host              nvarchar(128)
        , waiter_blocking_session   smallint
        , waiting_query             nvarchar(max)
        , waiter_tran_cnt           int
    );

    --таблица для поиска цепочек блокировок

    IF OBJECT_ID('tempdb..##chains', 'U') IS NOT NULL 
        DROP TABLE ##chains;
    
    IF @blocking_chains = 1
        CREATE TABLE ##chains (
            session_id              smallint, 
            blocking_session_id     smallint,
        );

    -------------------------------------------------------------------------------------------------------------------
    --если snapshot Блокировок пуст, он будет заполнен вне зависимости от значения @refill_data
    IF @refill_data = 0 AND NOT EXISTS (SELECT 1/0 FROM ##locks_snapshot) 
        BEGIN
            PRINT '@refill_data установлен в 1, поскольку snapshot блокировок пуст';
            SET @refill_data = 1;
        END

    --если @refill_data = 0, работа продолжается с существующим срезом, иначе - нужен новый snapshot блокировок
    IF @refill_data = 1 
        INSERT INTO ##locks_snapshot (
                    [db_name]                    
                    , resource_type                
                    , resource_description        
                    , hobt_id                    
                    , [object_name]                
                    , request_type                
                    , request_status            
                    , request_mode                
                    , session_id                
                    , login_name                
                    , [host_name]                
                    , [status]                    
                    , open_transaction_count
                    , blocking_session_id
                    , transaction_il
                    , [text]                    
        )
        SELECT 
            DB_NAME (dtl.resource_database_id) AS [db_name]
            , dtl.resource_type
            , dtl.resource_description
            , dtl.resource_associated_entity_id AS hobt_id
            , CASE 
                WHEN dtl.resource_type IN (N'DATABASE', N'METADATA', N'FILE') THEN dtl.resource_type
                WHEN dtl.resource_type = N'OBJECT' THEN OBJECT_NAME(dtl.resource_associated_entity_id, dtl.resource_database_id)
                WHEN dtl.resource_type IN (N'EXTENT', N'HOBT', N'KEY', N'PAGE', N'RID') THEN 
                    CASE 
                        WHEN dtl.resource_database_id <> DB_ID() THEN NULL  --если блокировка не в текущей БД, получим имя объекта позже
                        ELSE                                                --если в этой - получаем имя объекта ч\з sys.partitions
                        OBJECT_NAME (
                            (
                                SELECT object_id
                                FROM sys.partitions p --эта штука зависит от БД
                                WHERE p.hobt_id = dtl.resource_associated_entity_id
                            )            
                            , dtl.resource_database_id
                        ) END
                ELSE N'unknown' END AS [object_name]
            , dtl.request_type
            , dtl.request_status
            , dtl.request_mode
            , s.session_id
            , s.login_name
            , s.host_name
            , s.status
            , s.open_transaction_count
            , r.blocking_session_id
            , COALESCE(r.transaction_isolation_level, s.transaction_isolation_level)    --если нет возможности получить из реквеста, взять из сессии
            , est.text
        FROM sys.dm_tran_locks dtl
        LEFT JOIN sys.dm_exec_sessions s 
            ON dtl.request_session_id = s.session_id
        LEFT JOIN sys.dm_exec_connections c
            ON s.session_id = c.session_id
        LEFT JOIN sys.dm_exec_requests r
            ON s.session_id = r.session_id
        OUTER APPLY sys.dm_exec_sql_text (c.most_recent_sql_handle) est
        WHERE dtl.resource_type <> N'DATABASE'
            AND dtl.resource_database_id > CASE WHEN @system_dbs_info = 1 THEN 0 ELSE 4    END  --только пользовательские БД? задаётся параметром
            AND s.is_user_process >= CASE WHEN @system_spids_info = 1 THEN 0 ELSE 1 END         --и только пользовательские соединения? задаётся параметром
            AND dtl.request_session_id <> CASE WHEN @check_my_locks = 0 THEN @@SPID ELSE 0 END  --учитывать блокировки, наложенные сессией в которой запускается ХП? задаётся параметром
        OPTION (MAXDOP 1);

    --выделенная часть ниже использует dynamic sql для определения заблокированных объектов во всех БД
    --если для определения требуется обращение к sys.partitions, то нужен dynamic sql, 
    --поскольку его содержимое зависит от БД, в контексте которой выполняется запрос
    -------------------------------------------------------------------------------------------------------
    --список объектов во всех БД, для которых не удалось определить имя объекта
    INSERT INTO ##orphaned_objects (db_name, hobt_id, is_object)
    SELECT DISTINCT db_name, hobt_id, CASE WHEN resource_type = N'OBJECT' THEN 1 ELSE 0 END AS is_object    
    FROM ##locks_snapshot
    WHERE object_name IS NULL
    OPTION (MAXDOP 1);

    DECLARE @current_db_name AS sysname;
    DECLARE @hobt_id AS bigint;
    DECLARE @is_object AS bit;

    --DECLARE @cmd AS nvarchar(max);


    DECLARE dbhobt_cursor CURSOR FOR
        SELECT db_name, hobt_id, is_object
        FROM ##orphaned_objects;

    OPEN dbhobt_cursor

    FETCH NEXT FROM dbhobt_cursor INTO @current_db_name, @hobt_id, @is_object;

    WHILE @@FETCH_STATUS = 0
        BEGIN

        SET @sql = N'
            UPDATE ##locks_snapshot
            SET [object_name] = OBJECT_NAME (
            CASE WHEN ' + CAST(@is_object AS nvarchar(1)) + N' = 1 THEN ' + CAST(@hobt_id AS nvarchar(50)) + N' ELSE
            (SELECT object_id FROM ' + @current_db_name + N'.sys.partitions WHERE hobt_id = ' + CAST(@hobt_id AS nvarchar(50)) + N') END
            , DB_ID(N''' + @current_db_name + N'''))
            WHERE [object_name] IS NULL AND [db_name] = ''' + @current_db_name + N''' AND hobt_id = ' + CAST(@hobt_id AS nvarchar(50))
            + N' OPTION (MAXDOP 1)';

        --PRINT @sql;

        BEGIN TRY
            --Если, например, не хватает прав, свалится в исключение
            EXEC sp_executesql @sql;

        END TRY
        BEGIN CATCH
            --текст сообщения об ошибке будет на вкладке Messages
            PRINT error_message();
        
            --если не обновилось, убирает NULL, чтобы все необновлённые строки не сгруппировались в одну
            --TODO: заменить CASE на resource_type + N':' + ... ??? Сейчас риды, страницы, ключи имеют одинаковое имя и группируются, а так грануляция будет выше
            UPDATE ##locks_snapshot
            SET [object_name] = CASE WHEN resource_type = N'OBJECT' THEN N'OBJECT: ' ELSE N'HOBT: ' END + CAST(@hobt_id AS nvarchar(50))
            WHERE [object_name] IS NULL AND [db_name] = @current_db_name AND hobt_id = @hobt_id
            OPTION (MAXDOP 1);

        END CATCH

        FETCH NEXT FROM dbhobt_cursor INTO @current_db_name, @hobt_id, @is_object
        END;

    CLOSE dbhobt_cursor;

    DEALLOCATE dbhobt_cursor;

    DROP TABLE ##orphaned_objects;

    -------------------------------------------------------------------------------------------------------

    --snapshot готов, заполнен по-максимуму, теперь можно получать из него нужные данные

    --в ##locked_objects сохраняются данные об актуальных ожиданиях на блокировках:
    --только ожидания на блокировках - конкретные ресурсы, запросы, которые ждут, и последние запросы в сессиях, которые заблокировали ресурс
    
    --##locked_objects перезаполняется при каждом запуске, вне зависимости от @refill_data, 
    --поскольку при разных вызовах ХП могут использоваться разные параметры @db_name/@db_id/@SPID/@login, 
    --а ##locked_objects заполняется с учётом этих параметров    
    SET @sql = N'    
    INSERT INTO ##locked_objects (
            db_name                        
            , resource_type                
            , locked_object_name        
            , [resource]                
            , lock_mode 
            , lock_status
            , granted_SPID                
            , [login]                    
            , [host]                    
            , locking_query                
            , locking_session_status    
            , tran_cnt                    
            , waiting_lock_mode            
            , waiting_SPID                
            , waiting_login                
            , waiting_host
            , waiter_blocking_session
            , waiting_query                
            , waiter_tran_cnt            
        )
    SELECT grantee.db_name 
        , grantee.resource_type
        , grantee.object_name AS locked_object_name
        , grantee.resource_description AS [resource]
        , grantee.request_mode AS lock_mode
        , grantee.request_status AS lock_status
        , grantee.session_id AS granted_SPID
        , grantee.login_name AS [login]
        , grantee.host_name AS [host]
        , grantee.text AS locking_query
        , grantee.status AS locking_session_status
        , grantee.open_transaction_count AS tran_cnt
        , waiter.request_mode AS waiting_lock_mode
        , waiter.session_id AS waiting_SPID
        , waiter.login_name as waiting_login
        , waiter.host_name AS waiting_host
        , waiter.blocking_session_id
        , waiter.text AS waiting_query
        , waiter.open_transaction_count AS waiter_tran_cnt
    FROM ##locks_snapshot grantee
    JOIN ##locks_snapshot waiter '
    + CASE WHEN @use_blocking_SPID = 0 
        THEN
        N'ON grantee.db_name = waiter.db_name AND grantee.hobt_id = waiter.hobt_id AND grantee.object_name = waiter.object_name
            AND grantee.resource_description = waiter.resource_description AND grantee.session_id <> waiter.session_id
            AND grantee.request_status = N''GRANT'' AND grantee.request_status <> waiter.request_status '
        ELSE 
        N' ON grantee.session_id = waiter.blocking_session_id '
    END + 
    N'WHERE 1=1 AND '
    + CASE WHEN @db_name IS NULL THEN N' 1=1 ' ELSE N' grantee.db_name = ' + @db_name END + N' AND '
    + CASE 
        WHEN @SPID IS NULL THEN N' 1=1 ' 
        ELSE N' grantee.session_id = ' + CAST(@SPID as nvarchar(10)) 
            + N' OR waiter.session_id =  ' + CAST(@SPID as nvarchar(10))
    END + N' AND '
    + CASE 
        WHEN @login IS NULL THEN N' 1=1 '
        ELSE N' grantee.login_name = ' + @login + N' OR waiter.login_name = ' + @login 
    END + N' OPTION (MAXDOP 1);';
    


    --print @sql
    exec (@sql);


    --вывод данных
    --------------------------------------------------------------------------------------------------------------------------------

    --OVERVIEW-mode
    -----------------------------------------------------------------------------------------------

    --Вывод данных об ожиданиях на блокировке, отсортированных по:
    --1) имени БД;
    --2) количеству текущих ожиданий на блокировках у сессии (чем больше блокировок не может наложить сессия, тем выше в списке)
    --3) имени объекта
    --4) "структуре" ожиданий: объект целиком - экстент - страница - всё остальное
    
    --чтобы корректно учесть @use_blocking_SPID, используется dynamic sql
    DECLARE @lo_cmd AS nvarchar(max);
    SET @lo_cmd = N'
    SELECT 
        db_name                        
        , resource_type                
        , locked_object_name        
        , [resource]                
        , lock_mode  
        , lock_status
        , granted_SPID                
        , [login]                    
        , [host]                    
        , locking_query                
        , locking_session_status    
        , tran_cnt                    
        , waiting_lock_mode            
        , waiting_SPID                
        , waiting_login                
        , waiting_host
        , waiter_blocking_session
        , waiting_query                
        , waiter_tran_cnt  
    FROM ##locked_objects '
    + CASE WHEN @use_blocking_SPID <> 0 THEN N'WHERE waiter_blocking_session = granted_SPID ' ELSE N'' END +
    N'ORDER BY db_name
        , COUNT(*) OVER(PARTITION BY waiting_SPID) DESC
        , locked_object_name
        , CASE 
            WHEN resource_type = N''OBJECT'' THEN 1 
            WHEN resource_type = N''EXTENT'' THEN 2
            WHEN resource_type = N''PAGE'' THEN 3
            ELSE 4 END
        , granted_SPID
    OPTION (MAXDOP 1)';

    exec (@lo_cmd);


    --TODO: переделать, возможно, стоит убрать фильтр по БД, или каким-то образом выделять блокировки в выбранной БД
    --Вывод сгруппированных данных о сессиях и их блокировках 
    --учитываются только указанные в параметрах: БД, логин, сессия
    --
    SELECT session_id
        , login_name
        , host_name
        , CASE WHEN granted_locks > 0 AND status = N'SLEEPING' THEN UPPER(status) ELSE status END AS status
        , open_transaction_count
        , CASE 
            WHEN transaction_il = 0 THEN 'Unspecified'
            WHEN transaction_il = 1 THEN 'Read Uncommitted'
            WHEN transaction_il = 2 
                AND EXISTS (SELECT 1/0 FROM sys.dm_tran_active_snapshot_database_transactions asdt WHERE asdt.session_id = t.session_id) 
                THEN 'Read Committed Snapshot'
            WHEN transaction_il = 2 
                AND NOT EXISTS (SELECT 1/0 FROM sys.dm_tran_active_snapshot_database_transactions asdt WHERE asdt.session_id = t.session_id) 
                THEN 'Read Committed Snapshot'
            WHEN transaction_il = 3 THEN 'Repeatable Read'
            WHEN transaction_il = 4 THEN 'Serializable'
            WHEN transaction_il = 5 THEN 'Snapshot'
            ELSE 'UNKNOWN'
        END AS isolation_level
        , text
        , locks_in_dbs
        , granted_locks
        , waiting_locks
        , objects_with_granted_locks
        , objects_with_waiting_locks
        , nonintent_objects_granted_locks
        , CASE WHEN EXISTS (SELECT 1/0 FROM ##locked_objects lo where lo.granted_SPID = t.session_id) THEN 'V' ELSE '' END AS blocker_mark
    FROM
    (
        SELECT ls.session_id
            , ls.login_name
            , ls.host_name
            , ls.status
            , ls.open_transaction_count
            , ls.blocking_session_id
            , ls.transaction_il
            , ls.text    
            , COUNT(DISTINCT db_name) AS locks_in_dbs
            , SUM(CASE WHEN ls.request_status = N'GRANT' THEN 1 ELSE 0 END) AS granted_locks
            , SUM(CASE WHEN ls.request_status <> N'GRANT' THEN 1 ELSE 0 END) AS waiting_locks
            , COUNT(DISTINCT CASE 
                                WHEN ls.request_status = N'GRANT' 
                                    THEN ls.object_name 
                                    ELSE NULL 
                                END) AS objects_with_granted_locks
            , COUNT(DISTINCT CASE 
                                WHEN ls.request_status <> N'GRANT' 
                                    THEN ls.object_name 
                                    ELSE NULL 
                                END) AS objects_with_waiting_locks
            , COUNT(DISTINCT CASE 
                                WHEN ls.request_status = N'GRANT' AND ls.request_mode NOT LIKE N'I%' AND ls.resource_type = N'OBJECT'
                                    THEN ls.object_name 
                                    ELSE NULL 
                                END) AS nonintent_objects_granted_locks
        FROM ##locks_snapshot ls
        WHERE
            (db_name = @db_name OR @db_name IS NULL) AND
            (session_id = @SPID OR @SPID IS NULL) AND
            (login_name = @login OR @login IS NULL) 
        GROUP BY 
            ls.session_id
            , ls.login_name
            , ls.host_name
            , ls.status
            , ls.blocking_session_id
            , ls.transaction_il
            , ls.text
            , ls.open_transaction_count
    )t
    ORDER BY 
        blocker_mark DESC           --сначала те сессии, которые блокируют кого-то
        , CASE 
            WHEN EXISTS (SELECT 1/0 FROM ##locked_objects lo where lo.granted_SPID = t.session_id)
                THEN 9999 
            ELSE waiting_locks 
        END DESC                    --если сессия кого-то блокирует, то вверху списка будут те, которые сами не заблокированы 
        , granted_locks DESC        --потом те, у которых больше всего блокировок наложено
        , session_id ASC            --и потом уже по spid'ам по возрастанию
    OPTION (MAXDOP 1);

    --общая информация о блокировках по объектам, с учётом параметров ХП
    --TODO: убрать фильтры по спиду/логину?
    SELECT db_name
        , object_name    
        , COUNT(*) AS overall_locks
        , COUNT(CASE WHEN request_status = N'GRANT' THEN 1 ELSE NULL END) AS granted_locks
        , COUNT(DISTINCT CASE WHEN request_status = N'GRANT' THEN session_id ELSE NULL END) AS sessions_with_granted_locks
        , COUNT(DISTINCT CASE WHEN request_status <> N'GRANT' THEN session_id ELSE NULL END) AS waiting_sessions
        , COUNT(CASE WHEN request_status <> N'GRANT' THEN 1 ELSE NULL END) AS waiting_locks
        , COUNT(CASE WHEN resource_type = N'OBJECT' AND request_mode NOT LIKE N'I%' AND request_status = N'GRANT' THEN 1 ELSE NULL END) AS object_nonintent_locks_granted
        , COUNT(CASE WHEN resource_type = N'OBJECT' AND request_status <> N'GRANT' THEN 1 ELSE NULL END) AS object_locks_waiting
        , COUNT(CASE WHEN resource_type IN (N'KEY', N'RID') AND request_status = N'GRANT' THEN 1 ELSE NULL END) AS key_locks_granted
        , COUNT(CASE WHEN resource_type IN (N'KEY', N'RID') AND request_status <> N'GRANT' THEN 1 ELSE NULL END) AS key_locks_waiting
        , COUNT(CASE WHEN resource_type = N'PAGE' AND request_status = N'GRANT' THEN 1 ELSE NULL END) AS page_locks_granted
        , COUNT(CASE WHEN resource_type = N'PAGE' AND request_status <> N'GRANT' THEN 1 ELSE NULL END) AS page_locks_waiting
        , CAST(COUNT(CASE WHEN resource_type IN (N'KEY', N'RID') THEN 1 ELSE NULL END) * 100.0 / COUNT(*) AS decimal(18, 2)) AS key_locks_pctg
        , SUM(COUNT(*)) OVER (PARTITION BY db_name) AS locks_in_db_overall
        , CAST(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY db_name) AS decimal(18, 2)) AS tbl_locks_in_db_pctg
    FROM ##locks_snapshot
    WHERE 
            (db_name = @db_name OR @db_name IS NULL) 
            AND (session_id = @SPID OR @SPID IS NULL) 
            AND (login_name = @login OR @login IS NULL)
    GROUP BY db_name, object_name
    ORDER BY waiting_locks DESC, db_name, object_name
    OPTION (MAXDOP 1);
    


    --DETAILED-mode
    -----------------------------------------------------------------------------------------------

    --TODO: учитывать @db_name, @login_name, @session_id - либо выводить информацию только по ним, либо выделять в списке

    IF @mode = N'DETAILED'
    BEGIN

        --Список блокировок, сессий, входящих в "цепочки блокировок". Сюда попадают только сессии, которые создают ожидания
        --Если сессия 1 наложила блокировку на объект, которую ждёт сессия 2, при этом сессия 3 наложила блокировку, которую ждёт сессия 1,
        --в этом резалтсете будут все блокировки сессий 1 и 3 (владельцы блокировок), в следующем - все блокировки сессии 2 (которые ждут и никого не блокируют)
        --если второй резалтсет пустой - значит все сессии кого-то блокируют

        --TODO: разделить первый резалтсет на два. В первом только те, кто блокирует, но сами не заблокированы;
        --во втором - те, которые кого-то блокируют, но, при этом сами заблокированы сессиями из первого резалтсета

        --информация о сессиях, которые блокируют других, но сами не заблокированы ни кем
        SELECT 
            ls.session_id
            , ls.login_name
            , ls.host_name
            , ls.status            
            , ls.db_name
            , CASE 
                WHEN lo.locked_object_name = ls.object_name 
                    AND lo.resource_type = ls.resource_type 
                    AND lo.[resource] = ls.resource_description 
                THEN N'___' + ls.object_name + N'___' 
                ELSE ls.object_name
            END AS object_name    --если это тот ресурс, которого кто-то ждёт, имя выделяется нижними подчёркиваниями
            , ls.resource_type
            , ls.resource_description
            , ls.hobt_id
            , ls.request_type
            , request_status    
            , ls.request_mode
            , ls.open_transaction_count
            , ls.text
        FROM ##locks_snapshot ls
        INNER JOIN 
            (    
                SELECT DISTINCT granted_SPID, locked_object_name, resource_type, [resource]
                FROM ##locked_objects ilo
                WHERE NOT EXISTS (                              --получаем только тех, кто блокирует других, но сам не заблокирован
                    SELECT 1/0 FROM ##locked_objects ilo2 WHERE ilo2.waiting_SPID = ilo.granted_SPID
                )
            ) lo ON ls.session_id = lo.granted_SPID
        ORDER BY ls.session_id, ls.db_name, ls.object_name
            , CASE 
                WHEN ls.resource_type = N'OBJECT' THEN 1 
                WHEN ls.resource_type = N'EXTENT' THEN 2
                WHEN ls.resource_type = N'PAGE' THEN 3
                ELSE 4 END
        OPTION (MAXDOP 1);

        --информация о сессиях, которые блокируют других, при этом сами кем-то заблокированы
        SELECT 
            ls.session_id
            , ls.login_name
            , ls.host_name
            , ls.status
            , ls.db_name
            , CASE 
                WHEN lo.locked_object_name = ls.object_name 
                    AND lo.resource_type = ls.resource_type 
                    AND lo.[resource] = ls.resource_description 
                THEN N'___' + ls.object_name + N'___' 
                ELSE ls.object_name
            END AS object_name    --если это тот ресурс, которого кто-то ждёт, имя выделяется нижними подчёркиваниями
            , ls.resource_type
            , ls.resource_description
            , ls.hobt_id
            , ls.request_type
            , CASE 
                WHEN ls.request_status = N'GRANT' THEN ls.request_status
                ELSE N'___' + ls.request_status + N'___' 
            END AS request_status    --если блокирующая сессия сама кого-то ждёт, статус выделяется нижними подчёркиваниями
            , ls.request_mode
            , ls.open_transaction_count
            , ls.text
            , ls.blocking_session_id
        FROM ##locks_snapshot ls
        INNER JOIN 
            (    
                SELECT DISTINCT granted_SPID, locked_object_name, resource_type, [resource]
                FROM ##locked_objects ilo
                WHERE EXISTS (      --получаем только тех, кто блокирует других, но сам заблокирован
                    SELECT 1/0 FROM ##locked_objects ilo2 WHERE ilo2.waiting_SPID = ilo.granted_SPID
                )
            ) lo ON ls.session_id = lo.granted_SPID
        ORDER BY ls.session_id, ls.db_name, ls.object_name
            , CASE 
                WHEN ls.resource_type = N'OBJECT' THEN 1 
                WHEN ls.resource_type = N'EXTENT' THEN 2
                WHEN ls.resource_type = N'PAGE' THEN 3
                ELSE 4 END
        OPTION (MAXDOP 1);




        --информация о сессиях, которые заблокированы, но сами никого не блокируют
        SELECT
            ls.session_id
            , ls.login_name
            , ls.host_name
            , ls.status
            , ls.db_name
            , CASE 
                WHEN lo.locked_object_name = ls.object_name 
                    AND lo.resource_type = ls.resource_type 
                    AND lo.[resource] = ls.resource_description 
                THEN N'___' + ls.object_name + N'___' 
                ELSE ls.object_name
            END AS object_name    --если это тот ресурс, которого ждёт сессия, имя выделяется нижними подчёркиваниями
            , ls.resource_type
            , ls.resource_description
            , ls.hobt_id
            , ls.request_type
            , CASE 
                WHEN ls.request_status = N'GRANT' THEN ls.request_status
                ELSE N'___' + ls.request_status + N'___' 
            END AS request_status    --статус ожидания на ресурсе выделяется нижними подчёркиваниями
            , ls.request_mode
            , ls.open_transaction_count
            , ls.text
            , ls.blocking_session_id
        FROM ##locks_snapshot ls
        INNER JOIN 
            (    
                SELECT DISTINCT waiting_SPID, locked_object_name, resource_type, [resource]
                FROM ##locked_objects lo
                WHERE NOT EXISTS (SELECT 1/0 FROM ##locked_objects lo2 WHERE lo2.granted_SPID = lo.waiting_SPID)
            ) lo ON ls.session_id = lo.waiting_SPID
        ORDER BY ls.session_id, ls.db_name, ls.object_name
            , CASE 
                WHEN ls.resource_type = N'OBJECT' THEN 1 
                WHEN ls.resource_type = N'EXTENT' THEN 2
                WHEN ls.resource_type = N'PAGE' THEN 3
                ELSE 4 END
        OPTION (MAXDOP 1);

    END;

    --------------------------------------------------------------------------------------------------------------------------------

    --построение цепочки блокировок
    --------------------------------------------------------------------------------------------------------------------------------
    IF @blocking_chains = 1
    BEGIN
        
        INSERT INTO ##chains (session_id, blocking_session_id)
        SELECT session_id, blocking_session_id
        FROM ##locks_snapshot
        GROUP BY session_id, blocking_session_id;

        WITH chains_rec AS (
            SELECT 
                c.session_id, 
                --c.blocking_session_id AS b_s_id,
                c_prev.blocking_session_id AS prev_b_s_id,
                CAST (CASE 
                    WHEN c_prev.blocking_session_id IS NULL THEN ''
                    ELSE CAST (c_prev.blocking_session_id AS varchar(100)) + ' -> ' 
                END + CAST(c.blocking_session_id AS varchar(100)) AS varchar(100)) AS chain,
                1 AS lvl
            FROM ##chains c
            JOIN ##chains c_prev ON c.blocking_session_id = c_prev.session_id 

            UNION ALL

            SELECT 
                rec.session_id,
                c.blocking_session_id,
                CAST (rec.chain + 
                CASE 
                    WHEN c.blocking_session_id IS NULL THEN ''
                    ELSE ' -> ' + CAST (c.blocking_session_id AS varchar(100)) 
                END AS varchar(100)),
                rec.lvl + 1   --следующий уровень в иерархии
            FROM chains_rec rec
            JOIN ##chains c ON rec.prev_b_s_id = c.session_id 

        )
        SELECT session_id, chain + ' -> ' + CAST(session_id AS varchar(10)) AS blocking_chain
        FROM (
            SELECT session_id, chain, ROW_NUMBER() OVER(PARTITION BY session_id ORDER BY lvl) AS rn
            FROM chains_rec
        )t 
        WHERE rn = 1
        ORDER BY chain, session_id
        OPTION (MAXDOP 1);
    END;

    --------------------------------------------------------------------------------------------------------------------------------


    --очистка
    --------------------------------------------------------------------------------------------------------------------------------

    --Если @clear_data = 1, snapshot удаляется
    IF @clear_data = 1 AND OBJECT_ID('tempdb..##locks_snapshot', 'U') IS NOT NULL
        DROP TABLE ##locks_snapshot;

    IF OBJECT_ID('tempdb..##orphaned_objects', 'U') IS NOT NULL
        DROP TABLE ##orphaned_objects;

    IF OBJECT_ID('tempdb..##locked_objects', 'U') IS NOT NULL
        DROP TABLE ##locked_objects;

    IF OBJECT_ID('tempdb..##chains', 'U') IS NOT NULL 
        DROP TABLE ##chains;
    --------------------------------------------------------------------------------------------------------------------------------
    SET @dbg_info = N'Finished at: ' + CONVERT(nvarchar(50), CURRENT_TIMESTAMP, 120);
    RAISERROR (@dbg_info, 1, 1) WITH NOWAIT;
    --PRINT 'Here we are';
GO

SET STATISTICS TIME, IO ON;

--EXEC sp_GetLockingInfo 
--                        @clear_data = 0
--                        , @refill_data = 0
--                        , @mode = N'OVERVIEW';
--                        --, @SPID = 61
--                        --, @db_name = N'StackOverflow2010'
--                        --, @db_id = 5;

EXEC sp_GetLockingInfo 
                        @clear_data = 0
                        , @refill_data = 0
                        , @mode = N'DETAILED'
                        , @blocking_chains = 1
                        , @use_blocking_SPID = 1;
SET STATISTICS TIME, IO OFF;


