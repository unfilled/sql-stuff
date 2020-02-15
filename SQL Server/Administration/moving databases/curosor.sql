DECLARE @unc_backup_path AS varchar(max) = 'D:\SQLServer\backup\'--'\\newServer\backup_share\' --путь к шаре для бэкапа на новом сервере
	, @local_backup_path AS varchar(max) = 'D:\SQLServer\backup\'	--локальный путь на новом сервере к папке с бэкапами
	, @new_data_path as varchar(max) = 'D:\SQLServer\data\';		--локальный путь на новом сервере к папке, где должны оказаться данные

SET NOCOUNT ON;

IF OBJECT_ID ('tempdb..##CommandList', 'U') IS NULL
	CREATE TABLE ##CommandList (
		dbName sysname unique			--имя БД
		, backup_command varchar(max)	--сгенерированная команда для бэкапа
		, offline_command varchar(max)	--сгенерированная команда для перевода БД в офлайн после бэкапа
		, restore_command varchar(max)	--сгенерированная команда для восстановления БД на новом сервере
		, processed bit					--признак обработки: NULL - не обработано, 0 - обработано успешно, 1 - ошибка
		, start_dt datetime				--когда начали обработку
		, finish_dt datetime			--когда закончили обработку
		, error_msg varchar(max)		--сообщение об ошибке, при наличии
	);

INSERT INTO ##CommandList (dbname, backup_command, offline_command, restore_command)
SELECT name	
	, 'BACKUP DATABASE [' + name + '] TO DISK = ''' + @unc_backup_path + name + '.bak'' WITH INIT, STATS = 5;' AS backup_command --включает INIT - бэкап в месте назначения будет перезаписываться
	, 'ALTER DATABASE [' + name + '] SET OFFLINE WITH ROLLBACK IMMEDIATE;' AS offline_command
	, 'RESTORE DATABASE [' + name + '] FROM DISK = ''' + @local_backup_path + name + '.bak'' WITH ' 
		+ (
			SELECT 'MOVE ''' + mf.name + ''' TO ''' + 
				@new_data_path + REVERSE(LEFT(REVERSE(mf.physical_name), CHARINDEX('\', REVERSE(mf.physical_name))-1)) +
				''', '	
			FROM sys.master_files mf
			WHERE mf.database_id = d.database_id
			FOR XML PATH('')
		) + 'REPLACE, RECOVERY, STATS = 5;' AS restore_command	
FROM sys.databases d
WHERE database_id > 4 
	AND state_desc = N'ONLINE'
	AND name NOT IN (SELECT dbname FROM ##CommandList)
	AND name <> 'Maintenance';	--у меня linked server - это тот же экземпляр, поэтому исключаю БД, которая используется на "linked server"

DECLARE @dbname AS sysname
	, @backup_cmd AS varchar(max)
	, @restore_cmd AS varchar(max)
	, @offline_cmd AS varchar(max);

DECLARE BeginWork CURSOR
FOR 
SELECT dbName, backup_command, offline_command, restore_command
FROM ##CommandList
WHERE processed IS NULL;

OPEN BeginWork;

FETCH NEXT FROM BeginWork INTO @dbname, @backup_cmd, @offline_cmd, @restore_cmd;

WHILE @@FETCH_STATUS = 0
	BEGIN
		--имя БД и команды получены, теперь нужно:
		-- сделать бэкап
		-- добавить в таблицу-приёмник на новом экземпляре команду для восстановления
		-- перевести БД в офлайн, чтобы к ней не могли подключиться
		-- получить следующую БД из списка

		--делаем отметку о начале работ
		UPDATE ##CommandList
		SET start_dt = GETDATE()
		WHERE dbName = @dbname;

		BEGIN TRY
			
			RAISERROR ('Делаем бэкап %s', 0, 1, @dbname) WITH NOWAIT; --сообщения на вкладке messages будут появляться сразу
			
			-- делаем бэкап
			EXEC (@backup_cmd);

			RAISERROR ('Добавляем команду на восстановления %s', 0, 1, @dbname) WITH NOWAIT;

			-- добавляем запись в таблицу-приёмник на linked server
			INSERT INTO [(LOCAL)].[Maintenance].[dbo].[CommandList] (dbName, restore_command)
			VALUES (@dbname, @restore_cmd);

			RAISERROR ('Переводим %s в OFFLINE', 0, 1, @dbname) WITH NOWAIT;

			-- переводим БД в офлайн
			EXEC (@offline_cmd);

			--Ставим успешный статус, проставляем время окончания работы
			UPDATE ##CommandList
			SET processed = 0
				, finish_dt = GETDATE()
			WHERE dbName = @dbname;

		END TRY
		BEGIN CATCH
			
			RAISERROR ('ОШИБКА. Необходимо проверить error_msg в ##CommandList', 0, 1, @dbname) WITH NOWAIT;

			-- если что-то пошло не так, ставим ошибочный статус и описание ошибки
			UPDATE ##CommandList
			SET processed = 1
				, finish_dt = GETDATE()
				, error_msg = ERROR_MESSAGE();

		END CATCH

		FETCH NEXT FROM BeginWork INTO @dbname, @backup_cmd, @offline_cmd, @restore_cmd;
	END

CLOSE BeginWork;

DEALLOCATE BeginWork;

--выводим результат
SELECT dbName
	, CASE processed WHEN 1 THEN 'Ошибка' WHEN 0 THEN 'Успешно' ELSE 'Не обработано' END as Status 
	, start_dt
	, finish_dt
	, error_msg
FROM ##CommandList
ORDER BY start_dt;


--DROP TABLE ##CommandList;

--ALTER DATABASE dev1 SET ONLINE;
--ALTER DATABASE dev2 SET ONLINE;
--ALTER DATABASE dev3 SET ONLINE;
--ALTER DATABASE dev4 SET ONLINE;
--ALTER DATABASE dev5 SET ONLINE;
--ALTER DATABASE dev6 SET ONLINE;

