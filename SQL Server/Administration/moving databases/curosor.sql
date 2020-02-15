DECLARE @unc_backup_path AS varchar(max) = 'D:\SQLServer\backup\'--'\\newServer\backup_share\' --���� � ���� ��� ������ �� ����� �������
	, @local_backup_path AS varchar(max) = 'D:\SQLServer\backup\'	--��������� ���� �� ����� ������� � ����� � ��������
	, @new_data_path as varchar(max) = 'D:\SQLServer\data\';		--��������� ���� �� ����� ������� � �����, ��� ������ ��������� ������

SET NOCOUNT ON;

IF OBJECT_ID ('tempdb..##CommandList', 'U') IS NULL
	CREATE TABLE ##CommandList (
		dbName sysname unique			--��� ��
		, backup_command varchar(max)	--��������������� ������� ��� ������
		, offline_command varchar(max)	--��������������� ������� ��� �������� �� � ������ ����� ������
		, restore_command varchar(max)	--��������������� ������� ��� �������������� �� �� ����� �������
		, processed bit					--������� ���������: NULL - �� ����������, 0 - ���������� �������, 1 - ������
		, start_dt datetime				--����� ������ ���������
		, finish_dt datetime			--����� ��������� ���������
		, error_msg varchar(max)		--��������� �� ������, ��� �������
	);

INSERT INTO ##CommandList (dbname, backup_command, offline_command, restore_command)
SELECT name	
	, 'BACKUP DATABASE [' + name + '] TO DISK = ''' + @unc_backup_path + name + '.bak'' WITH INIT, STATS = 5;' AS backup_command --�������� INIT - ����� � ����� ���������� ����� ����������������
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
	AND name <> 'Maintenance';	--� ���� linked server - ��� ��� �� ���������, ������� �������� ��, ������� ������������ �� "linked server"

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
		--��� �� � ������� ��������, ������ �����:
		-- ������� �����
		-- �������� � �������-������� �� ����� ���������� ������� ��� ��������������
		-- ��������� �� � ������, ����� � ��� �� ����� ������������
		-- �������� ��������� �� �� ������

		--������ ������� � ������ �����
		UPDATE ##CommandList
		SET start_dt = GETDATE()
		WHERE dbName = @dbname;

		BEGIN TRY
			
			RAISERROR ('������ ����� %s', 0, 1, @dbname) WITH NOWAIT; --��������� �� ������� messages ����� ���������� �����
			
			-- ������ �����
			EXEC (@backup_cmd);

			RAISERROR ('��������� ������� �� �������������� %s', 0, 1, @dbname) WITH NOWAIT;

			-- ��������� ������ � �������-������� �� linked server
			INSERT INTO [(LOCAL)].[Maintenance].[dbo].[CommandList] (dbName, restore_command)
			VALUES (@dbname, @restore_cmd);

			RAISERROR ('��������� %s � OFFLINE', 0, 1, @dbname) WITH NOWAIT;

			-- ��������� �� � ������
			EXEC (@offline_cmd);

			--������ �������� ������, ����������� ����� ��������� ������
			UPDATE ##CommandList
			SET processed = 0
				, finish_dt = GETDATE()
			WHERE dbName = @dbname;

		END TRY
		BEGIN CATCH
			
			RAISERROR ('������. ���������� ��������� error_msg � ##CommandList', 0, 1, @dbname) WITH NOWAIT;

			-- ���� ���-�� ����� �� ���, ������ ��������� ������ � �������� ������
			UPDATE ##CommandList
			SET processed = 1
				, finish_dt = GETDATE()
				, error_msg = ERROR_MESSAGE();

		END CATCH

		FETCH NEXT FROM BeginWork INTO @dbname, @backup_cmd, @offline_cmd, @restore_cmd;
	END

CLOSE BeginWork;

DEALLOCATE BeginWork;

--������� ���������
SELECT dbName
	, CASE processed WHEN 1 THEN '������' WHEN 0 THEN '�������' ELSE '�� ����������' END as Status 
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

