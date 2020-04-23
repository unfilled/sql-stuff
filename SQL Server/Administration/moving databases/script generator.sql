DECLARE @unc_backup_path AS varchar(max) = '\\newServer\backup_share\'
	, @local_backup_path AS varchar(max) = 'E:\Backup\'
	, @new_data_path as varchar(max) = 'D:\SQLServer\data\';

SELECT name	
	, 'BACKUP DATABASE [' + name + '] TO DISK = ''' + @unc_backup_path + name + '.bak'' WITH INIT, STATS = 5;' AS backup_command
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
WHERE database_id > 4 AND state_desc = N'ONLINE';

