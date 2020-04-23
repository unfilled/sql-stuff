USE [Maintenance]
GO

--scripts for setting all user databases offline
SELECT N'ALTER DATABASE ' + d.name + N' SET OFFLINE WITH ROLLBACK IMMEDIATE;' AS offline_script  
FROM sys.databases d
WHERE d.database_id > 4;

--scripts for attaching all user databases back after changes
--SSMS MUST BE STARTED 'AS ADMINISTRATOR' in my case, idk why
SELECT
	N'CREATE DATABASE ' + QUOTENAME(d.name) + N' ON '
	+ STUFF((
		SELECT N', (FILENAME = N''' + mf.physical_name + N''')'
		FROM sys.master_files mf 
		WHERE mf.database_id = d.database_id
		FOR XML PATH('')
	), 1, 2, '')
	+ N' FOR ATTACH;' AS attach_script
FROM sys.databases d
WHERE d.database_id > 4;

--scripts for detaching all user databases before changing collation
SELECT N'exec sp_detach_db @dbname = ''' + d.name + N''', @skipchecks = ''true'';' AS detach_script
FROM sys.databases d
WHERE d.database_id > 4;