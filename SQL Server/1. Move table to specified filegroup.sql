DECLARE @filegroup AS varchar(20) = 'POSTS';
DECLARE @moveNCindexes AS bit = 1;
DECLARE @isOnline AS bit = 0;
DECLARE @tableName as varchar(50) = 'Posts';
DECLARE @schemaName as varchar(50) = 'dbo';

IF NOT EXISTS (
	SELECT * FROM sys.filegroups
	WHERE name = @filegroup
	)
	BEGIN
		PRINT('Nonexisting filegroup specified');
		GOTO STOPE;
	END


IF @isOnline = 0 
	BEGIN
		--disable nonclustered indexes first
		IF EXISTS (			
			SELECT *
			FROM sys.indexes
			WHERE object_id = OBJECT_ID(@schemaName+'.'+@tableName, 'U') AND index_id>1 AND is_disabled = 0
		)		
			BEGIN
				DECLARE @ixName as nvarchar(200);
				DECLARE @command as nvarchar(max);

				DECLARE nxiCursor CURSOR FOR
				SELECT name
				FROM sys.indexes
				WHERE object_id = OBJECT_ID(@schemaName+'.'+@tableName, 'U') AND index_id>1;

				OPEN nxiCursor
				FETCH NEXT FROM nxiCursor INTO @ixName
				
				WHILE @@FETCH_STATUS = 0
					BEGIN
						SET @command = 'ALTER INDEX [' + @ixName + '] ON [' + @schemaName + '].[' + @tableName + '] DISABLE;';						
						EXEC (@command);

						FETCH NEXT FROM nxiCursor INTO @ixName
					END;
				CLOSE nxiCursor;
				DEALLOCATE nxiCursor;

			END;

			--rebuild clustered index or heap in new filegroup
			IF EXISTS (
				SELECT *
				FROM sys.indexes i
				JOIN sys.filegroups f ON i.data_space_id = f.data_space_id
				WHERE object_id = OBJECT_ID(@schemaName+'.'+@tableName, 'U') 
					AND index_id IN (0,1)
					AND f.name <> @filegroup
			)
				BEGIN
					DECLARE @isClustered as int, @isUnique as bit, @idxName as nvarchar(200), @commandRebuild as nvarchar(max);
					
					SELECT @isClustered = i.index_id, @idxName = i.name, @isUnique = i.is_unique
					FROM sys.indexes i
					JOIN sys.filegroups f ON i.data_space_id = f.data_space_id
					WHERE object_id = OBJECT_ID(@schemaName+'.'+@tableName, 'U') 
						AND index_id IN (0,1)
						AND f.name <> @filegroup;

					--there may be a lot of paramters, I use defaults
					IF @isClustered > 0	--clustered index needs to be rebuild
						SET @commandRebuild = 'CREATE ' + CASE WHEN @isUnique = 1 THEN 'UNIQUE' ELSE '' END + 'INDEX [' + @idxName + '] ON [' + @schemaName + '].[' + @tableName + '] REBUILD ON [' + @filegroup + '];';
					ELSE	
						SET @commandRebuild = 'ALTER TABLE [' + @schemaName + '].[' + @tableName + '] REBUILD ON ' + @filegroup + ';';
					
					EXEC (@commandRebuild);

				END;



			SELECT * 
			FROM sys.indexes 
			WHERE object_id = OBJECT_ID(@schemaName+'.'+@tableName, 'U');

			SELECT * 
			FROM sys.filegroups
	END;

STOPE: