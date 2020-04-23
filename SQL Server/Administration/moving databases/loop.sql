﻿--SELECT *
--FROM CommandList;

SET NOCOUNT ON;

DECLARE @dbname AS sysname
	, @restore_cmd AS varchar(max);

WHILE 1 = 1	--можно придумать условие остановки, но мне было лень
BEGIN
	SELECT TOP 1 @dbname = dbName, @restore_cmd = restore_command 
	FROM CommandList
	WHERE processed IS NULL; --берём случайную БД из таблицы, среди необработанных

	IF @dbname IS NOT NULL 
	BEGIN
		--добавляем сообщение о начале обработки
		UPDATE CommandList
		SET start_dt = GETDATE()
		WHERE dbName = @dbname;

		RAISERROR('Начали восстановление %s', 0, 1, @dbname) WITH NOWAIT;
		
		BEGIN TRY

			--пробуем восстановить БД, если что-то не так, в CATCH запишем что не так
			EXEC (@restore_cmd);

			--добавляем информацию в журнал
			UPDATE CommandList
			SET processed = 0
				, finish_dt = GETDATE()
			WHERE dbName = @dbname;

			RAISERROR('База %s восстановлена успешно', 0, 1, @dbname) WITH NOWAIT;

		END TRY
		BEGIN CATCH

			RAISERROR('Возникла проблема с восстановлением %s', 0, 1, @dbname) WITH NOWAIT;

			UPDATE CommandList 
			SET processed = 1
				, finish_dt = GETDATE()
				, error_msg = ERROR_MESSAGE();

		END CATCH

	END
	ELSE	--если ничего не выбрали, то просто ждём 
		BEGIN

			RAISERROR('waiting', 0, 1) WITH NOWAIT;

			WAITFOR DELAY '00:00:30';

		END
		
	SET @dbname = NULL;
	SET @restore_cmd = NULL;

END

--TRUNCATE TABLE CommandList