--SELECT *
--FROM CommandList;

SET NOCOUNT ON;

DECLARE @dbname AS sysname
	, @restore_cmd AS varchar(max);

WHILE 1 = 1	--����� ��������� ������� ���������, �� ��� ���� ����
BEGIN
	SELECT TOP 1 @dbname = dbName, @restore_cmd = restore_command 
	FROM CommandList
	WHERE processed IS NULL; --���� ��������� �� �� �������, ����� ��������������

	IF @dbname IS NOT NULL 
	BEGIN
		--��������� ��������� � ������ ���������
		UPDATE CommandList
		SET start_dt = GETDATE()
		WHERE dbName = @dbname;

		RAISERROR('������ �������������� %s', 0, 1, @dbname) WITH NOWAIT;
		
		BEGIN TRY

			--������� ������������ ��, ���� ���-�� �� ���, � CATCH ������� ��� �� ���
			EXEC (@restore_cmd);

			--��������� ���������� � ������
			UPDATE CommandList
			SET processed = 0
				, finish_dt = GETDATE()
			WHERE dbName = @dbname;

			RAISERROR('���� %s ������������� �������', 0, 1, @dbname) WITH NOWAIT;

		END TRY
		BEGIN CATCH

			RAISERROR('�������� �������� � ��������������� %s', 0, 1, @dbname) WITH NOWAIT;

			UPDATE CommandList 
			SET processed = 1
				, finish_dt = GETDATE()
				, error_msg = ERROR_MESSAGE();

		END CATCH

	END
	ELSE	--���� ������ �� �������, �� ������ ��� 
		BEGIN

			RAISERROR('waiting', 0, 1) WITH NOWAIT;

			WAITFOR DELAY '00:00:30';

		END
		
	SET @dbname = NULL;
	SET @restore_cmd = NULL;

END

--TRUNCATE TABLE CommandList