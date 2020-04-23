USE [Maintenance]
GO

CREATE TABLE CommandList (
	dbName sysname unique						--имя БД
	, restore_command varchar(max)				--команда для восстановления
	, processed bit								--статус выполнения
	, creation_dt datetime DEFAULT GETDATE()	--время добавления записи
	, start_dt datetime							--время начала обработки
	, finish_dt datetime						--время окончания обработки
	, error_msg varchar(max)					--текст ошибки, при наличии
);