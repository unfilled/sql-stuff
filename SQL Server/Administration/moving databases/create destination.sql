USE [Maintenance]
GO

CREATE TABLE CommandList (
	dbName sysname unique						--им€ Ѕƒ
	, restore_command varchar(max)				--команда дл€ восстановлени€
	, processed bit								--статус выполнени€
	, creation_dt datetime DEFAULT GETDATE()	--врем€ добавлени€ записи
	, start_dt datetime							--врем€ начала обработки
	, finish_dt datetime						--врем€ окончани€ обработки
	, error_msg varchar(max)					--текст ошибки, при наличии
);