USE [Maintenance]
GO

CREATE TABLE CommandList (
	dbName sysname unique						--��� ��
	, restore_command varchar(max)				--������� ��� ��������������
	, processed bit								--������ ����������
	, creation_dt datetime DEFAULT GETDATE()	--����� ���������� ������
	, start_dt datetime							--����� ������ ���������
	, finish_dt datetime						--����� ��������� ���������
	, error_msg varchar(max)					--����� ������, ��� �������
);