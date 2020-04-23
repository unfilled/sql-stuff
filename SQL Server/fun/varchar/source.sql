--CREATE DATABASE chartest;
USE chartest;
GO

--CREATE TABLE vc10 (
--	i int IDENTITY(1,1) PRIMARY KEY
--	, d datetime not null
--	, v varchar(10)
--);

--CREATE TABLE c7 (
--	i int IDENTITY(1,1) PRIMARY KEY
--	, d datetime not null
--	, c char(7)
--);

--CREATE TABLE vc100 (
--	i int IDENTITY(1,1) PRIMARY KEY
--	, d datetime not null
--	, v varchar(100)
--);

--CREATE TABLE vcmax (
--	i int IDENTITY(1,1) PRIMARY KEY
--	, d datetime not null
--	, v varchar(max)
--);

--INSERT INTO vc10 (d, v)
--SELECT DATEADD(SECOND, ROW_NUMBER() OVER(ORDER BY (SELECT (NULL))), '20200101') d
--	, CAST(ROW_NUMBER() OVER(ORDER BY (SELECT (NULL))) AS varchar(10)) v
--from sys.columns c1, sys.columns c2;

--INSERT INTO vc100 (d, v)
--SELECT d, v
--FROM vc10;

--INSERT INTO vcmax (d,v)
--SELECT d, v
--FROM vc10;

--INSERT INTO c7 (d, c)
--SELECT d, v
--FROM vc10;

--SELECT TOP 10000 *	--it's ok, clustered index seek
--FROM vc10
--ORDER BY i;

--DBCC TRACEON (7470, -1)
--DBCC TRACEOFF (7470, -1)
SET STATISTICS IO, TIME ON;

SELECT TOP 1000 * --it is not ok - sort spills
FROM vc10
ORDER BY d;

SELECT TOP 1000 * --it is not ok - sort spills
FROM c7
ORDER BY d;

SELECT TOP 1000 * --it is ok - no spills, no warnings
FROM vc100
ORDER BY d;

SELECT TOP 1000 * --it is not ok - huge memory grant
FROM vcmax
ORDER BY d;

SET STATISTICS IO, TIME OFF;


--DROP TABLE vc10;
--DROP TABLE vc100;
--DROP TABLE vcmax;
