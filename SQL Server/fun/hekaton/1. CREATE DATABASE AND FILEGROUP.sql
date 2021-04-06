CREATE DATABASE test 
ON PRIMARY (
    NAME        = test,
    FILENAME    = 'D:\SQLServer\data\test.mdf',
    SIZE        = 1024 MB,
    MAXSIZE     = 10240 MB,
    FILEGROWTH  = 256 MB
) 
LOG ON (
    NAME        = test_log,
    FILENAME    = 'D:\SQLServer\data\test_log.ldf',
    SIZE        = 256 MB,
    MAXSIZE     = 10240 MB,
    FILEGROWTH  = 128 MB
);
GO

ALTER DATABASE test SET RECOVERY SIMPLE;
GO

ALTER DATABASE test 
    ADD FILEGROUP [HEKATON_TEST] CONTAINS MEMORY_OPTIMIZED_DATA;
GO

ALTER DATABASE test ADD FILE (
    NAME        = hekaton_test, 
    FILENAME    = 'D:\SQLServer\data\hekaton_test'
)
TO FILEGROUP [HEKATON_TEST];
GO
