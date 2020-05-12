select
	1 as row_num
	, d.name
	, mf.file_id
	, type_desc
	, cast(vfs.size_on_disk_bytes / 1024.0 / 1024 as decimal(10,2)) as file_size_mb
	, vfs.num_of_reads
	, vfs.num_of_bytes_read
	, vfs.io_stall_read_ms
	, vfs.num_of_writes
	, vfs.num_of_bytes_written
	, vfs.io_stall_write_ms
	, vfs.io_stall
into #t
from sys.databases d
join sys.master_files mf on d.database_id = mf.database_id
left join sys.dm_io_virtual_file_stats(NULL, NULL) vfs on mf.database_id = vfs.database_id and mf.file_id = vfs.file_id
waitfor delay '00:00:10'
insert into #t
select
	10 as row_num
	, d.name
	, mf.file_id
	, type_desc
	, cast(vfs.size_on_disk_bytes / 1024.0 / 1024 as decimal(10,2)) as file_size_mb
	, vfs.num_of_reads
	, vfs.num_of_bytes_read
	, vfs.io_stall_read_ms
	, vfs.num_of_writes
	, vfs.num_of_bytes_written
	, vfs.io_stall_write_ms
	, vfs.io_stall
from sys.databases d
join sys.master_files mf on d.database_id = mf.database_id
left join sys.dm_io_virtual_file_stats(NULL, NULL) vfs on mf.database_id = vfs.database_id and mf.file_id = vfs.file_id
select t1.name, t1.file_id, t1.type_desc, t1.file_size_mb
	, t1.num_of_reads - t2.num_of_reads as reads
	, cast((t1.num_of_bytes_read - t2.num_of_bytes_read)/1024.0 as decimal(10,2)) as kbytes_read
	, cast((t1.num_of_bytes_read - t2.num_of_bytes_read)/1024.0 / NULLIF(t1.num_of_reads - t2.num_of_reads, 0) AS decimal(10,2)) AS kb_per_read
	--, t1.io_stall_read_ms - t2.io_stall_read_ms AS read_ms
	, CAST(nullif(t1.io_stall_read_ms - t2.io_stall_read_ms, 0)/1.0/nullif((t1.num_of_reads - t2.num_of_reads), 0) as decimal(10,3)) as read_latency_ms
	, t1.num_of_writes - t2.num_of_writes AS writes
	, cast((t1.num_of_bytes_written - t2.num_of_bytes_written)/1024.0 as decimal(10,2)) as kbytes_written
	, CAST(((t1.num_of_bytes_written - t2.num_of_bytes_written)/1024.0) / NULLIF (t1.num_of_writes - t2.num_of_writes, 0) as decimal(10,2)) AS kb_per_write
	--, t1.io_stall_write_ms - t2.io_stall_write_ms as write_ms
	, CAST(nullif(t1.io_stall_write_ms - t2.io_stall_write_ms, 0)/1.0/nullif((t1.num_of_writes - t2.num_of_writes), 0) as decimal(10,3)) as write_latency_ms
	--, t1.io_stall - t2.io_stall as io_stall
	, CAST((t1.num_of_reads - t2.num_of_reads + t1.num_of_writes - t2.num_of_writes)/1.0/nullif(t1.io_stall - t2.io_stall, 0) as decimal(10,3)) as avg_latency_ms
from #t t1
join #t t2 on t1.name = t2.name and t1.file_id = t2.file_id
where t1.row_num = 10 and t2.row_num = 1
drop table #t