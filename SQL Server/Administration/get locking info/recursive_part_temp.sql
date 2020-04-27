--select * from sys.dm_exec_requests
--where session_id > 50

--select * from sys.dm_exec_sessions 
--where session_id > 50

; with t as (
select distinct session_id, blocking_session_id, cast(blocking_session_id as nvarchar(50)) b_id
from ##locks_snapshot
), r as(
select t1.session_id, t1.blocking_session_id fb, t2.blocking_session_id sb, cast(t1.b_id + CASE WHEN t2.b_id IS NULL THEN N'' ELSE N'->' + t2.b_id END as nvarchar(500)) as bchain, 1 as lvl
, cast(case when t2.blocking_session_id is null then N'' else t2.b_id + N' <- ' end + t1.b_id as nvarchar(500)) AS good_chain 
from t t1
join t t2 on t1.blocking_session_id = t2.session_id

union all

select r.session_id, r.sb, t.blocking_session_id, cast(r.bchain + case when t.b_id is null then N'' else N'->' + t.b_id end as nvarchar(500)), r.lvl + 1
, cast(case when t.b_id is null then N'' else t.b_id + N' <- ' end + r.good_chain as nvarchar(500))
from r
join t ON r.sb = t.session_id
where t.blocking_session_id is not null
)

select session_id, good_chain
from
(
select session_id, lvl, good_chain, row_number() over(partition by session_id order by lvl desc) as rn
from r
)x
where rn = 1
order by good_chain 