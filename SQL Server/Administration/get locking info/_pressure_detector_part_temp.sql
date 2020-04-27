
               WHEN der.transaction_isolation_level = 0 THEN ''Unspecified''
               WHEN der.transaction_isolation_level = 1 THEN ''Read Uncommitted''
               WHEN der.transaction_isolation_level = 2 AND EXISTS ( SELECT 1/0 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE der.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed Snapshot Isolation''
               WHEN der.transaction_isolation_level = 2 AND NOT EXISTS ( SELECT 1/0 FROM sys.dm_tran_active_snapshot_database_transactions AS trn WHERE der.session_id = trn.session_id AND is_snapshot = 0 ) THEN ''Read Committed''
               WHEN der.transaction_isolation_level = 3 THEN ''Repeatable Read''
               WHEN der.transaction_isolation_level = 4 THEN ''Serializable''
               WHEN der.transaction_isolation_level = 5 THEN ''Snapshot''
               ELSE ''???''
           END AS transaction_isolation_level ,
