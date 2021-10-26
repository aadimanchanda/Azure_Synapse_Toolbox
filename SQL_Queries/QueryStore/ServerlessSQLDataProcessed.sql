SELECT 
       ERH.query_text as [command],
       --ERH.status as [status],
       --ERH.login_name as [login_name],
       ERH.start_time as [start_time],
       ERH.end_time as [end_time],
       ERH.total_elapsed_time_ms as [duration_ms],
	   cast(ERH.total_elapsed_time_ms/1000.0 as decimal(12,2)) as [duration_sec],
       ERH.data_processed_mb as [data_processed_MB],
       cast(ERH.total_elapsed_time_ms/1000.0 as decimal(12,2)) as [duration_sec]
FROM sys.dm_exec_requests_history ERH
ORDER BY ERH.start_time desc


SELECT * FROM sys.dm_external_data_processed