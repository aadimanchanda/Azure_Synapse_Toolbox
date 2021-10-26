SELECT *
FROM   (SELECT *,
               Count(*)
                 OVER()                        AS total_count,
               Row_number()
                 OVER(
                   ORDER BY r.start_time DESC) AS [rownum]
        FROM   (SELECT req.status                                    AS [status]
                       ,
                       req.transaction_id
                       AS [request_id],
                       'SQL On-demand'                               AS
                       [SQL Resource],
                       s.login_name                                  AS
                       [login_name],
                       Cast(s.session_id AS VARCHAR)                 AS
                       [session_id],
                       req.start_time                                AS
                       [submit_time],
                       req.start_time                                AS
                       [start_time],
                       NULL                                          AS
                       [end_time],
                       req.command                                   AS
[command_clause],
Substring(sqltext.text, ( req.statement_start_offset / 2 ) + 1,
( (
CASE req.statement_end_offset
  WHEN -1 THEN Datalength(sqltext.text)
  ELSE req.statement_end_offset
END - req.statement_start_offset ) / 2 ) + 1) AS [command],
req.total_elapsed_time                        AS [duration],
'N/A'                                         AS
[queued_duration],
req.total_elapsed_time                        AS
[running_duration],
'N/A'                                         AS
[data_processed_in_MB],
'N/A'                                         AS [group_name],
'N/A'                                         AS [source],
'N/A'                                         AS [pipeline],
'N/A'                                         AS [importance],
'N/A'                                         AS
[classifier_name],
'N/A'                                         AS
         [data_processed_in_bytes]
FROM   sys.dm_exec_requests req
CROSS apply sys.Dm_exec_sql_text(sql_handle) sqltext
JOIN sys.dm_exec_sessions s
  ON req.session_id = s.session_id
     AND s.session_id NOT IN (SELECT DISTINCT session_id
                              FROM   sys.dm_exec_requests req
                                     CROSS apply
sys.Dm_exec_sql_text(sql_handle)
sqltext
WHERE
Substring(sqltext.text, (
req.statement_start_offset / 2 ) + 1, (
(
CASE req.statement_end_offset
WHEN -1
THEN Datalength(sqltext.text)
          ELSE
req.statement_end_offset
        END
-
req.statement_start_offset ) / 2 ) + 1) LIKE
'%@@Azure.Synapse.Monitoring.SQLQuerylist%')
UNION
SELECT h.status                             AS [status],
h.transaction_id                     AS [request_id],
'SQL On-Demand'                      AS [SQL Resource],
h.login_name                         AS [login_name],
'N/A'                                AS [session_id],
h.start_time                         AS [submit_time],
h.start_time                         AS [start_time],
h.end_time                           AS [end_time],
h.command                            AS [command_clause],
h.query_text                         AS [command],
h.total_elapsed_time_ms              AS [duration],
'N/A'                                AS [queued_duration],
h.total_elapsed_time_ms              AS [running_duration],
Cast(h.data_processed_mb AS VARCHAR) AS [data_processed_in_MB],
'N/A'                                AS [group_name],
'N/A'                                AS [source],
'N/A'                                AS [pipeline],
'N/A'                                AS [importance],
'N/A'                                AS [classifier_name],
'N/A'                                AS [data_processed_in_bytes]
FROM   sys.dm_exec_requests_history h) AS r
WHERE  (( r.start_time >= CONVERT(DATETIME, '2020-11-29T15:24:10Z')
AND r.start_time <= CONVERT(DATETIME, '2022-12-27T15:24:10Z') ))) AS
final_dataset_output
WHERE  final_dataset_output.rownum BETWEEN 0 AND 100 