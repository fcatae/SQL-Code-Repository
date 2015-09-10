IF OBJECT_ID('tempdb..#spBlockerPfe') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe
GO
-----------------------------------------------------------------------------------------------------
--
--
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spBlockerPFE(@timespan INT = 12)
AS

SET NOCOUNT ON
SET LOCK_TIMEOUT 30000

DECLARE @startDate DATETIME
DECLARE @prevDate DATETIME
DECLARE @endDate DATETIME
DECLARE @step INT = 0
DECLARE @time DATETIME

SELECT @startDate = GETDATE(), @prevDate = '1900-01-01', @step = 0

SET @endDate = DATEADD(HOUR,@timespan,GETDATE())

PRINT 'BLOCKER_PFE_SCRIPT_KATMAI Script v10.0.13 (SQL2008)'
PRINT ''
PRINT '  SQL Instance:       ' + @@SERVERNAME
PRINT '  SQL Version:        ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) + ' (' + CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR) + ')'
PRINT '  Start time:         ' + CONVERT(VARCHAR(24), GETDATE(), 121)
PRINT '  Scheduled end time: ' + CONVERT(VARCHAR(24), @endDate, 121)
	
-- SQL_TEXT --------------------------------------
CREATE TABLE #sqlquery_requested
(
	sql_handle			varbinary(64),
	plan_handle			varbinary(64),
	stmt_start			int,
	stmt_end			int,
	query_hash			binary(8),
	query_plan_hash		binary(8)
)

CREATE TABLE #filehandle
(
	file_handle VARBINARY(8) PRIMARY KEY, 
	database_id INT, 
	file_id INT, 
	filename NVARCHAR(260)
)

SET @time = GETDATE()
EXEC #spBlockerPfe_0

WHILE @startDate < @endDate
BEGIN
	PRINT ''
	PRINT 'BLOCKER_PFE_BEGIN SqlMonData ' + CONVERT(VARCHAR(24), GETDATE(), 121)

	SET @startDate = GETDATE()
	
	EXEC #spBlockerPfe_1 
	EXEC #spBlockerPfe_1_handle
	
	IF @step % 12 = 0 
	BEGIN
		DECLARE @savePrevDate DATETIME = GETDATE()
		
		EXEC #spBlockerPfe_2 @prevDate
		SET @prevDate = @savePrevDate
		
		EXEC #spBlockerPfe_2_handle
	END

	SET @step = @step + 1
	
	PRINT ''
	PRINT 'BLOCKER_PFE_END SqlMonData ' + CONVERT(VARCHAR(24), GETDATE(), 121) + ' ' + convert(VARCHAR(12), datediff(ms,@startDate,getdate())) 

	RAISERROR('',0,1) WITH NOWAIT
	
	WAITFOR DELAY '0:0:5'
END

GO
-----------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#spBlockerPfe_0') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe_0
	
IF OBJECT_ID('tempdb..#spBlockerPfe_1') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe_1

IF OBJECT_ID('tempdb..#spBlockerPfe_2') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe_2

IF OBJECT_ID('tempdb..#spBlockerPfe_1_handle') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe_1_handle

IF OBJECT_ID('tempdb..#spBlockerPfe_2_handle') IS NOT NULL
	DROP PROCEDURE #spBlockerPfe_2_handle
-----------------------------------------------------------------------------------------------------
GO
CREATE PROCEDURE #spBlockerPfe_0
AS

SET NOCOUNT ON

DECLARE @time DATETIME

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN MachineInfo'
PRINT ''
PRINT 'GeneralInformation'
PRINT REPLICATE('-',100)
PRINT 'ServerName: ' + @@SERVERNAME
PRINT 'PhysicalName: ' + CAST(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') AS VARCHAR)
PRINT 'ProductVersion: ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR)
PRINT 'ProductLevel: ' + CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR)
PRINT 'ResourceVersion: ' + CAST(SERVERPROPERTY('ResourceVersion') AS VARCHAR)
PRINT 'ResourceLastUpdateDateTime: ' + CAST(SERVERPROPERTY('ResourceLastUpdateDateTime') AS VARCHAR)
PRINT 'Edition: ' + CAST(SERVERPROPERTY('Edition') AS VARCHAR)
PRINT 'ProcessId: ' + CAST(SERVERPROPERTY('ProcessId') AS VARCHAR)
PRINT 'SessionId: ' + CAST(@@SPID AS VARCHAR)
PRINT 'Collation: ' + CAST(SERVERPROPERTY('Collation') AS VARCHAR(32))
PRINT ''
PRINT 'BLOCKER_PFE_END MachineInfo ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN @@version'
SELECT @@version AS 'version'
PRINT 'BLOCKER_PFE_END @@version ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

PRINT ''
PRINT 'BLOCKER_PFE_BEGIN xp_msver'
EXEC xp_msver
PRINT 'BLOCKER_PFE_END xp_msver ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.dm_os_sys_info'

SELECT 
	sqlserver_start_time, -- 2008
	cpu_count, hyperthread_ratio, 
	physical_memory_in_bytes/1024/1024 AS 'physical_memory(MB)',
	bpool_committed*8/1024 AS 'buffer_pool(MB)', 
	bpool_commit_target*8/1024 AS 'buffer_pool_target(MB)', 
	bpool_visible*8/1024 AS 'buffer_visible(MB)', 
	virtual_memory_in_bytes/1024/1024 AS 'virtual_memory(MB)',
	max_workers_count, scheduler_count
FROM sys.dm_os_sys_info

PRINT 'BLOCKER_PFE_END sys.dm_os_sys_info '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.dm_os_cluster_nodes'
SELECT NodeName FROM sys.dm_os_cluster_nodes 
PRINT 'BLOCKER_PFE_END sys.dm_os_cluster_nodes '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.dm_io_cluster_shared_drives'
SELECT DriveName FROM sys.dm_io_cluster_shared_drives 
PRINT 'BLOCKER_PFE_END sys.dm_io_cluster_shared_drives '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
 
SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.configurations'
SELECT name, value=CAST(value AS VARCHAR(16)), value_in_use=CAST(value_in_use AS VARCHAR(16)) 
FROM sys.configurations
ORDER BY name

PRINT 'BLOCKER_PFE_END sys.configurations ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
	
SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.databases'
SELECT 
d.database_id, d.name, state_desc=CAST(d.state_desc AS VARCHAR(20)), user_access_desc=CAST(d.user_access_desc AS VARCHAR(16)), d.compatibility_level, 
	d.create_date, collation_name=CAST(d.collation_name AS VARCHAR(64)), d.owner_sid, 
	d.log_reuse_wait_desc, 
	readonly = d.is_read_only, 
	autoclose = d.is_auto_close_on, 
	autoshrink = d.is_auto_shrink_on, 
	standby = d.is_in_standby, 
	cleanshut = d.is_cleanly_shutdown, 
	supplog = d.is_supplemental_logging_enabled, 
	snapshot = d.snapshot_isolation_state, 
	readsnap = d.is_read_committed_snapshot_on, 
	recovery = CAST(d.recovery_model_desc AS VARCHAR(8)), 
	pageverify =  CAST(d.page_verify_option_desc AS VARCHAR(8)), 
	autostat_crt = d.is_auto_create_stats_on, 
	autostat_upd = d.is_auto_update_stats_on, 
	autostat_async = d.is_auto_update_stats_async_on, 
	fulltext = d.is_fulltext_enabled, 
	trustworthy = d.is_trustworthy_on, 
	dbchain = d.is_db_chaining_on, 
	paramforced = d.is_parameterization_forced, 
	masterkey = d.is_master_key_encrypted_by_server, 
	rep_pub = d.is_published, 
	rep_sub = d.is_subscribed, 
	rep_merge = d.is_merge_published, 
	rep_dist = d.is_distributor, 
	sync_bkp = d.is_sync_with_backup, 
	sb_enabled = d.is_broker_enabled, 
	sb_guid = d.service_broker_guid, 
	datacorr = d.is_date_correlation_on
FROM sys.databases d
PRINT 'BLOCKER_PFE_END sys.databases '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.master_files'
-- VIEW ANY DEFINITION
SELECT 
	d.database_id, d.file_id, state_desc = CAST(d.state_desc AS VARCHAR(16)), type_desc=CAST(d.type_desc AS VARCHAR(16)), d.physical_name, d.file_guid, d.data_space_id, d.name, d.size, d.max_size, d.growth, d.is_media_read_only, d.is_read_only, d.is_sparse, d.is_percent_growth 
FROM sys.master_files d
PRINT 'BLOCKER_PFE_END sys.master_files '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.master_files[Size]'
-- VIEW ANY DEFINITION
SELECT 
	d.database_id, type_desc=CAST(d.type_desc AS VARCHAR(16)), 
	CAST(d.size AS BIGINT)*8/1024 AS 'Size(MB)', 
	CASE d.is_percent_growth 
		WHEN 0 THEN CAST(d.growth AS INT)*8/1024
		WHEN 1 THEN CAST(d.growth AS INT)*CAST(d.size AS INT)/100*8/1024
	END AS 'Growth(MB)',
	CASE d.is_percent_growth 
		WHEN 0 THEN CAST( (100*d.growth/d.size) AS SMALLINT )
		WHEN 1 THEN CAST( d.growth AS SMALLINT )
	END AS 'Growth(perc)',	
	d.physical_name
FROM sys.master_files d
ORDER BY d.physical_name
PRINT 'BLOCKER_PFE_END sys.master_files[Size] '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.traces'
-- VIEW ANY DEFINITION
SELECT 
	t.id, t.status, t.path, t.max_size, t.stop_time, t.max_files, t.is_rowset, t.is_rollover, t.is_shutdown, t.is_default, t.buffer_count, t.buffer_size, t.file_position, t.reader_spid, t.start_time, t.last_event_time, t.event_count, t.dropped_event_count 
FROM sys.traces t
PRINT 'BLOCKER_PFE_END sys.traces '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

TRUNCATE TABLE #filehandle

INSERT #filehandle(file_handle, database_id, file_id, filename)
SELECT vfs.file_handle, vfs.database_id, vfs.file_id, f.physical_name FROM sys.dm_io_virtual_file_stats(-1,-1) vfs
	LEFT JOIN sys.master_files f ON vfs.database_id = f.database_id AND vfs.file_id = f.file_id
	
SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.dm_exec_sessions'

SELECT 
	s.session_id, 
	s.login_time,
	s.status, 
	s.cpu_time, s.memory_usage, s.total_scheduled_time, s.total_elapsed_time, 
	s.last_request_start_time, s.last_request_end_time, 
	s.reads, s.writes, s.logical_reads, 
	s.row_count, 
	s.prev_error
FROM sys.dm_exec_sessions s 

PRINT 'BLOCKER_PFE_END sys.dm_exec_sessions '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
	
SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN sys.dm_exec_connections/sessions'

SELECT 
	s.session_id, 
	s.group_id, 
	CAST(s.status AS VARCHAR(16)) AS 'status',
	CAST(s.host_name AS VARCHAR(20)) AS 'host_name', 
	CAST(s.login_name AS VARCHAR(32)) AS 'login_name', 
	CAST(s.program_name AS VARCHAR(64)) AS 'program_name', 
	s.host_process_id, 
	c.connection_id,
	CAST(s.original_login_name AS VARCHAR(32)) AS 'original_login_name', 
	s.client_interface_name, s.client_version, 
	CAST(c.auth_scheme AS VARCHAR(16)) AS 'auth_scheme', 
	CAST(c.net_transport AS VARCHAR(16)) AS 'net_transport', 
	c.client_net_address, c.client_tcp_port, 
	CAST(c.most_recent_sql_handle AS VARBINARY(26)) AS 'most_recent_sql_handle', 
	c.net_packet_size, c.encrypt_option,
	c.connect_time, s.login_time
FROM sys.dm_exec_connections c left join sys.dm_exec_sessions s on c.session_id = s.session_id
	
PRINT 'BLOCKER_PFE_END sys.dm_exec_connections/sessions '  + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
	
GO
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spBlockerPfe_1_handle
AS

SET NOCOUNT ON
SET LOCK_TIMEOUT 250

DECLARE @time DATETIME

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN CollectSqlHandle'

select * from sys.messages e where e.message_id = 9100

-- COLLECT ADHOC REQUEST
INSERT #sqlquery_requested
SELECT
	sql_handle,
	plan_handle,
	statement_start_offset,
	statement_end_offset,
	query_hash,
	query_plan_hash
FROM sys.dm_exec_requests
WHERE sql_handle is not null AND session_id <> @@spid

-- COLLECT CURSOR
INSERT #sqlquery_requested
SELECT
	sql_handle,
	statement_start_offset,
	statement_end_offset,
	NULL,
	NULL
FROM sys.dm_exec_cursors(0)

-- OPENTRAN
INSERT #sqlquery_requested
SELECT 
	c.most_recent_sql_handle,
	0,
	0,
	NULL,
	NULL
FROM sys.dm_exec_connections c
WHERE session_id IN (SELECT session_id FROM sys.dm_tran_session_transactions)  AND session_id <> @@spid

PRINT 'BLOCKER_PFE_END CollectSqlHandle ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
GO
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spBlockerPfe_2_handle
AS

SET NOCOUNT ON
SET LOCK_TIMEOUT 3000

DECLARE @time DATETIME

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN CollectSqlHandle2'

-- OPENTRAN
INSERT #sqlquery_requested
SELECT 
	c.most_recent_sql_handle,
	0,
	0,
	NULL,
	NULL
FROM sys.dm_exec_connections c
WHERE session_id IN (SELECT session_id FROM sys.dm_tran_session_transactions) AND session_id <> @@spid

PRINT 'BLOCKER_PFE_END CollectSqlHandle2 ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 


SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN FlushSqlHandle[object_name]'

SELECT DISTINCT
	CAST(req.sql_handle AS VARBINARY(26)) AS 'sql_handle',
	
	CAST(
		DB_NAME(dbid) + N'.' + 
		OBJECT_SCHEMA_NAME(objectid,dbid) + N'.' + 
		OBJECT_NAME(objectid,dbid) AS NVARCHAR(128)) AS 'object_name',
	st.dbid, st.objectid
FROM #sqlquery_requested req
CROSS APPLY sys.dm_exec_sql_text(req.sql_handle) st
WHERE objectid IS NOT NULL
ORDER BY dbid, objectid

PRINT 'BLOCKER_PFE_END FlushSqlHandle[object_name] ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN FlushSqlHandle[sqlquery_requested]'
SELECT
'COUNT=',			Count=COUNT(*),
'SQLHANDLE=',		CAST(req.sql_handle AS VARBINARY(26)) AS 'sql_handle',
	req.stmt_start, 
	req.stmt_end,
	'QUERY_HASH=',		req.query_hash,
	'QUERY_PLAN_HASH=',	req.query_plan_hash
FROM #sqlquery_requested req
WHERE req.query_hash IS NOT NULL
GROUP BY sql_handle, query_hash, stmt_start, req.stmt_end, req.query_plan_hash
ORDER BY COUNT(*) DESC

PRINT 'BLOCKER_PFE_END FlushSqlHandle[sqlquery_requested] ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 

SET @time = GETDATE()
PRINT ''
PRINT 'BLOCKER_PFE_BEGIN FlushSqlHandle[dm_exec_sql_text]'
SELECT TOP 1000
'COUNT=',			COUNT(*),
'SQLHANDLE=',		CAST(req.sql_handle AS VARBINARY(26)) AS 'sql_handle',
'SQLHASH=',			req.query_hash,
	req.stmt_start, 
	req.stmt_end,
	CHAR(13) + CHAR(10),
	'SQLTEXT=',			sqltext=(SELECT SUBSTRING(	text, stmt_start/2 + 1, 
												((CASE	WHEN stmt_end = -1 THEN DATALENGTH(text) 
														WHEN stmt_end = 0 THEN 1024
														ELSE stmt_end END) - stmt_start)/2 )
						FROM sys.dm_exec_sql_text(sql_handle))
FROM #sqlquery_requested req 
GROUP BY sql_handle, query_hash, stmt_start, req.stmt_end
ORDER BY COUNT(*) DESC

TRUNCATE TABLE #sqlquery_requested

PRINT 'BLOCKER_PFE_END FlushSqlHandle[dm_exec_sql_text] ' + convert(VARCHAR(12), datediff(ms,@time,getdate())) 
GO

-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
GO

-----------------------------------------------------------------------------------------------------
EXEC #spBlockerPFE
-----------------------------------------------------------------------------------------------------
