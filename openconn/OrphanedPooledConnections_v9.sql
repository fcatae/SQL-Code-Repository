--select * from sys.dm_exec_connections
--use DBS600
go
-- truncate table #all_handles 
-- truncate table #current_handles 

-- CREATE TABLE #all_handles (sql_handle varbinary(64) primary key not null)
-- CREATE TABLE #current_handles (sql_handle varbinary(64))
	
IF OBJECT_ID('tempdb..#CurrentStaticSessions') IS NOT NULL
	DROP TABLE #CurrentStaticSessions
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------

IF OBJECT_ID('tempdb..#spPooledConnectionsPfe') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe
GO
CREATE PROCEDURE #spPooledConnectionsPfe(@timespan INT = 12, @fast INT = 0)
AS

SET NOCOUNT ON
SET LOCK_TIMEOUT 60000
SET ANSI_WARNINGS OFF

DECLARE @startDate DATETIME
DECLARE @endDate DATETIME

SELECT @startDate = GETDATE()
SET @endDate = DATEADD(HOUR,@timespan,GETDATE())

PRINT 'FIND ORPHANED CONNECTION Script v1.9'
PRINT ''
PRINT '  SQL Instance:       ' + @@SERVERNAME
PRINT '  SQL Version:        ' + CAST(SERVERPROPERTY('ProductVersion') AS VARCHAR) + ' (' + CAST(SERVERPROPERTY('ProductLevel') AS VARCHAR) + ')'
PRINT '  Start time:         ' + CONVERT(VARCHAR(24), GETDATE(), 121)
--PRINT '  Scheduled end time: ' + CONVERT(VARCHAR(24), @endDate, 121)
--PRINT ''
PRINT ''

-- SQL_TEXT --------------------------------------
CREATE TABLE #all_handles (sql_handle varbinary(64) primary key not null, total int);
CREATE TABLE #current_handles (sql_handle varbinary(64), total int);
CREATE TABLE #CurrentStaticSessions(
	appinstance_id smallint,
	session_id	smallint,
	last_request_start_time	datetime,
	last_request_end_time	datetime,
	login_time	datetime,
	status	nvarchar(60),
	connect_time	datetime,
	most_recent_sql_handle	varbinary(64));

-- Show databases
SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DATABASES';
SELECT name, database_id FROM sys.databases ORDER BY name;

SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_PROCESSES';
SELECT COUNT(*) total, DB_NAME(dbid) name, dbid database_id FROM master..sysprocesses 
WHERE LEN(net_library)>0
GROUP BY dbid
ORDER BY count(*) desc;


--WHILE @startDate <= @endDate
BEGIN
	--PRINT 'POOLED_CONN_START: ' + CONVERT(VARCHAR(24), GETDATE(), 121) 
	--PRINT ''

	SET @startDate = GETDATE()

	TRUNCATE TABLE #CurrentStaticSessions;

	EXEC #spPooledConnectionsPfe_clients @fast;
	
	IF @fast = 0
	BEGIN
		EXEC #spPooledConnectionsPfe_trace;
	END

	EXEC #spPooledConnectionsPfe_dumpcmds;

	-- SELECT CONVERT(VARCHAR(12), datediff(ms,@startDate,getdate())) as 'TimeSpent'

	RAISERROR('',0,1) WITH NOWAIT
	
	--WAITFOR DELAY '0:0:5'
END

PRINT 'END time: ' + CONVERT(VARCHAR(24), GETDATE(), 121)

GO
-----------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_trace') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_trace
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_dumpcmds') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_dumpcmds
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_dumpcmds_old') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_dumpcmds_old
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_trace_appid') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_trace_appid
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_dumpcmds_appid') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_dumpcmds_appid
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_clients') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_clients
IF OBJECT_ID('tempdb..#spPooledConnectionsPfe_minpool') IS NOT NULL
	DROP PROCEDURE #spPooledConnectionsPfe_minpool
GO
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spPooledConnectionsPfe_trace
AS

SET NOCOUNT ON;

DECLARE @appid INT
DECLARE @maxAppid INT

	
	DECLARE @startDate DATETIME;

	SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONNECTIONS_APPLICATION_ID';

	SET @startDate = GETDATE();
	

	-- SELECT MAX(appinstance_id) FROM #CurrentStaticSessions
	SELECT @appid = 1, @maxAppid = MAX(appinstance_id) FROM #CurrentStaticSessions

	WHILE @appid <= @maxAppid
	BEGIN
		EXEC #spPooledConnectionsPfe_trace_appid @appid;
		SET @appid = @appid + 1
	END

	
	--SELECT	CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_TRACE_END',
	--		CONVERT(VARCHAR(12), datediff(ms,@startDate,getdate())) as 'TimeSpent'
GO

CREATE PROCEDURE #spPooledConnectionsPfe_dumpcmds(@maxidletime INT = 480)
AS

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF

	--DECLARE @startDate DATETIME;

	--SET @startDate = GETDATE();

	truncate table #current_handles;

with CurrentStaticSessions
as ( select * from #CurrentStaticSessions )
	insert #current_handles(sql_handle,total)
		select most_recent_sql_handle,count(*) from CurrentStaticSessions cs
		where		status = 'sleeping' and
					most_recent_sql_handle not in (select sql_handle from #all_handles) and
					
					(
						( last_request_end_time < dateadd(s, -1, (select max(connect_time) from CurrentStaticSessions))  )  
						OR
						( datediff(s, last_request_start_time, getdate()) > @maxidletime )
					)
		group by most_recent_sql_handle;

	IF EXISTS(SELECT * FROM #current_handles)
	BEGIN

		SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DUMP_COMMANDS';
		
		/*
		
		WITH 
			nivel0 AS (SELECT cmd FROM tbCmd),
			nivel1 AS (SELECT LTRIM(cmd) + '  ' AS 'stdcmd' from nivel0),
			nivel2 AS (SELECT CHARINDEX(' ', stdcmd) p1, stdcmd from nivel1),
			nivel3 AS (SELECT p1, CHARINDEX(' ', stdcmd, p1+1) p2, stdcmd from nivel2),
			nivel4 AS (SELECT SUBSTRING(stdcmd, 0, p1) token1, SUBSTRING(stdcmd, p1 + 1, p2 - p1 - 1) token2 FROM nivel3),
			infoCommands AS (SELECT CASE WHEN UPPER(token1) <> 'EXEC' THEN token1 ELSE token2 END AS 'procedure' FROM nivel4)
		SELECT * FROM infoCommands;*/

		SELECT total, 'SQL=' AS 'cmd',
				CAST(add_sql_handles.sql_handle AS VARBINARY(45)) sql_handle, 
				info.dbid, info.objectid, 
				CHAR(13) + CHAR(10) + 'text=' AS ' ',
				case when objectid is NULL then info.text 
					 else OBJECT_NAME(info.objectid,info.dbid)
				end text
			from #current_handles add_sql_handles
		outer apply sys.dm_exec_sql_text(sql_handle) info
		ORDER BY total DESC;


		SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DUMP_PROCEDURES';
		
		SELECT total, CAST(add_sql_handles.sql_handle AS VARBINARY(45)) sql_handle, 
				QUOTENAME(DB_NAME(t.dbid)) + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid,t.dbid)) + N'.' + QUOTENAME(OBJECT_NAME(t.objectid,t.dbid)) AS 'Procedure',
				t.dbid, t.objectid 
			from #current_handles add_sql_handles
		outer apply sys.dm_exec_sql_text(sql_handle) t
		WHERE t.objectid IS NOT NULL
		ORDER BY total DESC;



		
		SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DUMP_GROUPED_COMMANDS';
		
		/*		
		WITH 
			nivel0 AS (SELECT cmd FROM tbCmd),
			nivel1 AS (SELECT LTRIM(cmd) + '  ' AS 'stdcmd' from nivel0),
			nivel2 AS (SELECT CHARINDEX(' ', stdcmd) p1, stdcmd from nivel1),
			nivel3 AS (SELECT p1, CHARINDEX(' ', stdcmd, p1+1) p2, stdcmd from nivel2),
			nivel4 AS (SELECT SUBSTRING(stdcmd, 0, p1) token1, SUBSTRING(stdcmd, p1 + 1, p2 - p1 - 1) token2 FROM nivel3),
			infoCommands AS (SELECT CASE WHEN UPPER(token1) <> 'EXEC' THEN token1 ELSE token2 END AS 'procedure' FROM nivel4)
		SELECT * FROM infoCommands;*/

		WITH tbCmd 
		AS (select	
				case when objectid is NULL then t.text 
					 else QUOTENAME(DB_NAME(t.dbid)) + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid,t.dbid)) + N'.' + QUOTENAME(OBJECT_NAME(t.objectid,t.dbid))
				end 'cmd'
				from #current_handles ch cross apply sys.dm_exec_sql_text(ch.sql_handle) t),
			nivel0 AS (SELECT cmd FROM tbCmd),
			nivel1 AS (SELECT LTRIM(cmd) + '  ' AS 'stdcmd' from nivel0),
			nivel2 AS (SELECT CHARINDEX(' ', stdcmd) p1, stdcmd from nivel1),
			nivel3 AS (SELECT p1, CHARINDEX(' ', stdcmd, p1+1) p2, stdcmd from nivel2),
			nivel4 AS (SELECT LTRIM(SUBSTRING(stdcmd, 0, p1)) token1, LTRIM(SUBSTRING(stdcmd, p1 + 1, p2 - p1 - 1)) token2 FROM nivel3),
			infoCommands AS (SELECT CASE WHEN UPPER(token1) <> 'EXEC' THEN QUOTENAME(token1) ELSE QUOTENAME(token2) END AS 'procedure' FROM nivel4)
		SELECT COUNT(*) total, [procedure] FROM infoCommands
		GROUP BY [procedure]
		ORDER BY COUNT(*) DESC;

		--insert #all_handles
		--select * from #current_handles
		--where sql_handle not in (select sql_handle from #all_handles);
		
		--SELECT	CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DUMPCMDS_END',
		--	CONVERT(VARCHAR(12), datediff(ms,@startDate,getdate())) as 'TimeSpent'

	END
	
GO

CREATE PROCEDURE #spPooledConnectionsPfe_trace_appid(@appid INT)
AS

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF;

-- existem conexoes 
WITH CurrentStaticSessions
AS (SELECT * FROM #CurrentStaticSessions WHERE appinstance_id = @appid)
SELECT 
	@appid appid,
	last_request_end_time, 
	login_time, 
	connect_time, 
	(select count(*) from CurrentStaticSessions tmp where tmp.connect_time > dateadd(s, 1, cs.last_request_end_time)) Busy, 
	datediff(s, login_time, getdate()) IdleTime, 
	datediff(s, login_time, last_request_start_time) delay_start, 
	datediff(s, last_request_start_time, last_request_end_time) last_duration, 

	CAST(
	(CASE WHEN t.objectid IS NULL 
		THEN REPLACE(REPLACE(LEFT(t.text,100), char(10), ' '),char(13),'')
		ELSE QUOTENAME(DB_NAME(t.dbid)) + N'.' + QUOTENAME(OBJECT_SCHEMA_NAME(t.objectid,t.dbid)) + N'.' + QUOTENAME(OBJECT_NAME(t.objectid,t.dbid)) 
	END) AS NVARCHAR(128)) AS 'Command', 

	cs.session_id, cs.most_recent_sql_handle, t.dbid, t.objectid	
	from CurrentStaticSessions cs OUTER APPLY sys.dm_exec_sql_text(most_recent_sql_handle) t
WHERE
	status = 'sleeping'
ORDER BY last_request_end_time, login_time;

GO
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spPooledConnectionsPfe_minpool
AS

SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_MINPOOL_SIZE';


DECLARE @appid INT
DECLARE @maxAppid INT
	-- SELECT MAX(appinstance_id) FROM #CurrentStaticSessions
	SELECT @appid = 1, @maxAppid = MAX(appinstance_id) FROM #CurrentStaticSessions

	WHILE @appid <= @maxAppid
	BEGIN

		declare curConnections cursor for
		select connect_time from #CurrentStaticSessions WHERE appinstance_id = @appid 
		order by connect_time;

		open curConnections;

		declare @connect_time datetime;
		declare @last_connect_time datetime;
		declare @number_pool int = 0;

		fetch curConnections into @connect_time;
		select @last_connect_time  = '2010-01-01';

		while @@FETCH_STATUS = 0
		BEGIN
			if datediff(s, @last_connect_time, @connect_time) > 1
				set @number_pool = @number_pool + 1;
			else if datediff(ms, @last_connect_time, @connect_time) > 500
				set @number_pool = @number_pool + 1;
	
			select @last_connect_time  = @connect_time;
			fetch curConnections into @connect_time;
		END

		--SELECT @number_pool 'NUMBER_POOLS';
		UPDATE #Clients SET pools_total = @number_pool
		WHERE appinstance_id = @appid; 

		close curConnections;
		deallocate curConnections;

		SET @appid = @appid + 1

	END
		
GO

-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spPooledConnectionsPfe_dumpcmds_appid(@appid INT, @maxidletime INT = 480)
AS

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF

	-- DECLARE @startDate DATETIME;

	-- SET @startDate = GETDATE();

	truncate table #current_handles;

with CurrentStaticSessions
as ( select * from #CurrentStaticSessions WHERE appinstance_id = @appid )
	insert #current_handles(sql_handle)
		select distinct		most_recent_sql_handle from CurrentStaticSessions cs
		where		status = 'sleeping' and
					most_recent_sql_handle not in (select sql_handle from #all_handles) and
					
					(
						( last_request_end_time < dateadd(s, -1, (select max(connect_time) from CurrentStaticSessions))  )  
						OR
						( datediff(s, last_request_start_time, getdate()) > @maxidletime )
					)


	IF EXISTS(SELECT * FROM #current_handles)
	BEGIN

		SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_IDENTIFIED_COMMAND';
		
		SELECT 'SQL=' AS 'cmd',
				CAST(add_sql_handles.sql_handle AS VARBINARY(45)) sql_handle, 
				info.dbid, info.objectid, 
				case when objectid is NULL then info.text 
					 else OBJECT_NAME(info.objectid,info.dbid)
				end text
			from #current_handles add_sql_handles
		outer apply sys.dm_exec_sql_text(sql_handle) info;

		insert #all_handles
		select distinct * from #current_handles
		where sql_handle not in (select sql_handle from #all_handles);
		
		--SELECT	CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_DUMPCMDS_END',
		--	CONVERT(VARCHAR(12), datediff(ms,@startDate,getdate())) as 'TimeSpent'

	END
	
GO
-----------------------------------------------------------------------------------------------------
CREATE PROCEDURE #spPooledConnectionsPfe_clients(@fast INT=0)
AS

SET NOCOUNT ON;
SET ANSI_WARNINGS OFF
	
--DECLARE @startDate DATETIME;

--SET @startDate = GETDATE();


-- Define the clients
SELECT 
		IDENTITY(INT,1,1) appinstance_id,
		s.host_name,
		s.program_name,
		s.host_process_id,
		p.dbid database_id,
		s.login_name,
		count(*) pool_size,
		count( case s.status when 'sleeping' then 1 else null end ) pool_unused, 
		-1 AS pools_total,
		datediff(s, min(connect_time), getdate()) pool_time,
		datediff(s, max(connect_time), getdate()) open_time,
		datediff(s, max(connect_time), max(last_request_start_time)) active_time,
		datediff(s, max(last_request_start_time), getdate()) idle_time,
		datediff(s, min(last_request_end_time), getdate()) oldconn_time,
		max(c.client_net_address) client_net_address,
		max(connect_time) most_recent_connection_time
INTO #Clients
FROM sys.dm_exec_sessions s inner join sys.dm_exec_connections c on s.session_id=c.session_id
	inner join master..sysprocesses p on p.spid = s.session_id
GROUP BY 
		s.host_name,
		s.program_name,
		s.host_process_id,
		p.dbid,
		s.login_name
HAVING count(*) > 1
ORDER BY pool_size DESC;

CREATE INDEX idx ON #Clients(database_id, host_process_id, host_name, program_name, login_name) INCLUDE (appinstance_id);

INSERT #CurrentStaticSessions(appinstance_id, session_id, last_request_start_time, last_request_end_time, login_time, status, connect_time, most_recent_sql_handle )
SELECT 
	(SELECT TOP 1 appinstance_id FROM #Clients ctb
			 WHERE	ctb.host_name = s.host_name AND 
					ctb.program_name = s.program_name AND
					ctb.host_process_id = s.host_process_id AND
					ctb.database_id = p.dbid AND 
					ctb.login_name = s.login_name ), 
	s.session_id, s.last_request_start_time, s.last_request_end_time, s.login_time, s.status, c.connect_time, c.most_recent_sql_handle 
FROM sys.dm_exec_sessions s inner join sys.dm_exec_connections c on s.session_id=c.session_id
		inner join master..sysprocesses p on p.spid = s.session_id;

IF @FAST=0
BEGIN

	EXEC #spPooledConnectionsPfe_minpool;

	SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_CLIENTS_STATISTICS';

	SELECT 
		CAST(host_name AS VARCHAR(16)) host_name,
		CAST(DB_NAME(database_id) AS VARCHAR(16)) database_name,
		CAST(login_name AS VARCHAR(20)) login_name,
		sum(pool_size) pool_size
	FROM #Clients
	GROUP BY host_name, database_id, login_name		
	ORDER BY pool_size DESC;
	
	SELECT 
		CAST(program_name AS VARCHAR(32)) program_name,
		CAST(DB_NAME(database_id) AS VARCHAR(16)) database_name,
		CAST(login_name AS VARCHAR(20)) login_name,
		SUM(pool_size) pool_size
	FROM #Clients
	GROUP BY program_name, database_id, login_name
	ORDER BY pool_size DESC;

	SELECT CONVERT(VARCHAR(24), GETDATE(), 121) as 'POOLED_CONN_CLIENTS';

	SELECT 
			appinstance_id appid,
			CAST(host_name AS VARCHAR(16)) host_name,
			CAST(program_name AS VARCHAR(32)) program_name,
			host_process_id processid,
			CAST(DB_NAME(database_id) AS VARCHAR(16)) database_name,
			CAST(login_name AS VARCHAR(20)) login_name,
			pool_size,
			pool_unused, 
			pools_total,
			pool_time,
			open_time,
			active_time,
			idle_time,
			oldconn_time,
			client_net_address,
			database_id,
			most_recent_connection_time
	FROM #Clients
	ORDER BY appinstance_id;


END

GO

-- Versao FAST
--EXEC #spPooledConnectionsPfe @timespan=0, @fast=0
--EXEC #spPooledConnectionsPfe @fast=1

-- Versao FULL
EXEC #spPooledConnectionsPfe @fast=0
