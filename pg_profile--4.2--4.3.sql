\echo Use "ALTER EXTENSION pg_profile UPDATE" to load this file. \quit
DROP FUNCTION report_queries_format(sserver_id integer, queries_list jsonb, start1_id integer, end1_id integer, start2_id integer, end2_id integer);
DROP FUNCTION collect_obj_stats;
DROP FUNCTION collect_pg_stat_statements_stats;
DROP FUNCTION dbstats_reset;
DROP FUNCTION get_report_context;
DROP FUNCTION dbstats_reset_format;
DROP FUNCTION dbstats_reset_format_diff;
DROP FUNCTION drop_server;
DROP FUNCTION get_report_datasets;
DROP FUNCTION init_sample;
DROP FUNCTION sample_dbobj_delta;
DROP FUNCTION sections_jsonb;
DROP FUNCTION show_servers;
DROP FUNCTION take_sample(sserver_id integer, skip_sizes boolean);
DROP FUNCTION top_io_tables_format;
DROP FUNCTION top_io_tables_format_diff;
DROP FUNCTION top_tables;
DROP FUNCTION top_tables_format;
DROP FUNCTION top_tables_format_diff;
DROP FUNCTION top_toasts;
DROP FUNCTION save_pg_stat_statements;

CREATE FUNCTION drop_server(IN server name) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    del_rows    integer;
    dserver_id  integer;
BEGIN
    SELECT server_id INTO STRICT dserver_id FROM servers WHERE server_name = server;
    DELETE FROM bl_samples WHERE server_id = dserver_id;
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_kcache_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_statements_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_database_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tables_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_indexes_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_tablespaces_srv%1$s',
      dserver_id);
    EXECUTE format('ALTER EXTENSION pg_profile DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    EXECUTE format(
      'DROP TABLE last_stat_user_functions_srv%1$s',
      dserver_id);
    DELETE FROM last_stat_cluster WHERE server_id = dserver_id;
    DELETE FROM last_stat_io WHERE server_id = dserver_id;
    DELETE FROM last_stat_slru WHERE server_id = dserver_id;
    DELETE FROM last_stat_wal WHERE server_id = dserver_id;
    DELETE FROM last_stat_archiver WHERE server_id = dserver_id;
    DELETE FROM sample_stat_tablespaces WHERE server_id = dserver_id;
    DELETE FROM tablespaces_list WHERE server_id = dserver_id;
    /*
     * We have several constraints that should be deferred to avoid
     * violation due to several cascade deletion branches
     */
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_toast_table,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles
      DEFERRED;
    DELETE FROM samples WHERE server_id = dserver_id;
    SET CONSTRAINTS
        fk_stat_indexes_indexes,
        fk_toast_table,
        fk_st_tablespaces_tablespaces,
        fk_st_tables_tables,
        fk_indexes_tables,
        fk_user_functions_functions,
        fk_stmt_list,
        fk_kcache_stmt_list,
        fk_statements_roles
      IMMEDIATE;
    DELETE FROM servers WHERE server_id = dserver_id;
    GET DIAGNOSTICS del_rows = ROW_COUNT;
    RETURN del_rows;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION drop_server(IN server name) IS 'Drop a server';
CREATE FUNCTION show_servers()
RETURNS TABLE(server_name name, connstr text, enabled boolean, max_sample_age integer, description text)
SET search_path=@extschema@ AS $$
DECLARE
  c_priv CURSOR FOR
    SELECT server_name, connstr, enabled, max_sample_age, server_description FROM servers;

  c_unpriv CURSOR FOR
    SELECT server_name, '<hidden>' as connstr, enabled, max_sample_age, server_description FROM servers;
BEGIN
  IF has_column_privilege('servers', 'connstr', 'SELECT') THEN
    FOR server_name, connstr, enabled, max_sample_age, description IN SELECT s.server_name, s.connstr, s.enabled, s.max_sample_age, s.server_description FROM servers s LOOP
      RETURN NEXT;
    END LOOP;
  ELSE
    FOR server_name, connstr, enabled, max_sample_age, description IN SELECT s.server_name, '<hidden>' as connstr, s.enabled, s.max_sample_age, s.server_description FROM servers s LOOP
      RETURN NEXT;
    END LOOP;
  END IF;
  RETURN;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION show_servers() IS 'Displays all servers';
CREATE FUNCTION collect_pg_stat_statements_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer, IN topn integer) RETURNS void SET search_path=@extschema@ AS $$
DECLARE
  qres              record;
  st_query          text;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Check if mandatory extensions exists
    IF NOT
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      )
    THEN
      RETURN;
    END IF;

    -- Save used statements extension in sample_settings
    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id,
      s.sample_time,
      2 as setting_scope,
      'statements_extension',
      'pg_stat_statements',
      'pg_stat_statements',
      'pg_stat_statements',
      null,
      null,
      null,
      false
    FROM samples s LEFT OUTER JOIN  v_sample_settings prm ON
      (s.server_id, s.sample_id, prm.name, prm.setting_scope) =
      (prm.server_id, prm.sample_id, 'statements_extension', 2)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting != 'pg_stat_statements');

    -- Dynamic statements query
    st_query := format(
      'SELECT '
        'st.userid,'
        'st.userid::regrole AS username,'
        'st.dbid,'
        'st.queryid,'
        '{statements_fields} '
      'FROM '
        '{statements_view} st '
    );

    st_query := replace(st_query, '{statements_view}',
      format('%1$I.pg_stat_statements(false)',
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        )
      )
    );

    -- pg_stat_statements versions
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7' THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
          'NULL as plans,'
          'NULL as total_plan_time,'
          'NULL as min_plan_time,'
          'NULL as max_plan_time,'
          'NULL as mean_plan_time,'
          'NULL as stddev_plan_time,'
          'st.calls,'
          'st.total_time as total_exec_time,'
          'st.min_time as min_exec_time,'
          'st.max_time as max_exec_time,'
          'st.mean_time as mean_exec_time,'
          'st.stddev_time as stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'NULL as wal_records,'
          'NULL as wal_fpi,'
          'NULL as wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.8' THEN
        st_query := replace(st_query, '{statements_fields}',
          'true as toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.9' THEN
        st_query := replace(st_query, '{statements_fields}',
          'st.toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'NULL as jit_functions, '
          'NULL as jit_generation_time, '
          'NULL as jit_inlining_count, '
          'NULL as jit_inlining_time, '
          'NULL as jit_optimization_count, '
          'NULL as jit_optimization_time, '
          'NULL as jit_emission_count, '
          'NULL as jit_emission_time, '
          'NULL as temp_blk_read_time, '
          'NULL as temp_blk_write_time '
        );
      WHEN '1.10' THEN
        st_query := replace(st_query, '{statements_fields}',
          'st.toplevel,'
          'st.plans,'
          'st.total_plan_time,'
          'st.min_plan_time,'
          'st.max_plan_time,'
          'st.mean_plan_time,'
          'st.stddev_plan_time,'
          'st.calls,'
          'st.total_exec_time,'
          'st.min_exec_time,'
          'st.max_exec_time,'
          'st.mean_exec_time,'
          'st.stddev_exec_time,'
          'st.rows,'
          'st.shared_blks_hit,'
          'st.shared_blks_read,'
          'st.shared_blks_dirtied,'
          'st.shared_blks_written,'
          'st.local_blks_hit,'
          'st.local_blks_read,'
          'st.local_blks_dirtied,'
          'st.local_blks_written,'
          'st.temp_blks_read,'
          'st.temp_blks_written,'
          'st.blk_read_time,'
          'st.blk_write_time,'
          'st.wal_records,'
          'st.wal_fpi,'
          'st.wal_bytes, '
          'st.jit_functions, '
          'st.jit_generation_time, '
          'st.jit_inlining_count, '
          'st.jit_inlining_time, '
          'st.jit_optimization_count, '
          'st.jit_optimization_time, '
          'st.jit_emission_count, '
          'st.jit_emission_time, '
          'st.temp_blk_read_time, '
          'st.temp_blk_write_time '
        );
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE; -- pg_stat_statememts versions

    -- Get statements data
    INSERT INTO last_stat_statements (
        server_id,
        sample_id,
        userid,
        username,
        datid,
        queryid,
        plans,
        total_plan_time,
        min_plan_time,
        max_plan_time,
        mean_plan_time,
        stddev_plan_time,
        calls,
        total_exec_time,
        min_exec_time,
        max_exec_time,
        mean_exec_time,
        stddev_exec_time,
        rows,
        shared_blks_hit,
        shared_blks_read,
        shared_blks_dirtied,
        shared_blks_written,
        local_blks_hit,
        local_blks_read,
        local_blks_dirtied,
        local_blks_written,
        temp_blks_read,
        temp_blks_written,
        blk_read_time,
        blk_write_time,
        wal_records,
        wal_fpi,
        wal_bytes,
        toplevel,
        in_sample,
        jit_functions,
        jit_generation_time,
        jit_inlining_count,
        jit_inlining_time,
        jit_optimization_count,
        jit_optimization_time,
        jit_emission_count,
        jit_emission_time,
        temp_blk_read_time,
        temp_blk_write_time
      )
    SELECT
      sserver_id,
      s_id,
      dbl.userid,
      dbl.username,
      dbl.datid,
      dbl.queryid,
      dbl.plans,
      dbl.total_plan_time,
      dbl.min_plan_time,
      dbl.max_plan_time,
      dbl.mean_plan_time,
      dbl.stddev_plan_time,
      dbl.calls,
      dbl.total_exec_time,
      dbl.min_exec_time,
      dbl.max_exec_time,
      dbl.mean_exec_time,
      dbl.stddev_exec_time,
      dbl.rows,
      dbl.shared_blks_hit,
      dbl.shared_blks_read,
      dbl.shared_blks_dirtied,
      dbl.shared_blks_written,
      dbl.local_blks_hit,
      dbl.local_blks_read,
      dbl.local_blks_dirtied,
      dbl.local_blks_written,
      dbl.temp_blks_read,
      dbl.temp_blks_written,
      dbl.blk_read_time,
      dbl.blk_write_time,
      dbl.wal_records,
      dbl.wal_fpi,
      dbl.wal_bytes,
      dbl.toplevel,
      false,
      dbl.jit_functions,
      dbl.jit_generation_time,
      dbl.jit_inlining_count,
      dbl.jit_inlining_time,
      dbl.jit_optimization_count,
      dbl.jit_optimization_time,
      dbl.jit_emission_count,
      dbl.jit_emission_time,
      dbl.temp_blk_read_time,
      dbl.temp_blk_write_time
    FROM dblink('server_connection',st_query)
    AS dbl (
      -- pg_stat_statements fields
        userid              oid,
        username            name,
        datid               oid,
        queryid             bigint,
        toplevel            boolean,
        plans               bigint,
        total_plan_time     double precision,
        min_plan_time       double precision,
        max_plan_time       double precision,
        mean_plan_time      double precision,
        stddev_plan_time    double precision,
        calls               bigint,
        total_exec_time     double precision,
        min_exec_time       double precision,
        max_exec_time       double precision,
        mean_exec_time      double precision,
        stddev_exec_time    double precision,
        rows                bigint,
        shared_blks_hit     bigint,
        shared_blks_read    bigint,
        shared_blks_dirtied bigint,
        shared_blks_written bigint,
        local_blks_hit      bigint,
        local_blks_read     bigint,
        local_blks_dirtied  bigint,
        local_blks_written  bigint,
        temp_blks_read      bigint,
        temp_blks_written   bigint,
        blk_read_time       double precision,
        blk_write_time      double precision,
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        jit_functions       bigint,
        jit_generation_time double precision,
        jit_inlining_count  bigint,
        jit_inlining_time   double precision,
        jit_optimization_count  bigint,
        jit_optimization_time   double precision,
        jit_emission_count  bigint,
        jit_emission_time   double precision,
        temp_blk_read_time  double precision,
        temp_blk_write_time double precision
      );
    EXECUTE format('ANALYZE last_stat_statements_srv%1$s',
      sserver_id);

    -- Rusage data collection when available
    IF
      (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_kcache'
      )
    THEN
      -- Dynamic rusage query
      st_query := format(
        'SELECT '
          'kc.userid,'
          'kc.dbid,'
          'kc.queryid,'
          '{kcache_fields} '
        'FROM '
          '{kcache_view} kc '
      );

      st_query := replace(st_query, '{kcache_view}',
        format('%1$I.pg_stat_kcache()',
          (
            SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
              AS x(extname text, extnamespace text)
            WHERE extname = 'pg_stat_kcache'
          )
        )
      );

      CASE -- pg_stat_kcache versions
        (
          SELECT extversion
          FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extversion text)
          WHERE extname = 'pg_stat_kcache'
        )
        -- pg_stat_kcache v.2.1.0 - 2.1.3
        WHEN '2.1.0','2.1.1','2.1.2','2.1.3' THEN
          st_query := replace(st_query, '{kcache_fields}',
            'true as toplevel,'
            'NULL as plan_user_time,'
            'NULL as plan_system_time,'
            'NULL as plan_minflts,'
            'NULL as plan_majflts,'
            'NULL as plan_nswaps,'
            'NULL as plan_reads,'
            'NULL as plan_writes,'
            'NULL as plan_msgsnds,'
            'NULL as plan_msgrcvs,'
            'NULL as plan_nsignals,'
            'NULL as plan_nvcsws,'
            'NULL as plan_nivcsws,'
            'kc.user_time as exec_user_time,'
            'kc.system_time as exec_system_time,'
            'kc.minflts as exec_minflts,'
            'kc.majflts as exec_majflts,'
            'kc.nswaps as exec_nswaps,'
            'kc.reads as exec_reads,'
            'kc.writes as exec_writes,'
            'kc.msgsnds as exec_msgsnds,'
            'kc.msgrcvs as exec_msgrcvs,'
            'kc.nsignals as exec_nsignals,'
            'kc.nvcsws as exec_nvcsws,'
            'kc.nivcsws as exec_nivcsws '
          );
        -- pg_stat_kcache v.2.2.0, 2.2.1, 2.2.2
        WHEN '2.2.0', '2.2.1', '2.2.2' THEN
          st_query := replace(st_query, '{kcache_fields}',
            'kc.top as toplevel,'
            'kc.plan_user_time as plan_user_time,'
            'kc.plan_system_time as plan_system_time,'
            'kc.plan_minflts as plan_minflts,'
            'kc.plan_majflts as plan_majflts,'
            'kc.plan_nswaps as plan_nswaps,'
            'kc.plan_reads as plan_reads,'
            'kc.plan_writes as plan_writes,'
            'kc.plan_msgsnds as plan_msgsnds,'
            'kc.plan_msgrcvs as plan_msgrcvs,'
            'kc.plan_nsignals as plan_nsignals,'
            'kc.plan_nvcsws as plan_nvcsws,'
            'kc.plan_nivcsws as plan_nivcsws,'
            'kc.exec_user_time as exec_user_time,'
            'kc.exec_system_time as exec_system_time,'
            'kc.exec_minflts as exec_minflts,'
            'kc.exec_majflts as exec_majflts,'
            'kc.exec_nswaps as exec_nswaps,'
            'kc.exec_reads as exec_reads,'
            'kc.exec_writes as exec_writes,'
            'kc.exec_msgsnds as exec_msgsnds,'
            'kc.exec_msgrcvs as exec_msgrcvs,'
            'kc.exec_nsignals as exec_nsignals,'
            'kc.exec_nvcsws as exec_nvcsws,'
            'kc.exec_nivcsws as exec_nivcsws '
          );
        ELSE
          st_query := NULL;
      END CASE; -- pg_stat_kcache versions

      IF st_query IS NOT NULL THEN
        INSERT INTO last_stat_kcache(
          server_id,
          sample_id,
          userid,
          datid,
          toplevel,
          queryid,
          plan_user_time,
          plan_system_time,
          plan_minflts,
          plan_majflts,
          plan_nswaps,
          plan_reads,
          plan_writes,
          plan_msgsnds,
          plan_msgrcvs,
          plan_nsignals,
          plan_nvcsws,
          plan_nivcsws,
          exec_user_time,
          exec_system_time,
          exec_minflts,
          exec_majflts,
          exec_nswaps,
          exec_reads,
          exec_writes,
          exec_msgsnds,
          exec_msgrcvs,
          exec_nsignals,
          exec_nvcsws,
          exec_nivcsws
        )
        SELECT
          sserver_id,
          s_id,
          dbl.userid,
          dbl.datid,
          dbl.toplevel,
          dbl.queryid,
          dbl.plan_user_time  AS plan_user_time,
          dbl.plan_system_time  AS plan_system_time,
          dbl.plan_minflts  AS plan_minflts,
          dbl.plan_majflts  AS plan_majflts,
          dbl.plan_nswaps  AS plan_nswaps,
          dbl.plan_reads  AS plan_reads,
          dbl.plan_writes  AS plan_writes,
          dbl.plan_msgsnds  AS plan_msgsnds,
          dbl.plan_msgrcvs  AS plan_msgrcvs,
          dbl.plan_nsignals  AS plan_nsignals,
          dbl.plan_nvcsws  AS plan_nvcsws,
          dbl.plan_nivcsws  AS plan_nivcsws,
          dbl.exec_user_time  AS exec_user_time,
          dbl.exec_system_time  AS exec_system_time,
          dbl.exec_minflts  AS exec_minflts,
          dbl.exec_majflts  AS exec_majflts,
          dbl.exec_nswaps  AS exec_nswaps,
          dbl.exec_reads  AS exec_reads,
          dbl.exec_writes  AS exec_writes,
          dbl.exec_msgsnds  AS exec_msgsnds,
          dbl.exec_msgrcvs  AS exec_msgrcvs,
          dbl.exec_nsignals  AS exec_nsignals,
          dbl.exec_nvcsws  AS exec_nvcsws,
          dbl.exec_nivcsws  AS exec_nivcsws
        FROM dblink('server_connection',st_query)
        AS dbl (
          userid            oid,
          datid             oid,
          queryid           bigint,
          toplevel          boolean,
          plan_user_time    double precision,
          plan_system_time  double precision,
          plan_minflts      bigint,
          plan_majflts      bigint,
          plan_nswaps       bigint,
          plan_reads        bigint,
          plan_writes       bigint,
          plan_msgsnds      bigint,
          plan_msgrcvs      bigint,
          plan_nsignals     bigint,
          plan_nvcsws       bigint,
          plan_nivcsws      bigint,
          exec_user_time    double precision,
          exec_system_time  double precision,
          exec_minflts      bigint,
          exec_majflts      bigint,
          exec_nswaps       bigint,
          exec_reads        bigint,
          exec_writes       bigint,
          exec_msgsnds      bigint,
          exec_msgrcvs      bigint,
          exec_nsignals     bigint,
          exec_nvcsws       bigint,
          exec_nivcsws      bigint
        ) JOIN last_stat_statements lss USING (userid, datid, queryid, toplevel)
        WHERE
          (lss.server_id, lss.sample_id) = (sserver_id, s_id);
        EXECUTE format('ANALYZE last_stat_kcache_srv%1$s',
          sserver_id);
      END IF; -- st_query is not null
    END IF; -- pg_stat_kcache extension is available

    PERFORM mark_pg_stat_statements(sserver_id, s_id, topn);

    -- Get queries texts
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8' THEN
        st_query :=
          'SELECT userid, dbid, true AS toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      WHEN '1.9', '1.10' THEN
        st_query :=
          'SELECT userid, dbid, toplevel, queryid, '||
          $o$regexp_replace(query,$i$\s+$i$,$i$ $i$,$i$g$i$) AS query $o$ ||
          'FROM %1$I.pg_stat_statements(true) '
          'WHERE queryid IN (%s)';
      ELSE
        RAISE 'Unsupported pg_stat_statements extension version.';
    END CASE;

    -- Substitute pg_stat_statements extension schema and queries list
    st_query := format(st_query,
        (
          SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
            AS x(extname text, extnamespace text)
          WHERE extname = 'pg_stat_statements'
        ),
        (
          SELECT string_agg(queryid::text,',')
          FROM last_stat_statements
          WHERE
            (server_id, sample_id, in_sample) =
            (sserver_id, s_id, true)
        )
    );

    -- Now we can save statement
    FOR qres IN (
      SELECT
        userid,
        datid,
        toplevel,
        queryid,
        query
      FROM dblink('server_connection',st_query) AS
        dbl(
            userid    oid,
            datid     oid,
            toplevel  boolean,
            queryid   bigint,
            query     text
          )
        JOIN last_stat_statements lst USING (userid, datid, toplevel, queryid)
      WHERE
        (lst.server_id, lst.sample_id, lst.in_sample) =
        (sserver_id, s_id, true)
    )
    LOOP
      -- statement texts
      INSERT INTO stmt_list AS isl (
          server_id,
          last_sample_id,
          queryid_md5,
          query
        )
      VALUES (
          sserver_id,
          NULL,
          md5(COALESCE(qres.query, '')),
          qres.query
        )
      ON CONFLICT ON CONSTRAINT pk_stmt_list
      DO UPDATE SET last_sample_id = NULL
      WHERE
        isl.last_sample_id IS NOT NULL;

      -- bind queryid to queryid_md5 for this sample
      -- different text queries can have the same queryid
      -- between samples
      UPDATE last_stat_statements SET queryid_md5 = md5(COALESCE(qres.query, ''))
      WHERE (server_id, sample_id, userid, datid, toplevel, queryid) =
        (sserver_id, s_id, qres.userid, qres.datid, qres.toplevel, qres.queryid);
    END LOOP; -- over sample statements

    -- Flushing pg_stat_kcache
    CASE (
        SELECT extversion FROM jsonb_to_recordset(properties #> '{extensions}')
          AS x(extname text, extversion text)
        WHERE extname = 'pg_stat_kcache'
    )
      WHEN '2.1.0','2.1.1','2.1.2','2.1.3','2.2.0','2.2.1' THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_kcache_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_kcache'
            )
          )
        ) AS t(res char(1));
      ELSE
        NULL;
    END CASE;

    -- Flushing statements
    CASE (
        SELECT extversion
        FROM jsonb_to_recordset(properties #> '{extensions}')
          AS ext(extname text, extversion text)
        WHERE extname = 'pg_stat_statements'
      )
      -- pg_stat_statements v 1.3-1.8
      WHEN '1.3','1.4','1.5','1.6','1.7','1.8','1.9','1.10' THEN
        SELECT * INTO qres FROM dblink('server_connection',
          format('SELECT %1$I.pg_stat_statements_reset()',
            (
              SELECT extnamespace FROM jsonb_to_recordset(properties #> '{extensions}')
                AS x(extname text, extnamespace text)
              WHERE extname = 'pg_stat_statements'
            )
          )
        ) AS t(res char(1));
      ELSE
        RAISE 'Unsupported pg_stat_statements version.';
    END CASE;

    -- Save the diffs in a sample
    PERFORM save_pg_stat_statements(sserver_id, s_id);
    -- Delete obsolete last_* data
    DELETE FROM last_stat_kcache WHERE server_id = sserver_id AND sample_id < s_id;
    DELETE FROM last_stat_statements WHERE server_id = sserver_id AND sample_id < s_id;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION save_pg_stat_statements(IN sserver_id integer, IN s_id integer)
RETURNS void
SET search_path=@extschema@ AS $$
  -- This function performs save marked statements data in sample tables
  -- User names
  INSERT INTO roles_list AS irl (
    server_id,
    last_sample_id,
    userid,
    username
  )
  SELECT DISTINCT
    sserver_id,
    NULL::integer,
    st.userid,
    COALESCE(st.username, '_unknown_')
  FROM
    last_stat_statements st
  WHERE (st.server_id, st.sample_id, in_sample) = (sserver_id, s_id, true)
  ON CONFLICT ON CONSTRAINT pk_roles_list
  DO UPDATE SET
    (last_sample_id, username) =
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  WHERE
    (irl.last_sample_id, irl.username) IS DISTINCT FROM
    (EXCLUDED.last_sample_id, EXCLUDED.username)
  ;

  -- Statement stats
  INSERT INTO sample_statements(
    server_id,
    sample_id,
    userid,
    datid,
    toplevel,
    queryid,
    queryid_md5,
    plans,
    total_plan_time,
    min_plan_time,
    max_plan_time,
    mean_plan_time,
    stddev_plan_time,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  )
  SELECT
    sserver_id,
    s_id,
    userid,
    datid,
    toplevel,
    queryid,
    queryid_md5,
    plans,
    total_plan_time,
    min_plan_time,
    max_plan_time,
    mean_plan_time,
    stddev_plan_time,
    calls,
    total_exec_time,
    min_exec_time,
    max_exec_time,
    mean_exec_time,
    stddev_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  FROM
    last_stat_statements JOIN stmt_list USING (server_id, queryid_md5)
  WHERE
    (server_id, sample_id, in_sample) = (sserver_id, s_id, true);

  /*
  * Aggregated statements stats
  */
  INSERT INTO sample_statements_total(
    server_id,
    sample_id,
    datid,
    plans,
    total_plan_time,
    calls,
    total_exec_time,
    rows,
    shared_blks_hit,
    shared_blks_read,
    shared_blks_dirtied,
    shared_blks_written,
    local_blks_hit,
    local_blks_read,
    local_blks_dirtied,
    local_blks_written,
    temp_blks_read,
    temp_blks_written,
    blk_read_time,
    blk_write_time,
    wal_records,
    wal_fpi,
    wal_bytes,
    statements,
    jit_functions,
    jit_generation_time,
    jit_inlining_count,
    jit_inlining_time,
    jit_optimization_count,
    jit_optimization_time,
    jit_emission_count,
    jit_emission_time,
    temp_blk_read_time,
    temp_blk_write_time
  )
  SELECT
    server_id,
    sample_id,
    datid,
    sum(lss.plans),
    sum(lss.total_plan_time),
    sum(lss.calls),
    sum(lss.total_exec_time),
    sum(lss.rows),
    sum(lss.shared_blks_hit),
    sum(lss.shared_blks_read),
    sum(lss.shared_blks_dirtied),
    sum(lss.shared_blks_written),
    sum(lss.local_blks_hit),
    sum(lss.local_blks_read),
    sum(lss.local_blks_dirtied),
    sum(lss.local_blks_written),
    sum(lss.temp_blks_read),
    sum(lss.temp_blks_written),
    sum(lss.blk_read_time),
    sum(lss.blk_write_time),
    sum(lss.wal_records),
    sum(lss.wal_fpi),
    sum(lss.wal_bytes),
    count(*),
    sum(lss.jit_functions),
    sum(lss.jit_generation_time),
    sum(lss.jit_inlining_count),
    sum(lss.jit_inlining_time),
    sum(lss.jit_optimization_count),
    sum(lss.jit_optimization_time),
    sum(lss.jit_emission_count),
    sum(lss.jit_emission_time),
    sum(lss.temp_blk_read_time),
    sum(lss.temp_blk_write_time)
  FROM
    last_stat_statements lss
    -- In case of already dropped database
    JOIN sample_stat_database ssd USING (server_id, sample_id, datid)
  WHERE
    (server_id, sample_id) = (sserver_id, s_id)
  GROUP BY
    server_id,
    sample_id,
    datid
  ;

  /*
  * If rusage data is available we should just save it in sample for saved
  * statements
  */
  INSERT INTO sample_kcache (
      server_id,
      sample_id,
      userid,
      datid,
      queryid,
      queryid_md5,
      plan_user_time,
      plan_system_time,
      plan_minflts,
      plan_majflts,
      plan_nswaps,
      plan_reads,
      plan_writes,
      plan_msgsnds,
      plan_msgrcvs,
      plan_nsignals,
      plan_nvcsws,
      plan_nivcsws,
      exec_user_time,
      exec_system_time,
      exec_minflts,
      exec_majflts,
      exec_nswaps,
      exec_reads,
      exec_writes,
      exec_msgsnds,
      exec_msgrcvs,
      exec_nsignals,
      exec_nvcsws,
      exec_nivcsws,
      toplevel
  )
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.userid,
    cur.datid,
    cur.queryid,
    sst.queryid_md5,
    cur.plan_user_time,
    cur.plan_system_time,
    cur.plan_minflts,
    cur.plan_majflts,
    cur.plan_nswaps,
    cur.plan_reads,
    cur.plan_writes,
    cur.plan_msgsnds,
    cur.plan_msgrcvs,
    cur.plan_nsignals,
    cur.plan_nvcsws,
    cur.plan_nivcsws,
    cur.exec_user_time,
    cur.exec_system_time,
    cur.exec_minflts,
    cur.exec_majflts,
    cur.exec_nswaps,
    cur.exec_reads,
    cur.exec_writes,
    cur.exec_msgsnds,
    cur.exec_msgrcvs,
    cur.exec_nsignals,
    cur.exec_nvcsws,
    cur.exec_nivcsws,
    cur.toplevel
  FROM
    last_stat_kcache cur JOIN last_stat_statements sst ON
      (sst.server_id, cur.server_id, sst.sample_id, sst.userid, sst.datid, sst.queryid, sst.toplevel) =
      (sserver_id, sserver_id, cur.sample_id, cur.userid, cur.datid, cur.queryid, cur.toplevel)
  WHERE
    (cur.server_id, cur.sample_id, sst.in_sample) = (sserver_id, s_id, true)
    AND sst.queryid_md5 IS NOT NULL;

  -- Aggregated pg_stat_kcache data
  INSERT INTO sample_kcache_total(
    server_id,
    sample_id,
    datid,
    plan_user_time,
    plan_system_time,
    plan_minflts,
    plan_majflts,
    plan_nswaps,
    plan_reads,
    plan_writes,
    plan_msgsnds,
    plan_msgrcvs,
    plan_nsignals,
    plan_nvcsws,
    plan_nivcsws,
    exec_user_time,
    exec_system_time,
    exec_minflts,
    exec_majflts,
    exec_nswaps,
    exec_reads,
    exec_writes,
    exec_msgsnds,
    exec_msgrcvs,
    exec_nsignals,
    exec_nvcsws,
    exec_nivcsws,
    statements
  )
  SELECT
    cur.server_id,
    cur.sample_id,
    cur.datid,
    sum(plan_user_time),
    sum(plan_system_time),
    sum(plan_minflts),
    sum(plan_majflts),
    sum(plan_nswaps),
    sum(plan_reads),
    sum(plan_writes),
    sum(plan_msgsnds),
    sum(plan_msgrcvs),
    sum(plan_nsignals),
    sum(plan_nvcsws),
    sum(plan_nivcsws),
    sum(exec_user_time),
    sum(exec_system_time),
    sum(exec_minflts),
    sum(exec_majflts),
    sum(exec_nswaps),
    sum(exec_reads),
    sum(exec_writes),
    sum(exec_msgsnds),
    sum(exec_msgrcvs),
    sum(exec_nsignals),
    sum(exec_nvcsws),
    sum(exec_nivcsws),
    count(*)
  FROM
    last_stat_kcache cur
    -- In case of already dropped database
    JOIN sample_stat_database db USING (server_id, sample_id, datid)
  WHERE
    (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
    toplevel
  GROUP BY
    server_id,
    sample_id,
    datid
  ;
$$ LANGUAGE sql;
CREATE FUNCTION init_sample(IN sserver_id integer
) RETURNS jsonb SET search_path=@extschema@ SET lock_timeout=300000 AS $$
DECLARE
    server_properties jsonb = '{"extensions":[],"settings":[],"timings":{},"properties":{}}'; -- version, extensions, etc.
    qres              record;
    server_connstr    text;

    server_query      text;
    server_host       text = NULL;
BEGIN
    -- Get server connstr
    SELECT properties INTO server_properties FROM get_connstr(sserver_id, server_properties);

    -- Getting timing collection setting
    BEGIN
        SELECT current_setting('pg_profile.track_sample_timings')::boolean AS collect_timings
          INTO qres;
        server_properties := jsonb_set(server_properties,
          '{collect_timings}',
          to_jsonb(qres.collect_timings)
        );
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{collect_timings}',
            to_jsonb(false)
          );
    END;

    -- Getting TopN setting
    BEGIN
        SELECT current_setting('pg_profile.topn')::integer AS topn INTO qres;
        server_properties := jsonb_set(server_properties,'{properties,topn}',to_jsonb(qres.topn));
    EXCEPTION
        WHEN OTHERS THEN
          server_properties := jsonb_set(server_properties,
            '{properties,topn}',
            to_jsonb(20)
          );
    END;

    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;

    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    IF dblink_get_connections() @> ARRAY['server_connection'] THEN
        PERFORM dblink_disconnect('server_connection');
    END IF;

    -- Only one running take_sample() function allowed per server!
    -- Explicitly lock server in servers table
    BEGIN
        SELECT * INTO qres FROM servers WHERE server_id = sserver_id FOR UPDATE NOWAIT;
    EXCEPTION
        WHEN OTHERS THEN RAISE 'Can''t get lock on server. Is there another take_sample() function running on this server?';
    END;
    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect}',jsonb_build_object('start',clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Server connection
    PERFORM dblink_connect('server_connection', server_properties #>> '{properties,server_connstr}');
    -- Transaction
    PERFORM dblink('server_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
    -- Setting application name
    PERFORM dblink('server_connection','SET application_name=''pg_profile''');
    -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
    PERFORM dblink('server_connection','SET lock_timeout=3000');
    -- Reset search_path for security reasons
    PERFORM dblink('server_connection','SET search_path=''''');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,connect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,get server environment}',jsonb_build_object('start',clock_timestamp()));
    END IF;
    -- Get settings values for the server
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT name, '
          'reset_val, '
          'unit, '
          'pending_restart '
          'FROM pg_catalog.pg_settings '
          'WHERE name IN ('
            '''server_version_num'''
          ')')
        AS dbl(name text, reset_val text, unit text, pending_restart boolean)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"settings",0}',to_jsonb(qres));
    END LOOP;

    -- Get extensions, that we need to perform statements stats collection
    FOR qres IN
      SELECT * FROM dblink('server_connection',
          'SELECT extname, '
          'extnamespace::regnamespace::name AS extnamespace, '
          'extversion '
          'FROM pg_catalog.pg_extension '
          'WHERE extname IN ('
            '''pg_stat_statements'','
            '''pg_wait_sampling'','
            '''pg_stat_kcache'''
          ')')
        AS dbl(extname name, extnamespace name, extversion text)
    LOOP
      server_properties := jsonb_insert(server_properties,'{"extensions",0}',to_jsonb(qres));
    END LOOP;

    -- Check system identifier
    WITH remote AS (
      SELECT
        dbl.system_identifier
      FROM dblink('server_connection',
        'SELECT system_identifier '
        'FROM pg_catalog.pg_control_system()'
      ) AS dbl (system_identifier bigint)
    )
    SELECT min(reset_val::bigint) != (
        SELECT
          system_identifier
        FROM remote
      ) AS sysid_changed,
      (
        SELECT
          s.server_name = 'local' AND cs.system_identifier != r.system_identifier
        FROM
          pg_catalog.pg_control_system() cs
          CROSS JOIN remote r
          JOIN servers s ON (s.server_id = sserver_id)
      ) AS local_missmatch
      INTO STRICT qres
    FROM sample_settings
    WHERE server_id = sserver_id AND name = 'system_identifier';
    IF qres.sysid_changed THEN
      RAISE 'Server system_identifier has changed! '
        'Ensure server connection string is correct. '
        'Consider creating a new server for this cluster.';
    END IF;
    IF qres.local_missmatch THEN
      RAISE 'Local system_identifier does not match '
        'with server specified by connection string of '
        '"local" server';
    END IF;

    RETURN server_properties;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean
) RETURNS integer SET search_path=@extschema@ AS $$
DECLARE
    s_id              integer;
    topn              integer;
    ret               integer;
    server_properties jsonb;
    qres              record;
    settings_refresh  boolean = true;
    collect_timings   boolean = false;

    server_query      text;
BEGIN
    -- Initialize sample
    server_properties := init_sample(sserver_id);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;

    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    topn := (server_properties #>> '{properties,topn}')::integer;

    -- Creating a new sample record
    UPDATE servers SET last_sample_id = last_sample_id + 1 WHERE server_id = sserver_id
      RETURNING last_sample_id INTO s_id;
    INSERT INTO samples(sample_time,server_id,sample_id)
      VALUES (now(),sserver_id,s_id);

    -- Getting max_sample_age setting
    BEGIN
        ret := COALESCE(current_setting('pg_profile.max_sample_age')::integer);
    EXCEPTION
        WHEN OTHERS THEN ret := 7;
    END;
    -- Applying skip sizes policy
    SELECT * INTO qres FROM servers WHERE server_id = sserver_id;
    IF skip_sizes IS NULL THEN
      IF num_nulls(qres.size_smp_wnd_start, qres.size_smp_wnd_dur, qres.size_smp_interval) > 0 THEN
        skip_sizes := false;
      ELSE
        /*
        Skip sizes collection if there was a sample with sizes recently
        or if we are not in size collection time window
        */
        SELECT
          count(*) > 0 OR
          NOT
          CASE WHEN timezone('UTC',current_time) > timezone('UTC',qres.size_smp_wnd_start) THEN
            timezone('UTC',now()) <=
            timezone('UTC',now())::date +
            timezone('UTC',qres.size_smp_wnd_start) +
            qres.size_smp_wnd_dur
          ELSE
            timezone('UTC',now()) <=
            timezone('UTC',now() - interval '1 day')::date +
            timezone('UTC',qres.size_smp_wnd_start) +
            qres.size_smp_wnd_dur
          END
            INTO STRICT skip_sizes
        FROM
          sample_stat_tables_total st
          JOIN samples s USING (server_id, sample_id)
        WHERE
          server_id = sserver_id
          AND st.relsize_diff IS NOT NULL
          AND sample_time > now() - qres.size_smp_interval;
      END IF;
    END IF;

    -- Collecting postgres parameters
    /* We might refresh all parameters if version() was changed
    * This is needed for deleting obsolete parameters, not appearing in new
    * Postgres version.
    */
    SELECT ss.setting != dblver.version INTO settings_refresh
    FROM v_sample_settings ss, dblink('server_connection','SELECT version() as version') AS dblver (version text)
    WHERE ss.server_id = sserver_id AND ss.sample_id = s_id AND ss.name='version' AND ss.setting_scope = 2;
    settings_refresh := COALESCE(settings_refresh,true);

    -- Constructing server sql query for settings
    server_query := 'SELECT 1 as setting_scope,name,setting,reset_val,boot_val,unit,sourcefile,sourceline,pending_restart '
      'FROM pg_catalog.pg_settings '
      'UNION ALL SELECT 2 as setting_scope,''version'',version(),version(),NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_postmaster_start_time'','
      'pg_catalog.pg_postmaster_start_time()::text,'
      'pg_catalog.pg_postmaster_start_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''pg_conf_load_time'','
      'pg_catalog.pg_conf_load_time()::text,pg_catalog.pg_conf_load_time()::text,NULL,NULL,NULL,NULL,False '
      'UNION ALL SELECT 2 as setting_scope,''system_identifier'','
      'system_identifier::text,system_identifier::text,system_identifier::text,'
      'NULL,NULL,NULL,False FROM pg_catalog.pg_control_system()';

    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id as server_id,
      s.sample_time as first_seen,
      cur.setting_scope,
      cur.name,
      cur.setting,
      cur.reset_val,
      cur.boot_val,
      cur.unit,
      cur.sourcefile,
      cur.sourceline,
      cur.pending_restart
    FROM
      sample_settings lst JOIN (
        -- Getting last versions of settings
        SELECT server_id, name, max(first_seen) as first_seen
        FROM sample_settings
        WHERE server_id = sserver_id AND (
          NOT settings_refresh
          -- system identifier shouldn't have a duplicate in case of version change
          -- this breaks export/import procedures, as those are related to this ID
          OR name = 'system_identifier'
        )
        GROUP BY server_id, name
      ) lst_times
      USING (server_id, name, first_seen)
      -- Getting current settings values
      RIGHT OUTER JOIN dblink('server_connection',server_query
          ) AS cur (
            setting_scope smallint,
            name text,
            setting text,
            reset_val text,
            boot_val text,
            unit text,
            sourcefile text,
            sourceline integer,
            pending_restart boolean
          )
        USING (setting_scope, name)
      JOIN samples s ON (s.server_id = sserver_id AND s.sample_id = s_id)
    WHERE
      cur.reset_val IS NOT NULL AND (
        lst.name IS NULL
        OR cur.reset_val != lst.reset_val
        OR cur.pending_restart != lst.pending_restart
        OR lst.sourcefile != cur.sourcefile
        OR lst.sourceline != cur.sourceline
        OR lst.unit != cur.unit
      );

    INSERT INTO sample_settings(
      server_id,
      first_seen,
      setting_scope,
      name,
      setting,
      reset_val,
      boot_val,
      unit,
      sourcefile,
      sourceline,
      pending_restart
    )
    SELECT
      s.server_id,
      s.sample_time,
      1 as setting_scope,
      'pg_profile.topn',
      topn,
      topn,
      topn,
      null,
      null,
      null,
      false
    FROM samples s LEFT OUTER JOIN  v_sample_settings prm ON
      (s.server_id = prm.server_id AND s.sample_id = prm.sample_id AND prm.name = 'pg_profile.topn' AND prm.setting_scope = 1)
    WHERE s.server_id = sserver_id AND s.sample_id = s_id AND (prm.setting IS NULL OR prm.setting::integer != topn);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,get server environment,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct pg_stat_database query
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'dbs.session_time, '
            'dbs.active_time, '
            'dbs.idle_in_transaction_time, '
            'dbs.sessions, '
            'dbs.sessions_abandoned, '
            'dbs.sessions_fatal, '
            'dbs.sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'dbs.checksum_failures, '
            'dbs.checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 120000
      )
      THEN
        server_query := 'SELECT '
            'dbs.datid, '
            'dbs.datname, '
            'dbs.xact_commit, '
            'dbs.xact_rollback, '
            'dbs.blks_read, '
            'dbs.blks_hit, '
            'dbs.tup_returned, '
            'dbs.tup_fetched, '
            'dbs.tup_inserted, '
            'dbs.tup_updated, '
            'dbs.tup_deleted, '
            'dbs.conflicts, '
            'dbs.temp_files, '
            'dbs.temp_bytes, '
            'dbs.deadlocks, '
            'NULL as checksum_failures, '
            'NULL as checksum_last_failure, '
            'dbs.blk_read_time, '
            'dbs.blk_write_time, '
            'NULL as session_time, '
            'NULL as active_time, '
            'NULL as idle_in_transaction_time, '
            'NULL as sessions, '
            'NULL as sessions_abandoned, '
            'NULL as sessions_fatal, '
            'NULL as sessions_killed, '
            'dbs.stats_reset, '
            'pg_database_size(dbs.datid) as datsize, '
            '0 as datsize_delta, '
            'db.datistemplate, '
            'db.dattablespace, '
            'db.datallowconn '
          'FROM pg_catalog.pg_stat_database dbs '
          'JOIN pg_catalog.pg_database db ON (dbs.datid = db.oid) '
          'WHERE dbs.datname IS NOT NULL';
    END CASE;

    -- pg_stat_database data
    INSERT INTO last_stat_database (
        server_id,
        sample_id,
        datid,
        datname,
        xact_commit,
        xact_rollback,
        blks_read,
        blks_hit,
        tup_returned,
        tup_fetched,
        tup_inserted,
        tup_updated,
        tup_deleted,
        conflicts,
        temp_files,
        temp_bytes,
        deadlocks,
        checksum_failures,
        checksum_last_failure,
        blk_read_time,
        blk_write_time,
        session_time,
        active_time,
        idle_in_transaction_time,
        sessions,
        sessions_abandoned,
        sessions_fatal,
        sessions_killed,
        stats_reset,
        datsize,
        datsize_delta,
        datistemplate,
        dattablespace,
        datallowconn)
    SELECT
        sserver_id,
        s_id,
        datid,
        datname,
        xact_commit AS xact_commit,
        xact_rollback AS xact_rollback,
        blks_read AS blks_read,
        blks_hit AS blks_hit,
        tup_returned AS tup_returned,
        tup_fetched AS tup_fetched,
        tup_inserted AS tup_inserted,
        tup_updated AS tup_updated,
        tup_deleted AS tup_deleted,
        conflicts AS conflicts,
        temp_files AS temp_files,
        temp_bytes AS temp_bytes,
        deadlocks AS deadlocks,
        checksum_failures as checksum_failures,
        checksum_last_failure as checksum_failures,
        blk_read_time AS blk_read_time,
        blk_write_time AS blk_write_time,
        session_time AS session_time,
        active_time AS active_time,
        idle_in_transaction_time AS idle_in_transaction_time,
        sessions AS sessions,
        sessions_abandoned AS sessions_abandoned,
        sessions_fatal AS sessions_fatal,
        sessions_killed AS sessions_killed,
        stats_reset,
        datsize AS datsize,
        datsize_delta AS datsize_delta,
        datistemplate AS datistemplate,
        dattablespace AS dattablespace,
        datallowconn AS datallowconn
    FROM dblink('server_connection',server_query) AS rs (
        datid oid,
        datname name,
        xact_commit bigint,
        xact_rollback bigint,
        blks_read bigint,
        blks_hit bigint,
        tup_returned bigint,
        tup_fetched bigint,
        tup_inserted bigint,
        tup_updated bigint,
        tup_deleted bigint,
        conflicts bigint,
        temp_files bigint,
        temp_bytes bigint,
        deadlocks bigint,
        checksum_failures bigint,
        checksum_last_failure timestamp with time zone,
        blk_read_time double precision,
        blk_write_time double precision,
        session_time double precision,
        active_time double precision,
        idle_in_transaction_time double precision,
        sessions bigint,
        sessions_abandoned bigint,
        sessions_fatal bigint,
        sessions_killed bigint,
        stats_reset timestamp with time zone,
        datsize bigint,
        datsize_delta bigint,
        datistemplate boolean,
        dattablespace oid,
        datallowconn boolean
        );

    EXECUTE format('ANALYZE last_stat_database_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;
    -- Calc stat_database diff
    INSERT INTO sample_stat_database(
      server_id,
      sample_id,
      datid,
      datname,
      xact_commit,
      xact_rollback,
      blks_read,
      blks_hit,
      tup_returned,
      tup_fetched,
      tup_inserted,
      tup_updated,
      tup_deleted,
      conflicts,
      temp_files,
      temp_bytes,
      deadlocks,
      checksum_failures,
      checksum_last_failure,
      blk_read_time,
      blk_write_time,
      session_time,
      active_time,
      idle_in_transaction_time,
      sessions,
      sessions_abandoned,
      sessions_fatal,
      sessions_killed,
      stats_reset,
      datsize,
      datsize_delta,
      datistemplate
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.datid,
        cur.datname,
        cur.xact_commit - COALESCE(lst.xact_commit,0),
        cur.xact_rollback - COALESCE(lst.xact_rollback,0),
        cur.blks_read - COALESCE(lst.blks_read,0),
        cur.blks_hit - COALESCE(lst.blks_hit,0),
        cur.tup_returned - COALESCE(lst.tup_returned,0),
        cur.tup_fetched - COALESCE(lst.tup_fetched,0),
        cur.tup_inserted - COALESCE(lst.tup_inserted,0),
        cur.tup_updated - COALESCE(lst.tup_updated,0),
        cur.tup_deleted - COALESCE(lst.tup_deleted,0),
        cur.conflicts - COALESCE(lst.conflicts,0),
        cur.temp_files - COALESCE(lst.temp_files,0),
        cur.temp_bytes - COALESCE(lst.temp_bytes,0),
        cur.deadlocks - COALESCE(lst.deadlocks,0),
        cur.checksum_failures - COALESCE(lst.checksum_failures,0),
        cur.checksum_last_failure,
        cur.blk_read_time - COALESCE(lst.blk_read_time,0),
        cur.blk_write_time - COALESCE(lst.blk_write_time,0),
        cur.session_time - COALESCE(lst.session_time,0),
        cur.active_time - COALESCE(lst.active_time,0),
        cur.idle_in_transaction_time - COALESCE(lst.idle_in_transaction_time,0),
        cur.sessions - COALESCE(lst.sessions,0),
        cur.sessions_abandoned - COALESCE(lst.sessions_abandoned,0),
        cur.sessions_fatal - COALESCE(lst.sessions_fatal,0),
        cur.sessions_killed - COALESCE(lst.sessions_killed,0),
        cur.stats_reset,
        cur.datsize as datsize,
        cur.datsize - COALESCE(lst.datsize,0) as datsize_delta,
        cur.datistemplate
    FROM last_stat_database cur
      LEFT OUTER JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
        (sserver_id, s_id - 1, cur.datid, cur.datname)
        AND lst.stats_reset IS NOT DISTINCT FROM cur.stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    /*
    * In case of statistics reset full database size, and checksum checksum_failures
    * is incorrectly considered as increment by previous query.
    * So, we need to update it with correct value
    */
    UPDATE sample_stat_database sdb
    SET
      datsize_delta = cur.datsize - lst.datsize,
      checksum_failures = cur.checksum_failures - lst.checksum_failures,
      checksum_last_failure = cur.checksum_last_failure
    FROM
      last_stat_database cur
      JOIN last_stat_database lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.datname) =
        (sserver_id, s_id - 1, cur.datid, cur.datname)
    WHERE cur.stats_reset IS DISTINCT FROM lst.stats_reset AND
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      (sdb.server_id, sdb.sample_id, sdb.datid, sdb.datname) =
      (cur.server_id, cur.sample_id, cur.datid, cur.datname);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate database stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Construct tablespace stats query
    server_query := 'SELECT '
        'oid as tablespaceid,'
        'spcname as tablespacename,'
        'pg_catalog.pg_tablespace_location(oid) as tablespacepath,'
        'pg_catalog.pg_tablespace_size(oid) as size,'
        '0 as size_delta '
        'FROM pg_catalog.pg_tablespace ';

    -- Get tablespace stats
    INSERT INTO last_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      tablespacename,
      tablespacepath,
      size,
      size_delta
    )
    SELECT
      sserver_id,
      s_id,
      dbl.tablespaceid,
      dbl.tablespacename,
      dbl.tablespacepath,
      dbl.size AS size,
      dbl.size_delta AS size_delta
    FROM dblink('server_connection', server_query)
    AS dbl (
        tablespaceid            oid,
        tablespacename          name,
        tablespacepath          text,
        size                    bigint,
        size_delta              bigint
    );

    EXECUTE format('ANALYZE last_stat_tablespaces_srv%1$s',
      sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for statements statistics extension
    CASE
      -- pg_stat_statements statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_stat_statements'
      ) THEN
        PERFORM collect_pg_stat_statements_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect statement stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Search for wait sampling extension
    CASE
      -- pg_wait_sampling statistics collection
      WHEN (
        SELECT count(*) = 1
        FROM jsonb_to_recordset(server_properties #> '{extensions}') AS ext(extname text)
        WHERE extname = 'pg_wait_sampling'
      ) THEN
        PERFORM collect_pg_wait_sampling_stats(server_properties, sserver_id, s_id, topn);
      ELSE
        NULL;
    END CASE;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect wait sampling stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_bgwriter data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer < 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_xlog_location_diff(pg_catalog.pg_last_xlog_replay_location(),''0/00000000'') '
          'ELSE pg_catalog.pg_xlog_location_diff(pg_catalog.pg_current_xlog_location(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
          'THEN pg_catalog.pg_last_xlog_replay_location() '
          'ELSE pg_catalog.pg_current_xlog_location() '
          'END AS wal_lsn,'
          'pg_is_in_recovery() AS in_recovery '
          'FROM pg_catalog.pg_stat_bgwriter';
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 100000
      )
      THEN
        server_query := 'SELECT '
          'checkpoints_timed,'
          'checkpoints_req,'
          'checkpoint_write_time,'
          'checkpoint_sync_time,'
          'buffers_checkpoint,'
          'buffers_clean,'
          'maxwritten_clean,'
          'buffers_backend,'
          'buffers_backend_fsync,'
          'buffers_alloc,'
          'stats_reset,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_last_wal_replay_lsn(),''0/00000000'') '
            'ELSE pg_catalog.pg_wal_lsn_diff(pg_catalog.pg_current_wal_lsn(),''0/00000000'') '
          'END AS wal_size,'
          'CASE WHEN pg_catalog.pg_is_in_recovery() '
            'THEN pg_catalog.pg_last_wal_replay_lsn() '
            'ELSE pg_catalog.pg_current_wal_lsn() '
          'END AS wal_lsn,'
          'pg_catalog.pg_is_in_recovery() as in_recovery '
        'FROM pg_catalog.pg_stat_bgwriter';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_cluster (
        server_id,
        sample_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery)
      SELECT
        sserver_id,
        s_id,
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset,
        wal_size,
        wal_lsn,
        in_recovery
      FROM dblink('server_connection',server_query) AS rs (
        checkpoints_timed bigint,
        checkpoints_req bigint,
        checkpoint_write_time double precision,
        checkpoint_sync_time double precision,
        buffers_checkpoint bigint,
        buffers_clean bigint,
        maxwritten_clean bigint,
        buffers_backend bigint,
        buffers_backend_fsync bigint,
        buffers_alloc bigint,
        stats_reset timestamp with time zone,
        wal_size bigint,
        wal_lsn pg_lsn,
        in_recovery boolean);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_bgwriter,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_wal data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 140000
      )
      THEN
        server_query := 'SELECT '
          'wal_records,'
          'wal_fpi,'
          'wal_bytes,'
          'wal_buffers_full,'
          'wal_write,'
          'wal_sync,'
          'wal_write_time,'
          'wal_sync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_wal';
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_wal (
        server_id,
        sample_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        wal_records,
        wal_fpi,
        wal_bytes,
        wal_buffers_full,
        wal_write,
        wal_sync,
        wal_write_time,
        wal_sync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        wal_records         bigint,
        wal_fpi             bigint,
        wal_bytes           numeric,
        wal_buffers_full    bigint,
        wal_write           bigint,
        wal_sync            bigint,
        wal_write_time      double precision,
        wal_sync_time       double precision,
        stats_reset         timestamp with time zone);
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_wal,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_io}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_io data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 160000
      )
      THEN
        server_query := 'SELECT '
          'backend_type,'
          'object,'
          'context,'
          'reads,'
          'read_time,'
          'writes,'
          'write_time,'
          'writebacks,'
          'writeback_time,'
          'extends,'
          'extend_time,'
          'op_bytes,'
          'hits,'
          'evictions,'
          'reuses,'
          'fsyncs,'
          'fsync_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_io '
          'WHERE greatest('
              'reads,'
              'writes,'
              'writebacks,'
              'extends,'
              'hits,'
              'evictions,'
              'reuses,'
              'fsyncs'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_io (
        server_id,
        sample_id,
        backend_type,
        object,
        context,
        reads,
        read_time,
        writes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        backend_type,
        object,
        context,
        reads,
        read_time,
        writes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        backend_type      text,
        object            text,
        context           text,
        reads             bigint,
        read_time         double precision,
        writes            bigint,
        write_time        double precision,
        writebacks        bigint,
        writeback_time    double precision,
        extends           bigint,
        extend_time       double precision,
        op_bytes          bigint,
        hits              bigint,
        evictions         bigint,
        reuses            bigint,
        fsyncs            bigint,
        fsync_time        double precision,
        stats_reset       timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_io,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_slru}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_slru data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer >= 130000
      )
      THEN
        server_query := 'SELECT '
          'name,'
          'blks_zeroed,'
          'blks_hit,'
          'blks_read,'
          'blks_written,'
          'blks_exists,'
          'flushes,'
          'truncates,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_slru '
          'WHERE greatest('
              'blks_zeroed,'
              'blks_hit,'
              'blks_read,'
              'blks_written,'
              'blks_exists,'
              'flushes,'
              'truncates'
            ') > 0'
          ;
      ELSE
        server_query := NULL;
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_slru (
        server_id,
        sample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      )
      SELECT
        sserver_id,
        s_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        name          text,
        blks_zeroed   bigint,
        blks_hit      bigint,
        blks_read     bigint,
        blks_written  bigint,
        blks_exists   bigint,
        flushes       bigint,
        truncates     bigint,
        stats_reset   timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_slru,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- pg_stat_archiver data
    CASE
      WHEN (
        SELECT count(*) = 1 FROM jsonb_to_recordset(server_properties #> '{settings}')
          AS x(name text, reset_val text)
        WHERE name = 'server_version_num'
          AND reset_val::integer > 90500
      )
      THEN
        server_query := 'SELECT '
          'archived_count,'
          'last_archived_wal,'
          'last_archived_time,'
          'failed_count,'
          'last_failed_wal,'
          'last_failed_time,'
          'stats_reset '
          'FROM pg_catalog.pg_stat_archiver';
    END CASE;

    IF server_query IS NOT NULL THEN
      INSERT INTO last_stat_archiver (
        server_id,
        sample_id,
        archived_count,
        last_archived_wal,
        last_archived_time,
        failed_count,
        last_failed_wal,
        last_failed_time,
        stats_reset)
      SELECT
        sserver_id,
        s_id,
        archived_count as archived_count,
        last_archived_wal as last_archived_wal,
        last_archived_time as last_archived_time,
        failed_count as failed_count,
        last_failed_wal as last_failed_wal,
        last_failed_time as last_failed_time,
        stats_reset as stats_reset
      FROM dblink('server_connection',server_query) AS rs (
        archived_count              bigint,
        last_archived_wal           text,
        last_archived_time          timestamp with time zone,
        failed_count                bigint,
        last_failed_wal             text,
        last_failed_time            timestamp with time zone,
        stats_reset                 timestamp with time zone
      );
    END IF;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,query pg_stat_archiver,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,disconnect}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    PERFORM dblink('server_connection', 'COMMIT');
    PERFORM dblink_disconnect('server_connection');

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,disconnect,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,collect object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Collecting stat info for objects of all databases
    server_properties := collect_obj_stats(server_properties, sserver_id, s_id, skip_sizes);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,collect object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,maintain repository}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary table in case of object renaming:
    -- Databases
    UPDATE sample_stat_database AS db
    SET datname = lst.datname
    FROM last_stat_database AS lst
    WHERE
      (db.server_id, lst.server_id, lst.sample_id, db.datid) =
      (sserver_id, sserver_id, s_id, lst.datid)
      AND db.datname != lst.datname;
    -- Tables
    UPDATE tables_list AS tl
    SET (schemaname, relname) = (lst.schemaname, lst.relname)
    FROM last_stat_tables AS lst
    WHERE (tl.server_id, lst.server_id, lst.sample_id, tl.datid, tl.relid, tl.relkind) =
        (sserver_id, sserver_id, s_id, lst.datid, lst.relid, lst.relkind)
      AND (tl.schemaname, tl.relname) != (lst.schemaname, lst.relname);
    -- Functions
    UPDATE funcs_list AS fl
    SET (schemaname, funcname, funcargs) =
      (lst.schemaname, lst.funcname, lst.funcargs)
    FROM last_stat_user_functions AS lst
    WHERE (fl.server_id, lst.server_id, lst.sample_id, fl.datid, fl.funcid) =
        (sserver_id, sserver_id, s_id, lst.datid, lst.funcid)
      AND (fl.schemaname, fl.funcname, fl.funcargs) !=
        (lst.schemaname, lst.funcname, lst.funcargs);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,maintain repository,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    INSERT INTO tablespaces_list AS itl (
        server_id,
        last_sample_id,
        tablespaceid,
        tablespacename,
        tablespacepath
      )
    SELECT
      cur.server_id,
      NULL,
      cur.tablespaceid,
      cur.tablespacename,
      cur.tablespacepath
    FROM
      last_stat_tablespaces cur
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    ON CONFLICT ON CONSTRAINT pk_tablespace_list DO
    UPDATE SET
        (last_sample_id, tablespacename, tablespacepath) =
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath)
      WHERE
        (itl.last_sample_id, itl.tablespacename, itl.tablespacepath) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.tablespacename, EXCLUDED.tablespacepath);

    -- Calculate diffs for tablespaces
    INSERT INTO sample_stat_tablespaces(
      server_id,
      sample_id,
      tablespaceid,
      size,
      size_delta
    )
    SELECT
      cur.server_id as server_id,
      cur.sample_id as sample_id,
      cur.tablespaceid as tablespaceid,
      cur.size as size,
      cur.size - COALESCE(lst.size, 0) AS size_delta
    FROM last_stat_tablespaces cur
      LEFT OUTER JOIN last_stat_tablespaces lst ON
        (lst.server_id, lst.sample_id, cur.tablespaceid) =
        (sserver_id, s_id - 1, lst.tablespaceid)
    WHERE (cur.server_id, cur.sample_id) = ( sserver_id, s_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate tablespace stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- collect databases objects stats
    server_properties := sample_dbobj_delta(server_properties,sserver_id,s_id,topn,skip_sizes);
    ASSERT server_properties IS NOT NULL, 'lost properties';

    DELETE FROM last_stat_tablespaces WHERE server_id = sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_database WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate object stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc stat cluster diff
    INSERT INTO sample_stat_cluster(
      server_id,
      sample_id,
      checkpoints_timed,
      checkpoints_req,
      checkpoint_write_time,
      checkpoint_sync_time,
      buffers_checkpoint,
      buffers_clean,
      maxwritten_clean,
      buffers_backend,
      buffers_backend_fsync,
      buffers_alloc,
      stats_reset,
      wal_size,
      wal_lsn,
      in_recovery
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.checkpoints_timed - COALESCE(lst.checkpoints_timed,0),
        cur.checkpoints_req - COALESCE(lst.checkpoints_req,0),
        cur.checkpoint_write_time - COALESCE(lst.checkpoint_write_time,0),
        cur.checkpoint_sync_time - COALESCE(lst.checkpoint_sync_time,0),
        cur.buffers_checkpoint - COALESCE(lst.buffers_checkpoint,0),
        cur.buffers_clean - COALESCE(lst.buffers_clean,0),
        cur.maxwritten_clean - COALESCE(lst.maxwritten_clean,0),
        cur.buffers_backend - COALESCE(lst.buffers_backend,0),
        cur.buffers_backend_fsync - COALESCE(lst.buffers_backend_fsync,0),
        cur.buffers_alloc - COALESCE(lst.buffers_alloc,0),
        cur.stats_reset,
        cur.wal_size - COALESCE(lst.wal_size,0),
        cur.wal_lsn,
        cur.in_recovery
        /* We will overwrite this value in case of stats reset
         * (see below)
         */
    FROM last_stat_cluster cur
      LEFT OUTER JOIN last_stat_cluster lst ON
        (lst.server_id, lst.sample_id) =
        (sserver_id, s_id - 1)
        AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    /* wal_size is calculated since 0 to current value when stats reset happened
     * so, we need to update it
     */
    UPDATE sample_stat_cluster ssc
    SET wal_size = cur.wal_size - lst.wal_size
    FROM last_stat_cluster cur
      JOIN last_stat_cluster lst ON
        (lst.server_id, lst.sample_id) =
        (sserver_id, s_id - 1)
    WHERE
      (ssc.server_id, ssc.sample_id) = (sserver_id, s_id) AND
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      cur.stats_reset IS DISTINCT FROM lst.stats_reset;

    DELETE FROM last_stat_cluster WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate cluster stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate IO stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc I/O stat diff
    INSERT INTO sample_stat_io(
        server_id,
        sample_id,
        backend_type,
        object,
        context,
        reads,
        read_time,
        writes,
        write_time,
        writebacks,
        writeback_time,
        extends,
        extend_time,
        op_bytes,
        hits,
        evictions,
        reuses,
        fsyncs,
        fsync_time,
        stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.backend_type,
        cur.object,
        cur.context,
        cur.reads - COALESCE(lst.reads, 0),
        cur.read_time - COALESCE(lst.read_time, 0),
        cur.writes - COALESCE(lst.writes, 0),
        cur.write_time - COALESCE(lst.write_time, 0),
        cur.writebacks - COALESCE(lst.writebacks, 0),
        cur.writeback_time - COALESCE(lst.writeback_time, 0),
        cur.extends - COALESCE(lst.extends, 0),
        cur.extend_time - COALESCE(lst.extend_time, 0),
        cur.op_bytes,
        cur.hits - COALESCE(lst.hits, 0),
        cur.evictions - COALESCE(lst.evictions, 0),
        cur.reuses - COALESCE(lst.reuses, 0),
        cur.fsyncs - COALESCE(lst.fsyncs, 0),
        cur.fsync_time - COALESCE(lst.fsync_time, 0),
        cur.stats_reset
    FROM last_stat_io cur
    LEFT OUTER JOIN last_stat_io lst ON
      (lst.server_id, lst.sample_id, lst.backend_type, lst.object, lst.context) =
      (sserver_id, s_id - 1, cur.backend_type, cur.object, cur.context)
      AND (cur.op_bytes,cur.stats_reset) IS NOT DISTINCT FROM (lst.op_bytes,lst.stats_reset)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      GREATEST(
        cur.reads - COALESCE(lst.reads, 0),
        cur.writes - COALESCE(lst.writes, 0),
        cur.writebacks - COALESCE(lst.writebacks, 0),
        cur.extends - COALESCE(lst.extends, 0),
        cur.hits - COALESCE(lst.hits, 0),
        cur.evictions - COALESCE(lst.evictions, 0),
        cur.reuses - COALESCE(lst.reuses, 0),
        cur.fsyncs - COALESCE(lst.fsyncs, 0)
      ) > 0;

    DELETE FROM last_stat_io WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate IO stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate SLRU stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc SLRU stat diff
    INSERT INTO sample_stat_slru(
        server_id,
        sample_id,
        name,
        blks_zeroed,
        blks_hit,
        blks_read,
        blks_written,
        blks_exists,
        flushes,
        truncates,
        stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.name,
        cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
        cur.blks_hit - COALESCE(lst.blks_hit, 0),
        cur.blks_read - COALESCE(lst.blks_read, 0),
        cur.blks_written - COALESCE(lst.blks_written, 0),
        cur.blks_exists - COALESCE(lst.blks_exists, 0),
        cur.flushes - COALESCE(lst.flushes, 0),
        cur.truncates - COALESCE(lst.truncates, 0),
        cur.stats_reset
    FROM last_stat_slru cur
    LEFT OUTER JOIN last_stat_slru lst ON
      (lst.server_id, lst.sample_id, lst.name) =
      (sserver_id, s_id - 1, cur.name)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id) AND
      GREATEST(
        cur.blks_zeroed - COALESCE(lst.blks_zeroed, 0),
        cur.blks_hit - COALESCE(lst.blks_hit, 0),
        cur.blks_read - COALESCE(lst.blks_read, 0),
        cur.blks_written - COALESCE(lst.blks_written, 0),
        cur.blks_exists - COALESCE(lst.blks_exists, 0),
        cur.flushes - COALESCE(lst.flushes, 0),
        cur.truncates - COALESCE(lst.truncates, 0)
      ) > 0;

    DELETE FROM last_stat_slru WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate SLRU stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc WAL stat diff
    INSERT INTO sample_stat_wal(
      server_id,
      sample_id,
      wal_records,
      wal_fpi,
      wal_bytes,
      wal_buffers_full,
      wal_write,
      wal_sync,
      wal_write_time,
      wal_sync_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.wal_records - COALESCE(lst.wal_records,0),
        cur.wal_fpi - COALESCE(lst.wal_fpi,0),
        cur.wal_bytes - COALESCE(lst.wal_bytes,0),
        cur.wal_buffers_full - COALESCE(lst.wal_buffers_full,0),
        cur.wal_write - COALESCE(lst.wal_write,0),
        cur.wal_sync - COALESCE(lst.wal_sync,0),
        cur.wal_write_time - COALESCE(lst.wal_write_time,0),
        cur.wal_sync_time - COALESCE(lst.wal_sync_time,0),
        cur.stats_reset
    FROM last_stat_wal cur
    LEFT OUTER JOIN last_stat_wal lst ON
      (lst.server_id, lst.sample_id) = (sserver_id, s_id - 1)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id);

    DELETE FROM last_stat_wal WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate WAL stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Calc stat archiver diff
    INSERT INTO sample_stat_archiver(
      server_id,
      sample_id,
      archived_count,
      last_archived_wal,
      last_archived_time,
      failed_count,
      last_failed_wal,
      last_failed_time,
      stats_reset
    )
    SELECT
        cur.server_id,
        cur.sample_id,
        cur.archived_count - COALESCE(lst.archived_count,0),
        cur.last_archived_wal,
        cur.last_archived_time,
        cur.failed_count - COALESCE(lst.failed_count,0),
        cur.last_failed_wal,
        cur.last_failed_time,
        cur.stats_reset
    FROM last_stat_archiver cur
    LEFT OUTER JOIN last_stat_archiver lst ON
      (lst.server_id, lst.sample_id) =
      (cur.server_id, cur.sample_id - 1)
      AND cur.stats_reset IS NOT DISTINCT FROM lst.stats_reset
    WHERE cur.sample_id = s_id AND cur.server_id = sserver_id;

    DELETE FROM last_stat_archiver WHERE server_id = sserver_id AND sample_id != s_id;

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,calculate archiver stats,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Updating dictionary tables setting last_sample_id
    UPDATE tablespaces_list utl SET last_sample_id = s_id - 1
    FROM tablespaces_list tl LEFT JOIN sample_stat_tablespaces cur
      ON (cur.server_id, cur.sample_id, cur.tablespaceid) =
        (sserver_id, s_id, tl.tablespaceid)
    WHERE
      tl.last_sample_id IS NULL AND
      (utl.server_id, utl.tablespaceid) = (sserver_id, tl.tablespaceid) AND
      tl.server_id = sserver_id AND cur.server_id IS NULL;

    UPDATE funcs_list ufl SET last_sample_id = s_id - 1
    FROM funcs_list fl LEFT JOIN sample_stat_user_functions cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.funcid) =
        (sserver_id, s_id, fl.datid, fl.funcid)
    WHERE
      fl.last_sample_id IS NULL AND
      fl.server_id = sserver_id AND cur.server_id IS NULL AND
      (ufl.server_id, ufl.datid, ufl.funcid) =
      (sserver_id, fl.datid, fl.funcid);

    UPDATE indexes_list uil SET last_sample_id = s_id - 1
    FROM indexes_list il LEFT JOIN sample_stat_indexes cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.indexrelid) =
        (sserver_id, s_id, il.datid, il.indexrelid)
    WHERE
      il.last_sample_id IS NULL AND
      il.server_id = sserver_id AND cur.server_id IS NULL AND
      (uil.server_id, uil.datid, uil.indexrelid) =
      (sserver_id, il.datid, il.indexrelid);

    UPDATE tables_list utl SET last_sample_id = s_id - 1
    FROM tables_list tl LEFT JOIN sample_stat_tables cur
      ON (cur.server_id, cur.sample_id, cur.datid, cur.relid) =
        (sserver_id, s_id, tl.datid, tl.relid)
    WHERE
      tl.last_sample_id IS NULL AND
      tl.server_id = sserver_id AND cur.server_id IS NULL AND
      (utl.server_id, utl.datid, utl.relid) =
      (sserver_id, tl.datid, tl.relid);

    UPDATE stmt_list slu SET last_sample_id = s_id - 1
    FROM sample_statements ss RIGHT JOIN stmt_list sl
      ON (ss.server_id, ss.sample_id, ss.queryid_md5) =
        (sserver_id, s_id, sl.queryid_md5)
    WHERE
      sl.server_id = sserver_id AND
      sl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (slu.server_id, slu.queryid_md5) = (sserver_id, sl.queryid_md5);

    UPDATE roles_list rlu SET last_sample_id = s_id - 1
    FROM
        sample_statements ss
      RIGHT JOIN roles_list rl
      ON (ss.server_id, ss.sample_id, ss.userid) =
        (sserver_id, s_id, rl.userid)
    WHERE
      rl.server_id = sserver_id AND
      rl.last_sample_id IS NULL AND
      ss.server_id IS NULL AND
      (rlu.server_id, rlu.userid) = (sserver_id, rl.userid);

    -- Deleting obsolete baselines
    DELETE FROM baselines
    WHERE keep_until < now()
      AND server_id = sserver_id;

    -- Deleting obsolete samples
    PERFORM num_nulls(min(s.sample_id),max(s.sample_id)) > 0 OR
      delete_samples(sserver_id, min(s.sample_id), max(s.sample_id)) > 0
    FROM samples s JOIN
      servers n USING (server_id)
    WHERE s.server_id = sserver_id
        AND s.sample_time < now() - (COALESCE(n.max_sample_age,ret) || ' days')::interval
        AND (s.server_id,s.sample_id) NOT IN (SELECT server_id,sample_id FROM bl_samples WHERE server_id = sserver_id);

    IF (server_properties #>> '{collect_timings}')::boolean THEN
      server_properties := jsonb_set(server_properties,'{timings,delete obsolete samples,end}',to_jsonb(clock_timestamp()));
      server_properties := jsonb_set(server_properties,'{timings,total,end}',to_jsonb(clock_timestamp()));
      -- Save timing statistics of sample
      INSERT INTO sample_timings
      SELECT sserver_id, s_id, key,(value::jsonb #>> '{end}')::timestamp with time zone - (value::jsonb #>> '{start}')::timestamp with time zone as time_spent
      FROM jsonb_each_text(server_properties #> '{timings}');
    END IF;
    ASSERT server_properties IS NOT NULL, 'lost properties';

    RETURN 0;
END;
$$ LANGUAGE plpgsql;
COMMENT ON FUNCTION take_sample(IN sserver_id integer, IN skip_sizes boolean) IS
  'Statistics sample creation function (by server_id)';
CREATE FUNCTION collect_obj_stats(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN skip_sizes boolean
) RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    --Cursor over databases 
    c_dblist CURSOR FOR
    SELECT
      datid,
      datname,
      dattablespace AS tablespaceid
    FROM last_stat_database ldb
      JOIN servers n ON
        (n.server_id = sserver_id AND array_position(n.db_exclude,ldb.datname) IS NULL)
    WHERE
      NOT ldb.datistemplate AND ldb.datallowconn AND
      (ldb.server_id, ldb.sample_id) = (sserver_id, s_id);

    qres        record;
    db_connstr  text;
    t_query     text;
    result      jsonb := collect_obj_stats.properties;
BEGIN
    -- Adding dblink extension schema to search_path if it does not already there
    IF (SELECT count(*) = 0 FROM pg_catalog.pg_extension WHERE extname = 'dblink') THEN
      RAISE 'dblink extension must be installed';
    END IF;
    SELECT extnamespace::regnamespace AS dblink_schema INTO STRICT qres FROM pg_catalog.pg_extension WHERE extname = 'dblink';
    IF NOT string_to_array(current_setting('search_path'),', ') @> ARRAY[qres.dblink_schema::text] THEN
      EXECUTE 'SET LOCAL search_path TO ' || current_setting('search_path')||','|| qres.dblink_schema;
    END IF;

    -- Disconnecting existing connection
    IF dblink_get_connections() @> ARRAY['server_db_connection'] THEN
        PERFORM dblink_disconnect('server_db_connection');
    END IF;

    -- Load new data from statistic views of all cluster databases
    FOR qres IN c_dblist LOOP
      db_connstr := concat_ws(' ',properties #>> '{properties,server_connstr}',
        format($o$dbname='%s'$o$,replace(qres.datname,$o$'$o$,$o$\'$o$))
      );
      PERFORM dblink_connect('server_db_connection',db_connstr);
      -- Transaction
      PERFORM dblink('server_db_connection','BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY');
      -- Setting application name
      PERFORM dblink('server_db_connection','SET application_name=''pg_profile''');
      -- Setting lock_timout prevents hanging of take_sample() call due to DDL in long transaction
      PERFORM dblink('server_db_connection','SET lock_timeout=3000');
      -- Reset search_path for security reasons
      PERFORM dblink('server_db_connection','SET search_path=''''');

      IF (properties #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate Table stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 130000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'NULL as n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'NULL AS last_seq_scan,'
            'NULL AS last_idx_scan,'
            'NULL AS n_tup_newpage_upd '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.schemaname,'
            'st.relname,'
            'st.seq_scan,'
            'st.seq_tup_read,'
            'st.idx_scan,'
            'st.idx_tup_fetch,'
            'st.n_tup_ins,'
            'st.n_tup_upd,'
            'st.n_tup_del,'
            'st.n_tup_hot_upd,'
            'st.n_live_tup,'
            'st.n_dead_tup,'
            'st.n_mod_since_analyze,'
            'st.n_ins_since_vacuum,'
            'st.last_vacuum,'
            'st.last_autovacuum,'
            'st.last_analyze,'
            'st.last_autoanalyze,'
            'st.vacuum_count,'
            'st.autovacuum_count,'
            'st.analyze_count,'
            'st.autoanalyze_count,'
            'stio.heap_blks_read,'
            'stio.heap_blks_hit,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            'stio.toast_blks_read,'
            'stio.toast_blks_hit,'
            'stio.tidx_blks_read,'
            'stio.tidx_blks_hit,'
            -- Size of all forks without TOAST
            '{relation_size} relsize,'
            '0 relsize_diff,'
            'class.reltablespace AS tablespaceid,'
            'class.reltoastrelid,'
            'class.relkind,'
            'class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff,'
            'st.last_seq_scan,'
            'st.last_idx_scan,'
            'st.n_tup_newpage_upd '
          'FROM pg_catalog.pg_stat_all_tables st '
          'JOIN pg_catalog.pg_statio_all_tables stio USING (relid, schemaname, relname) '
          'JOIN pg_catalog.pg_class class ON (st.relid = class.oid) '
          -- is relation or its dependant is locked
          '{lock_join}'
          ;

        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}','CASE locked.objid WHEN st.relid THEN NULL ELSE '
          'pg_catalog.pg_table_size(st.relid) - '
          'coalesce(pg_catalog.pg_relation_size(class.reltoastrelid),0) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL '
            '(WITH RECURSIVE deps (objid) AS ('
              'SELECT relation FROM pg_catalog.pg_locks WHERE granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'' '
              'UNION '
              'SELECT refobjid FROM pg_catalog.pg_depend d JOIN deps dd ON (d.objid = dd.objid)'
            ') '
            'SELECT objid FROM deps) AS locked ON (st.relid = locked.objid)');
      END IF;

      INSERT INTO last_stat_tables(
        server_id,
        sample_id,
        datid,
        relid,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind,
        in_sample,
        relpages_bytes,
        relpages_bytes_diff,
        last_seq_scan,
        last_idx_scan,
        n_tup_newpage_upd
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        dbl.relid,
        dbl.schemaname,
        dbl.relname,
        dbl.seq_scan AS seq_scan,
        dbl.seq_tup_read AS seq_tup_read,
        dbl.idx_scan AS idx_scan,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.n_tup_ins AS n_tup_ins,
        dbl.n_tup_upd AS n_tup_upd,
        dbl.n_tup_del AS n_tup_del,
        dbl.n_tup_hot_upd AS n_tup_hot_upd,
        dbl.n_live_tup AS n_live_tup,
        dbl.n_dead_tup AS n_dead_tup,
        dbl.n_mod_since_analyze AS n_mod_since_analyze,
        dbl.n_ins_since_vacuum AS n_ins_since_vacuum,
        dbl.last_vacuum,
        dbl.last_autovacuum,
        dbl.last_analyze,
        dbl.last_autoanalyze,
        dbl.vacuum_count AS vacuum_count,
        dbl.autovacuum_count AS autovacuum_count,
        dbl.analyze_count AS analyze_count,
        dbl.autoanalyze_count AS autoanalyze_count,
        dbl.heap_blks_read AS heap_blks_read,
        dbl.heap_blks_hit AS heap_blks_hit,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.toast_blks_read AS toast_blks_read,
        dbl.toast_blks_hit AS toast_blks_hit,
        dbl.tidx_blks_read AS tidx_blks_read,
        dbl.tidx_blks_hit AS tidx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN dbl.tablespaceid=0 THEN qres.tablespaceid ELSE dbl.tablespaceid END AS tablespaceid,
        dbl.reltoastrelid,
        dbl.relkind,
        false,
        dbl.relpages_bytes,
        dbl.relpages_bytes_diff,
        dbl.last_seq_scan,
        dbl.last_idx_scan,
        dbl.n_tup_newpage_upd
      FROM dblink('server_db_connection', t_query)
      AS dbl (
          relid                 oid,
          schemaname            name,
          relname               name,
          seq_scan              bigint,
          seq_tup_read          bigint,
          idx_scan              bigint,
          idx_tup_fetch         bigint,
          n_tup_ins             bigint,
          n_tup_upd             bigint,
          n_tup_del             bigint,
          n_tup_hot_upd         bigint,
          n_live_tup            bigint,
          n_dead_tup            bigint,
          n_mod_since_analyze   bigint,
          n_ins_since_vacuum    bigint,
          last_vacuum           timestamp with time zone,
          last_autovacuum       timestamp with time zone,
          last_analyze          timestamp with time zone,
          last_autoanalyze      timestamp with time zone,
          vacuum_count          bigint,
          autovacuum_count      bigint,
          analyze_count         bigint,
          autoanalyze_count     bigint,
          heap_blks_read        bigint,
          heap_blks_hit         bigint,
          idx_blks_read         bigint,
          idx_blks_hit          bigint,
          toast_blks_read       bigint,
          toast_blks_hit        bigint,
          tidx_blks_read        bigint,
          tidx_blks_hit         bigint,
          relsize               bigint,
          relsize_diff          bigint,
          tablespaceid          oid,
          reltoastrelid         oid,
          relkind               char,
          relpages_bytes        bigint,
          relpages_bytes_diff   bigint,
          last_seq_scan         timestamp with time zone,
          last_idx_scan         timestamp with time zone,
          n_tup_newpage_upd     bigint
      );

      EXECUTE format('ANALYZE last_stat_tables_srv%1$s',
        sserver_id);

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect tables stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate index stats query
      CASE
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer < 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'NULL AS last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u'')) '
            '{lock_join}'
            ;
        WHEN (
          SELECT count(*) = 1 FROM jsonb_to_recordset(properties #> '{settings}')
            AS x(name text, reset_val text)
          WHERE name = 'server_version_num'
            AND reset_val::integer >= 160000
        )
        THEN
          t_query := 'SELECT '
            'st.relid,'
            'st.indexrelid,'
            'st.schemaname,'
            'st.relname,'
            'st.indexrelname,'
            'st.idx_scan,'
            'st.last_idx_scan,'
            'st.idx_tup_read,'
            'st.idx_tup_fetch,'
            'stio.idx_blks_read,'
            'stio.idx_blks_hit,'
            '{relation_size} relsize,'
            '0,'
            'pg_class.reltablespace as tablespaceid,'
            '(ix.indisunique OR con.conindid IS NOT NULL) AS indisunique,'
            'pg_class.relpages::bigint * current_setting(''block_size'')::bigint AS relpages_bytes,'
            '0 AS relpages_bytes_diff '
          'FROM pg_catalog.pg_stat_all_indexes st '
            'JOIN pg_catalog.pg_statio_all_indexes stio USING (relid, indexrelid, schemaname, relname, indexrelname) '
            'JOIN pg_catalog.pg_index ix ON (ix.indexrelid = st.indexrelid) '
            'JOIN pg_catalog.pg_class ON (pg_class.oid = st.indexrelid) '
            'LEFT OUTER JOIN pg_catalog.pg_constraint con ON (con.conindid = ix.indexrelid AND con.contype in (''p'',''u'')) '
            '{lock_join}'
            ;
        ELSE
          RAISE 'Unsupported server version.';
      END CASE;

      IF skip_sizes THEN
        t_query := replace(t_query,'{relation_size}','NULL');
        t_query := replace(t_query,'{lock_join}','');
      ELSE
        t_query := replace(t_query,'{relation_size}',
          'CASE l.relation WHEN st.indexrelid THEN NULL ELSE pg_relation_size(st.indexrelid) END');
        t_query := replace(t_query,'{lock_join}',
          'LEFT OUTER JOIN LATERAL ('
            'SELECT relation '
            'FROM pg_catalog.pg_locks '
            'WHERE '
            '(relation = st.indexrelid AND granted AND locktype = ''relation'' AND mode=''AccessExclusiveLock'')'
          ') l ON (l.relation = st.indexrelid)');
      END IF;

      INSERT INTO last_stat_indexes(
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        idx_scan,
        last_idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        in_sample,
        relpages_bytes,
        relpages_bytes_diff
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        relid,
        indexrelid,
        schemaname,
        relname,
        indexrelname,
        dbl.idx_scan AS idx_scan,
        dbl.last_idx_scan AS last_idx_scan,
        dbl.idx_tup_read AS idx_tup_read,
        dbl.idx_tup_fetch AS idx_tup_fetch,
        dbl.idx_blks_read AS idx_blks_read,
        dbl.idx_blks_hit AS idx_blks_hit,
        dbl.relsize AS relsize,
        dbl.relsize_diff AS relsize_diff,
        CASE WHEN tablespaceid=0 THEN qres.tablespaceid ELSE tablespaceid END tablespaceid,
        indisunique,
        false,
        dbl.relpages_bytes,
        dbl.relpages_bytes_diff
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         relid          oid,
         indexrelid     oid,
         schemaname     name,
         relname        name,
         indexrelname   name,
         idx_scan       bigint,
         last_idx_scan  timestamp with time zone,
         idx_tup_read   bigint,
         idx_tup_fetch  bigint,
         idx_blks_read  bigint,
         idx_blks_hit   bigint,
         relsize        bigint,
         relsize_diff   bigint,
         tablespaceid   oid,
         indisunique    bool,
         relpages_bytes bigint,
         relpages_bytes_diff  bigint
      );

      EXECUTE format('ANALYZE last_stat_indexes_srv%1$s',
        sserver_id);

      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect indexes stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname)],jsonb_build_object('start',clock_timestamp()));
      END IF;

      -- Generate Function stats query
      t_query := 'SELECT f.funcid,'
        'f.schemaname,'
        'f.funcname,'
        'pg_get_function_arguments(f.funcid) AS funcargs,'
        'f.calls,'
        'f.total_time,'
        'f.self_time,'
        'p.prorettype::regtype::text =''trigger'' AS trg_fn '
      'FROM pg_catalog.pg_stat_user_functions f '
        'JOIN pg_catalog.pg_proc p ON (f.funcid = p.oid) '
      'WHERE pg_get_function_arguments(f.funcid) IS NOT NULL';

      INSERT INTO last_stat_user_functions(
        server_id,
        sample_id,
        datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        calls,
        total_time,
        self_time,
        trg_fn
      )
      SELECT
        sserver_id,
        s_id,
        qres.datid,
        funcid,
        schemaname,
        funcname,
        funcargs,
        dbl.calls AS calls,
        dbl.total_time AS total_time,
        dbl.self_time AS self_time,
        dbl.trg_fn
      FROM dblink('server_db_connection', t_query)
      AS dbl (
         funcid       oid,
         schemaname   name,
         funcname     name,
         funcargs     text,
         calls        bigint,
         total_time   double precision,
         self_time    double precision,
         trg_fn       boolean
      );

      EXECUTE format('ANALYZE last_stat_user_functions_srv%1$s',
        sserver_id);

      PERFORM dblink('server_db_connection', 'COMMIT');
      PERFORM dblink_disconnect('server_db_connection');
      IF (result #>> '{collect_timings}')::boolean THEN
        result := jsonb_set(result,ARRAY['timings',format('db:%s collect functions stats',qres.datname),'end'],to_jsonb(clock_timestamp()));
      END IF;
    END LOOP;
   RETURN result;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION sample_dbobj_delta(IN properties jsonb, IN sserver_id integer, IN s_id integer,
  IN topn integer, IN skip_sizes boolean) RETURNS jsonb AS $$
DECLARE
    qres    record;
    result  jsonb := sample_dbobj_delta.properties;
BEGIN

    /* This function will calculate statistics increments for database objects
    * and store top objects values in sample.
    * Due to relations between objects we need to mark top objects (and their
    * dependencies) first, and calculate increments later
    */
    IF (properties #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(properties,'{timings,calculate tables stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- Marking functions
    UPDATE last_stat_user_functions ulf
    SET in_sample = true
    FROM
        (SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.funcid,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.total_time - COALESCE(lst.total_time,0) DESC) time_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.self_time - COALESCE(lst.self_time,0) DESC) stime_rank,
            row_number() OVER (PARTITION BY cur.trg_fn ORDER BY cur.calls - COALESCE(lst.calls,0) DESC) calls_rank
        FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
          LEFT OUTER JOIN last_stat_database dblst ON
            (dblst.server_id, dblst.datid, dblst.sample_id) =
            (sserver_id, dbcur.datid, s_id - 1)
            AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
          LEFT OUTER JOIN last_stat_user_functions lst ON
            (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
            (sserver_id, s_id - 1, dblst.datid, cur.funcid)
        WHERE
            (cur.server_id, cur.sample_id) =
            (sserver_id, s_id)
            AND cur.calls - COALESCE(lst.calls,0) > 0) diff
    WHERE
      least(
        time_rank,
        calls_rank,
        stime_rank
      ) <= topn
      AND (ulf.server_id, ulf.sample_id, ulf.datid, ulf.funcid) =
        (diff.server_id, diff.sample_id, diff.datid, diff.funcid);

    -- Marking indexes
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          -- Index ranks
          row_number() OVER (ORDER BY cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) DESC) read_rank,
          row_number() OVER (ORDER BY cur.idx_blks_read+cur.idx_blks_hit-
            COALESCE(lst.idx_blks_read+lst.idx_blks_hit,0) DESC) gets_rank,
          row_number() OVER (PARTITION BY cur.idx_scan - COALESCE(lst.idx_scan,0) = 0
            ORDER BY tblcur.n_tup_ins - COALESCE(tbllst.n_tup_ins,0) +
            tblcur.n_tup_upd - COALESCE(tbllst.n_tup_upd,0) +
            tblcur.n_tup_del - COALESCE(tbllst.n_tup_del,0) DESC) dml_unused_rank,
          row_number() OVER (ORDER BY (tblcur.vacuum_count - COALESCE(tbllst.vacuum_count,0) +
            tblcur.autovacuum_count - COALESCE(tbllst.autovacuum_count,0)) *
              -- Coalesce is used here in case of skipped size collection
              COALESCE(cur.relsize,lst.relsize) DESC) vacuum_bytes_rank
      FROM last_stat_indexes cur JOIN last_stat_tables tblcur USING (server_id, sample_id, datid, relid)
        JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
        LEFT OUTER JOIN last_stat_tables tbllst ON
          (tbllst.server_id, tbllst.sample_id, tbllst.datid, tbllst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.relid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      (least(
        read_rank,
        gets_rank,
        vacuum_bytes_rank
      ) <= topn
      OR (dml_unused_rank <= topn AND idx_scan = 0))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_indexes uli
    SET in_sample = true
    FROM
      (SELECT
          cur.server_id,
          cur.sample_id,
          cur.datid,
          cur.indexrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          -- Index ranks
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize,0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) DESC NULLS LAST) pagegrowth_rank
      FROM last_stat_indexes cur
        LEFT OUTER JOIN last_stat_indexes lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid, cur.indexrelid)
      WHERE
        (cur.server_id, cur.sample_id) =
        (sserver_id, s_id)
      ) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (uli.server_id, uli.sample_id, uli.datid, uli.indexrelid, uli.in_sample) =
        (diff.server_id, diff.sample_id, diff.datid, diff.indexrelid, false);

    -- Marking tables
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          -- Seq. scanned blocks rank
          row_number() OVER (ORDER BY
            (cur.seq_scan - COALESCE(lst.seq_scan,0)) * cur.relsize +
            (tcur.seq_scan - COALESCE(tlst.seq_scan,0)) * tcur.relsize DESC) scan_rank,
          row_number() OVER (ORDER BY cur.n_tup_ins + cur.n_tup_upd + cur.n_tup_del -
            COALESCE(lst.n_tup_ins + lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_ins + tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_ins + tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) dml_rank,
          row_number() OVER (ORDER BY cur.n_tup_upd+cur.n_tup_del -
            COALESCE(lst.n_tup_upd + lst.n_tup_del, 0) +
            COALESCE(tcur.n_tup_upd + tcur.n_tup_del, 0) -
            COALESCE(tlst.n_tup_upd + tlst.n_tup_del, 0) DESC) vacuum_dml_rank,
          row_number() OVER (ORDER BY
            cur.n_dead_tup / NULLIF(cur.n_live_tup+cur.n_dead_tup, 0)
            DESC NULLS LAST) dead_pct_rank,
          row_number() OVER (ORDER BY
            cur.n_mod_since_analyze / NULLIF(cur.n_live_tup, 0)
            DESC NULLS LAST) mod_pct_rank,
          -- Read rank
          row_number() OVER (ORDER BY
            cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) +
            cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) +
            cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) +
            cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) DESC) read_rank,
          -- Page processing rank
          row_number() OVER (ORDER BY cur.heap_blks_read+cur.heap_blks_hit+cur.idx_blks_read+cur.idx_blks_hit+
            cur.toast_blks_read+cur.toast_blks_hit+cur.tidx_blks_read+cur.tidx_blks_hit-
            COALESCE(lst.heap_blks_read+lst.heap_blks_hit+lst.idx_blks_read+lst.idx_blks_hit+
            lst.toast_blks_read+lst.toast_blks_hit+lst.tidx_blks_read+lst.tidx_blks_hit, 0) DESC) gets_rank,
          -- Vacuum rank
          row_number() OVER (ORDER BY cur.vacuum_count - COALESCE(lst.vacuum_count, 0) +
            cur.autovacuum_count - COALESCE(lst.autovacuum_count, 0) DESC) vacuum_rank,
          row_number() OVER (ORDER BY cur.analyze_count - COALESCE(lst.analyze_count,0) +
            cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) DESC) analyze_rank,

          -- Newpage updates rank (since PG16)
          CASE WHEN cur.n_tup_newpage_upd IS NOT NULL THEN
            row_number() OVER (ORDER BY cur.n_tup_newpage_upd -
              COALESCE(lst.n_tup_newpage_upd, 0) DESC)
          ELSE
            NULL
          END newpage_upd_rank
      FROM
        -- main relations diff
        last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
        LEFT OUTER JOIN last_stat_database dblst ON
          (dblst.server_id, dblst.datid, dblst.sample_id) =
          (sserver_id, dbcur.datid, s_id - 1)
          AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, dblst.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, dbcur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, dblst.datid, lst.reltoastrelid)
      WHERE
        (cur.server_id, cur.sample_id, cur.in_sample) =
        (sserver_id, s_id, false)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      least(
        scan_rank,
        dml_rank,
        dead_pct_rank,
        mod_pct_rank,
        vacuum_dml_rank,
        read_rank,
        gets_rank,
        vacuum_rank,
        analyze_rank,
        newpage_upd_rank
      ) <= topn
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    -- Growth rank is to be calculated independently of database stats_reset value
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM (
      SELECT
          cur.server_id AS server_id,
          cur.sample_id AS sample_id,
          cur.datid AS datid,
          cur.relid AS relid,
          tcur.relid AS toastrelid,
          cur.relsize IS NOT NULL AS relsize_avail,
          cur.relpages_bytes IS NOT NULL AS relpages_avail,
          row_number() OVER (ORDER BY cur.relsize - COALESCE(lst.relsize, 0) +
            COALESCE(tcur.relsize,0) - COALESCE(tlst.relsize, 0) DESC NULLS LAST) growth_rank,
          row_number() OVER (ORDER BY cur.relpages_bytes - COALESCE(lst.relpages_bytes, 0) +
            COALESCE(tcur.relpages_bytes,0) - COALESCE(tlst.relpages_bytes, 0) DESC NULLS LAST) pagegrowth_rank
      FROM
        -- main relations diff
        last_stat_tables cur
        LEFT OUTER JOIN last_stat_tables lst ON
          (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
          (sserver_id, s_id - 1, cur.datid, cur.relid)
        -- toast relations diff
        LEFT OUTER JOIN last_stat_tables tcur ON
          (tcur.server_id, tcur.sample_id, tcur.datid, tcur.relid) =
          (sserver_id, s_id, cur.datid, cur.reltoastrelid)
        LEFT OUTER JOIN last_stat_tables tlst ON
          (tlst.server_id, tlst.sample_id, tlst.datid, tlst.relid) =
          (sserver_id, s_id - 1, lst.datid, lst.reltoastrelid)
      WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
        AND cur.relkind IN ('r','m')) diff
    WHERE
      ((relsize_avail AND growth_rank <= topn) OR
      ((NOT relsize_avail) AND relpages_avail AND pagegrowth_rank <= topn))
      AND (ulst.server_id, ulst.sample_id, ulst.datid, in_sample) =
        (sserver_id, s_id, diff.datid, false)
      AND (ulst.relid = diff.relid OR ulst.relid = diff.toastrelid);

    /* Also mark tables having marked indexes on them including main
    * table in case of a TOAST index and TOAST table if index is on
    * main table
    */
    UPDATE last_stat_tables ulst
    SET in_sample = true
    FROM
      last_stat_indexes ix
      JOIN last_stat_tables tbl USING (server_id, sample_id, datid, relid)
      LEFT JOIN last_stat_tables mtbl ON
        (mtbl.server_id, mtbl.sample_id, mtbl.datid, mtbl.reltoastrelid) =
        (sserver_id, s_id, tbl.datid, tbl.relid)
    WHERE
      (ix.server_id, ix.sample_id, ix.in_sample) =
      (sserver_id, s_id, true)
      AND (ulst.server_id, ulst.sample_id, ulst.datid, ulst.in_sample) =
        (sserver_id, s_id, tbl.datid, false)
      AND ulst.relid IN (tbl.relid, tbl.reltoastrelid, mtbl.relid);

    -- Insert marked objects statistics increments
    -- New table names
    INSERT INTO tables_list AS itl (
      server_id,
      last_sample_id,
      datid,
      relid,
      relkind,
      reltoastrelid,
      schemaname,
      relname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.relid,
      cur.relkind,
      NULLIF(cur.reltoastrelid, 0),
      cur.schemaname,
      cur.relname
    FROM
      last_stat_tables cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_tables_list DO
      UPDATE SET
        (last_sample_id, reltoastrelid, schemaname, relname) =
        (EXCLUDED.last_sample_id, EXCLUDED.reltoastrelid, EXCLUDED.schemaname, EXCLUDED.relname)
      WHERE
        (itl.last_sample_id, itl.reltoastrelid, itl.schemaname, itl.relname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.reltoastrelid, EXCLUDED.schemaname, EXCLUDED.relname);

    -- Tables
    INSERT INTO sample_stat_tables (
      server_id,
      sample_id,
      datid,
      relid,
      tablespaceid,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
      n_live_tup,
      n_dead_tup,
      n_mod_since_analyze,
      n_ins_since_vacuum,
      last_vacuum,
      last_autovacuum,
      last_analyze,
      last_autoanalyze,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count,
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit,
      tidx_blks_read,
      tidx_blks_hit,
      relsize,
      relsize_diff,
      relpages_bytes,
      relpages_bytes_diff,
      last_seq_scan,
      last_idx_scan,
      n_tup_newpage_upd
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.relid AS relid,
      cur.tablespaceid AS tablespaceid,
      cur.seq_scan - COALESCE(lst.seq_scan,0) AS seq_scan,
      cur.seq_tup_read - COALESCE(lst.seq_tup_read,0) AS seq_tup_read,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.n_tup_ins - COALESCE(lst.n_tup_ins,0) AS n_tup_ins,
      cur.n_tup_upd - COALESCE(lst.n_tup_upd,0) AS n_tup_upd,
      cur.n_tup_del - COALESCE(lst.n_tup_del,0) AS n_tup_del,
      cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0) AS n_tup_hot_upd,
      cur.n_live_tup AS n_live_tup,
      cur.n_dead_tup AS n_dead_tup,
      cur.n_mod_since_analyze AS n_mod_since_analyze,
      cur.n_ins_since_vacuum AS n_ins_since_vacuum,
      cur.last_vacuum AS last_vacuum,
      cur.last_autovacuum AS last_autovacuum,
      cur.last_analyze AS last_analyze,
      cur.last_autoanalyze AS last_autoanalyze,
      cur.vacuum_count - COALESCE(lst.vacuum_count,0) AS vacuum_count,
      cur.autovacuum_count - COALESCE(lst.autovacuum_count,0) AS autovacuum_count,
      cur.analyze_count - COALESCE(lst.analyze_count,0) AS analyze_count,
      cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0) AS autoanalyze_count,
      cur.heap_blks_read - COALESCE(lst.heap_blks_read,0) AS heap_blks_read,
      cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0) AS heap_blks_hit,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.toast_blks_read - COALESCE(lst.toast_blks_read,0) AS toast_blks_read,
      cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0) AS toast_blks_hit,
      cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0) AS tidx_blks_read,
      cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0) AS tidx_blks_hit,
      cur.relsize AS relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_seq_scan AS last_seq_scan,
      cur.last_idx_scan AS last_idx_scan,
      cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0) AS n_tup_newpage_upd
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_tables usst
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (usst.server_id, usst.sample_id, usst.datid, usst.relid) =
        (sserver_id, s_id, cur.datid, cur.relid);

    -- Total table stats
    INSERT INTO sample_stat_tables_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      relkind,
      seq_scan,
      seq_tup_read,
      idx_scan,
      idx_tup_fetch,
      n_tup_ins,
      n_tup_upd,
      n_tup_del,
      n_tup_hot_upd,
      vacuum_count,
      autovacuum_count,
      analyze_count,
      autoanalyze_count,
      heap_blks_read,
      heap_blks_hit,
      idx_blks_read,
      idx_blks_hit,
      toast_blks_read,
      toast_blks_hit,
      tidx_blks_read,
      tidx_blks_hit,
      relsize_diff,
      n_tup_newpage_upd
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      cur.relkind,
      sum(cur.seq_scan - COALESCE(lst.seq_scan,0)),
      sum(cur.seq_tup_read - COALESCE(lst.seq_tup_read,0)),
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.n_tup_ins - COALESCE(lst.n_tup_ins,0)),
      sum(cur.n_tup_upd - COALESCE(lst.n_tup_upd,0)),
      sum(cur.n_tup_del - COALESCE(lst.n_tup_del,0)),
      sum(cur.n_tup_hot_upd - COALESCE(lst.n_tup_hot_upd,0)),
      sum(cur.vacuum_count - COALESCE(lst.vacuum_count,0)),
      sum(cur.autovacuum_count - COALESCE(lst.autovacuum_count,0)),
      sum(cur.analyze_count - COALESCE(lst.analyze_count,0)),
      sum(cur.autoanalyze_count - COALESCE(lst.autoanalyze_count,0)),
      sum(cur.heap_blks_read - COALESCE(lst.heap_blks_read,0)),
      sum(cur.heap_blks_hit - COALESCE(lst.heap_blks_hit,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      sum(cur.toast_blks_read - COALESCE(lst.toast_blks_read,0)),
      sum(cur.toast_blks_hit - COALESCE(lst.toast_blks_hit,0)),
      sum(cur.tidx_blks_read - COALESCE(lst.tidx_blks_read,0)),
      sum(cur.tidx_blks_hit - COALESCE(lst.tidx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END,
      sum(cur.n_tup_newpage_upd - COALESCE(lst.n_tup_newpage_upd,0))
    FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.datid, dblst.sample_id) =
        (sserver_id, dbcur.datid, s_id - 1)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_tables lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid)
    WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid;

    IF NOT skip_sizes THEN
    /* Update incorrectly calculated aggregated tables growth in case of
     * database statistics reset
     */
      UPDATE sample_stat_tables_total usstt
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.relkind,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_tables cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_tables lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
              (sserver_id, s_id - 1, dblst.datid, cur.relid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.relkind, cur.tablespaceid
        ) calc
      WHERE (usstt.server_id, usstt.sample_id, usstt.datid, usstt.relkind, usstt.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.relkind, calc.tablespaceid);

    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_tables cur
    SET relsize = lst.relsize
    FROM last_stat_tables lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.relid) =
      (cur.server_id, s_id - 1, cur.datid, cur.relid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate tables stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate indexes stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New index names
    INSERT INTO indexes_list AS iil (
      server_id,
      last_sample_id,
      datid,
      indexrelid,
      relid,
      schemaname,
      indexrelname
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.indexrelid,
      cur.relid,
      cur.schemaname,
      cur.indexrelname
    FROM
      last_stat_indexes cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_indexes_list DO
      UPDATE SET
        (last_sample_id, relid, schemaname, indexrelname) =
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname)
      WHERE
        (iil.last_sample_id, iil.relid, iil.schemaname, iil.indexrelname) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.relid, EXCLUDED.schemaname, EXCLUDED.indexrelname);

    -- Index stats
    INSERT INTO sample_stat_indexes (
      server_id,
      sample_id,
      datid,
      indexrelid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize,
      relsize_diff,
      indisunique,
      relpages_bytes,
      relpages_bytes_diff,
      last_idx_scan
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.indexrelid AS indexrelid,
      cur.tablespaceid AS tablespaceid,
      cur.idx_scan - COALESCE(lst.idx_scan,0) AS idx_scan,
      cur.idx_tup_read - COALESCE(lst.idx_tup_read,0) AS idx_tup_read,
      cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0) AS idx_tup_fetch,
      cur.idx_blks_read - COALESCE(lst.idx_blks_read,0) AS idx_blks_read,
      cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0) AS idx_blks_hit,
      cur.relsize,
      cur.relsize - COALESCE(lst.relsize,0) AS relsize_diff,
      cur.indisunique,
      cur.relpages_bytes AS relpages_bytes,
      cur.relpages_bytes - COALESCE(lst.relpages_bytes,0) AS relpages_bytes_diff,
      cur.last_idx_scan
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Update incorrectly calculated relation growth in case of database stats reset
    UPDATE sample_stat_indexes ussi
    SET
      relsize_diff = cur.relsize - COALESCE(lst.relsize,0),
      relpages_bytes_diff = cur.relpages_bytes - COALESCE(lst.relpages_bytes,0)
    FROM
      last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true)
      AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
      AND (ussi.server_id, ussi.sample_id, ussi.datid, ussi.indexrelid) =
        (sserver_id, s_id, cur.datid, cur.indexrelid);

    -- Total indexes stats
    INSERT INTO sample_stat_indexes_total(
      server_id,
      sample_id,
      datid,
      tablespaceid,
      idx_scan,
      idx_tup_read,
      idx_tup_fetch,
      idx_blks_read,
      idx_blks_hit,
      relsize_diff
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      cur.tablespaceid,
      sum(cur.idx_scan - COALESCE(lst.idx_scan,0)),
      sum(cur.idx_tup_read - COALESCE(lst.idx_tup_read,0)),
      sum(cur.idx_tup_fetch - COALESCE(lst.idx_tup_fetch,0)),
      sum(cur.idx_blks_read - COALESCE(lst.idx_blks_read,0)),
      sum(cur.idx_blks_hit - COALESCE(lst.idx_blks_hit,0)),
      CASE
        WHEN skip_sizes THEN NULL
        ELSE sum(cur.relsize - COALESCE(lst.relsize,0))
      END
    FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_indexes lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.relid, lst.indexrelid) =
        (sserver_id, s_id - 1, dblst.datid, cur.relid, cur.indexrelid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid;

    /* Update incorrectly calculated aggregated index growth in case of
     * database statistics reset
     */
    IF NOT skip_sizes THEN
      UPDATE sample_stat_indexes_total ussit
      SET relsize_diff = calc.relsize_diff
      FROM (
          SELECT
            cur.server_id,
            cur.sample_id,
            cur.datid,
            cur.tablespaceid,
            sum(cur.relsize - COALESCE(lst.relsize,0)) AS relsize_diff
          FROM last_stat_indexes cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
            JOIN last_stat_database dblst ON
              (dblst.server_id, dblst.sample_id, dblst.datid) =
              (sserver_id, s_id - 1, dbcur.datid)
            LEFT OUTER JOIN last_stat_indexes lst ON
              (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
              (sserver_id, s_id - 1, dblst.datid, cur.indexrelid)
          WHERE (cur.server_id, cur.sample_id) = (sserver_id, s_id)
            AND dblst.stats_reset IS DISTINCT FROM dbcur.stats_reset
          GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.tablespaceid
        ) calc
      WHERE (ussit.server_id, ussit.sample_id, ussit.datid, ussit.tablespaceid) =
        (sserver_id, s_id, calc.datid, calc.tablespaceid);
    END IF;
    /*
    Preserve previous relation sizes in if we couldn't collect
    size this time (for example, due to locked relation)*/
    UPDATE last_stat_indexes cur
    SET relsize = lst.relsize
    FROM last_stat_indexes lst
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
      AND (lst.server_id, lst.sample_id, lst.datid, lst.indexrelid) =
      (sserver_id, s_id - 1, cur.datid, cur.indexrelid)
      AND cur.relsize IS NULL;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate indexes stats,end}',to_jsonb(clock_timestamp()));
      result := jsonb_set(result,'{timings,calculate functions stats}',jsonb_build_object('start',clock_timestamp()));
    END IF;

    -- New function names
    INSERT INTO funcs_list AS ifl (
      server_id,
      last_sample_id,
      datid,
      funcid,
      schemaname,
      funcname,
      funcargs
    )
    SELECT
      cur.server_id,
      NULL,
      cur.datid,
      cur.funcid,
      cur.schemaname,
      cur.funcname,
      cur.funcargs
    FROM
      last_stat_user_functions cur
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) =
      (sserver_id, s_id, true)
    ON CONFLICT ON CONSTRAINT pk_funcs_list DO
      UPDATE SET
        (last_sample_id, funcid, schemaname, funcname, funcargs) =
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs)
      WHERE
        (ifl.last_sample_id, ifl.funcid, ifl.schemaname,
          ifl.funcname, ifl.funcargs) IS DISTINCT FROM
        (EXCLUDED.last_sample_id, EXCLUDED.funcid, EXCLUDED.schemaname,
          EXCLUDED.funcname, EXCLUDED.funcargs);

    -- Function stats
    INSERT INTO sample_stat_user_functions (
      server_id,
      sample_id,
      datid,
      funcid,
      calls,
      total_time,
      self_time,
      trg_fn
    )
    SELECT
      cur.server_id AS server_id,
      cur.sample_id AS sample_id,
      cur.datid AS datid,
      cur.funcid,
      cur.calls - COALESCE(lst.calls,0) AS calls,
      cur.total_time - COALESCE(lst.total_time,0) AS total_time,
      cur.self_time - COALESCE(lst.self_time,0) AS self_time,
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id, cur.in_sample) = (sserver_id, s_id, true);

    -- Total functions stats
    INSERT INTO sample_stat_user_func_total(
      server_id,
      sample_id,
      datid,
      calls,
      total_time,
      trg_fn
    )
    SELECT
      cur.server_id,
      cur.sample_id,
      cur.datid,
      sum(cur.calls - COALESCE(lst.calls,0)),
      sum(cur.total_time - COALESCE(lst.total_time,0)),
      cur.trg_fn
    FROM last_stat_user_functions cur JOIN last_stat_database dbcur USING (server_id, sample_id, datid)
      LEFT OUTER JOIN last_stat_database dblst ON
        (dblst.server_id, dblst.sample_id, dblst.datid) =
        (sserver_id, s_id - 1, dbcur.datid)
        AND dblst.stats_reset IS NOT DISTINCT FROM dbcur.stats_reset
      LEFT OUTER JOIN last_stat_user_functions lst ON
        (lst.server_id, lst.sample_id, lst.datid, lst.funcid) =
        (sserver_id, s_id - 1, dblst.datid, cur.funcid)
    WHERE
      (cur.server_id, cur.sample_id) = (sserver_id, s_id)
    GROUP BY cur.server_id, cur.sample_id, cur.datid, cur.trg_fn;

    IF (result #>> '{collect_timings}')::boolean THEN
      result := jsonb_set(result,'{timings,calculate functions stats,end}',to_jsonb(clock_timestamp()));
    END IF;

    -- Clear data in last_ tables, holding data only for next diff sample
    DELETE FROM last_stat_tables WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_indexes WHERE server_id=sserver_id AND sample_id != s_id;

    DELETE FROM last_stat_user_functions WHERE server_id=sserver_id AND sample_id != s_id;

    RETURN result;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION cluster_stat_io(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id             integer,
    backend_type          text,
    object                text,
    context               text,
    reads                 bigint,
    read_bytes            bigint,
    read_time             double precision,
    writes                bigint,
    write_bytes           bigint,
    write_time            double precision,
    writebacks            bigint,
    writeback_bytes       bigint,
    writeback_time        double precision,
    extends               bigint,
    extend_bytes          bigint,
    extend_time           double precision,
    hits                  bigint,
    evictions             bigint,
    reuses                bigint,
    fsyncs                bigint,
    fsync_time            double precision,
    total_io_time         double precision
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.backend_type AS backend_type,
        st.object AS object,
        st.context AS context,
        SUM(reads)::bigint AS reads,
        SUM(reads * op_bytes)::bigint AS read_bytes,
        SUM(read_time)::double precision AS read_time,
        SUM(writes)::bigint AS writes,
        SUM(writes * op_bytes)::bigint AS write_bytes,
        SUM(write_time)::double precision AS write_time,
        SUM(writebacks)::bigint AS writebacks,
        SUM(writebacks * op_bytes)::bigint AS writeback_bytes,
        SUM(writeback_time)::double precision AS writeback_time,
        SUM(extends)::bigint AS extends,
        SUM(extends * op_bytes)::bigint AS extend_bytes,
        SUM(extend_time)::double precision AS extend_time,
        SUM(hits)::bigint AS hits,
        SUM(evictions)::bigint AS evictions,
        SUM(reuses)::bigint AS reuses,
        SUM(fsyncs)::bigint AS fsyncs,
        SUM(fsync_time)::double precision AS fsync_time,
        SUM(
          COALESCE(read_time, 0.0) +
          COALESCE(write_time, 0.0) +
          COALESCE(writeback_time, 0.0) +
          COALESCE(extend_time, 0.0) +
          COALESCE(fsync_time, 0.0)
        )::double precision AS total_io_time
    FROM sample_stat_io st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.backend_type, st.object, st.context
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_io_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    backend_type          text,
    object                text,
    context               text,

    reads                 bigint,
    read_sz               text,
    read_time             numeric,
    writes                bigint,
    write_sz              text,
    write_time            numeric,
    writebacks            bigint,
    writeback_sz          text,
    writeback_time        numeric,
    extends               bigint,
    extend_sz             text,
    extend_time           numeric,
    hits                  bigint,
    evictions             bigint,
    reuses                bigint,
    fsyncs                bigint,
    fsync_time            double precision
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(backend_type, 'Total'),
    COALESCE(object, '*'),
    COALESCE(context, '*'),

    NULLIF(SUM(reads), 0) AS reads,
    pg_size_pretty(NULLIF(SUM(read_bytes), 0)) AS read_sz,
    ROUND(CAST(NULLIF(SUM(read_time), 0.0) / 1000 AS numeric),2) AS read_time,
    NULLIF(SUM(writes), 0) AS writes,
    pg_size_pretty(NULLIF(SUM(write_bytes), 0)) AS write_sz,
    ROUND(CAST(NULLIF(SUM(write_time), 0.0) / 1000 AS numeric),2) AS write_time,
    NULLIF(SUM(writebacks), 0) AS writebacks,
    pg_size_pretty(NULLIF(SUM(writeback_bytes), 0)) AS writeback_sz,
    ROUND(CAST(NULLIF(SUM(writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time,
    NULLIF(SUM(extends), 0) AS extends,
    pg_size_pretty(NULLIF(SUM(extend_bytes), 0)) AS extend_sz,
    ROUND(CAST(NULLIF(SUM(extend_time), 0.0) / 1000 AS numeric),2) AS extend_time,
    NULLIF(SUM(hits), 0) AS hits,
    NULLIF(SUM(evictions), 0) AS evictions,
    NULLIF(SUM(reuses), 0) AS reuses,
    NULLIF(SUM(fsyncs), 0) AS fsyncs,
    ROUND(CAST(NULLIF(SUM(fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time

  FROM cluster_stat_io(sserver_id, start_id, end_id)
  GROUP BY ROLLUP(object, backend_type, context)
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_io_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    backend_type          text,
    object                text,
    context               text,

    reads1                bigint,
    read_sz1              text,
    read_time1            numeric,
    writes1               bigint,
    write_sz1             text,
    write_time1           numeric,
    writebacks1           bigint,
    writeback_sz1         text,
    writeback_time1       numeric,
    extends1              bigint,
    extend_sz1            text,
    extend_time1          numeric,
    hits1                 bigint,
    evictions1            bigint,
    reuses1               bigint,
    fsyncs1               bigint,
    fsync_time1           double precision,

    reads2                bigint,
    read_sz2              text,
    read_time2            numeric,
    writes2               bigint,
    write_sz2             text,
    write_time2           numeric,
    writebacks2           bigint,
    writeback_sz2         text,
    writeback_time2       numeric,
    extends2              bigint,
    extend_sz2            text,
    extend_time2          numeric,
    hits2                 bigint,
    evictions2            bigint,
    reuses2               bigint,
    fsyncs2               bigint,
    fsync_time2           double precision
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(backend_type, 'Total'),
    COALESCE(object, '*'),
    COALESCE(context, '*'),

    NULLIF(SUM(st1.reads), 0) AS reads1,
    pg_size_pretty(NULLIF(SUM(st1.read_bytes), 0)) AS read_sz1,
    ROUND(CAST(NULLIF(SUM(st1.read_time), 0.0) / 1000 AS numeric),2) AS read_time1,
    NULLIF(SUM(st1.writes), 0) AS writes1,
    pg_size_pretty(NULLIF(SUM(st1.write_bytes), 0)) AS write_sz1,
    ROUND(CAST(NULLIF(SUM(st1.write_time), 0.0) / 1000 AS numeric),2) AS write_time1,
    NULLIF(SUM(st1.writebacks), 0) AS writebacks1,
    pg_size_pretty(NULLIF(SUM(st1.writeback_bytes), 0)) AS writeback_sz1,
    ROUND(CAST(NULLIF(SUM(st1.writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time1,
    NULLIF(SUM(st1.extends), 0) AS extends1,
    pg_size_pretty(NULLIF(SUM(st1.extend_bytes), 0)) AS extend_sz1,
    ROUND(CAST(NULLIF(SUM(st1.extend_time), 0.0) / 1000 AS numeric),2) AS extend_time1,
    NULLIF(SUM(st1.hits), 0) AS hits1,
    NULLIF(SUM(st1.evictions), 0) AS evictions1,
    NULLIF(SUM(st1.reuses), 0) AS reuses1,
    NULLIF(SUM(st1.fsyncs), 0) AS fsyncs1,
    ROUND(CAST(NULLIF(SUM(st1.fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time1,

    NULLIF(SUM(st2.reads), 0) AS reads2,
    pg_size_pretty(NULLIF(SUM(st2.read_bytes), 0)) AS read_sz2,
    ROUND(CAST(NULLIF(SUM(st2.read_time), 0.0) / 1000 AS numeric),2) AS read_time2,
    NULLIF(SUM(st2.writes), 0) AS writes2,
    pg_size_pretty(NULLIF(SUM(st2.write_bytes), 0)) AS write_sz2,
    ROUND(CAST(NULLIF(SUM(st2.write_time), 0.0) / 1000 AS numeric),2) AS write_time2,
    NULLIF(SUM(st2.writebacks), 0) AS writebacks2,
    pg_size_pretty(NULLIF(SUM(st2.writeback_bytes), 0)) AS writeback_sz2,
    ROUND(CAST(NULLIF(SUM(st2.writeback_time), 0.0) / 1000 AS numeric),2) AS writeback_time2,
    NULLIF(SUM(st2.extends), 0) AS extends2,
    pg_size_pretty(NULLIF(SUM(st2.extend_bytes), 0)) AS extend_sz2,
    ROUND(CAST(NULLIF(SUM(st2.extend_time), 0.0) / 1000 AS numeric),2) AS extend_time2,
    NULLIF(SUM(st2.hits), 0) AS hits2,
    NULLIF(SUM(st2.evictions), 0) AS evictions2,
    NULLIF(SUM(st2.reuses), 0) AS reuses2,
    NULLIF(SUM(st2.fsyncs), 0) AS fsyncs2,
    ROUND(CAST(NULLIF(SUM(st2.fsync_time), 0.0) / 1000 AS numeric),2) AS fsync_time2
    
  FROM cluster_stat_io(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN cluster_stat_io(sserver_id, start2_id, end2_id) st2
    USING (server_id, backend_type, object, context)
  GROUP BY ROLLUP(object, backend_type, context)
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_io_resets(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
)
SET search_path=@extschema@ AS $$
    WITH first_val AS (
      SELECT backend_type, object, context, stats_reset
      FROM sample_stat_io st JOIN (
         SELECT backend_type, object, context, MIN(sample_id) AS sample_id
         FROM sample_stat_io
         WHERE server_id = sserver_id AND
           sample_id BETWEEN start_id AND end_id
         GROUP BY backend_type, object, context
       ) f USING (backend_type, object, context, sample_id)
     WHERE st.server_id = sserver_id
    )
    SELECT
      server_id,
      min(sample_id),
      backend_type,
      object,
      context,
      st.stats_reset
    FROM sample_stat_io st JOIN first_val USING (backend_type, object, context)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      AND st.stats_reset IS DISTINCT FROM first_val.stats_reset
    GROUP BY server_id, backend_type, object, context, st.stats_reset
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_io_reset_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    backend_type,
    object,
    context,
    stats_reset
  FROM cluster_stat_io_resets(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_io_reset_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    sample_id     integer,
    backend_type  text,
    object        text,
    context       text,
    stats_reset   timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    backend_type,
    object,
    context,
    stats_reset
  FROM (
    SELECT
      sample_id,
      backend_type,
      object,
      context,
      stats_reset
    FROM cluster_stat_io_resets(sserver_id, start1_id, end1_id)
    UNION
    SELECT
      sample_id,
      backend_type,
      object,
      context,
      stats_reset
    FROM cluster_stat_io_resets(sserver_id, start2_id, end2_id)
    ) st
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    name          text,
    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint
)
SET search_path=@extschema@ AS $$
    SELECT
        st.server_id AS server_id,
        st.name AS name,
        SUM(blks_zeroed)::bigint AS blks_zeroed,
        SUM(blks_hit)::bigint AS blks_hit,
        SUM(blks_read)::bigint AS blks_read,
        SUM(blks_written)::bigint AS blks_written,
        SUM(blks_exists)::bigint AS blks_exists,
        SUM(flushes)::bigint AS flushes,
        SUM(truncates)::bigint AS truncates
    FROM sample_stat_slru st
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id, st.name
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    name          text,

    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    hit_pct       numeric,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(name, 'Total') AS name,

    NULLIF(SUM(blks_zeroed), 0) AS blks_zeroed,
    NULLIF(SUM(blks_hit), 0) AS blks_hit,
    NULLIF(SUM(blks_read), 0) AS blks_read,
    ROUND(NULLIF(SUM(blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(blks_hit), 0) + COALESCE(SUM(blks_read), 0), 0), 2)
      AS hit_pct,
    NULLIF(SUM(blks_written), 0) AS blks_written,
    NULLIF(SUM(blks_exists), 0) AS blks_exists,
    NULLIF(SUM(flushes), 0) AS flushes,
    NULLIF(SUM(truncates), 0) AS truncates

  FROM cluster_stat_slru(sserver_id, start_id, end_id)
  GROUP BY ROLLUP(name)
  ORDER BY NULLIF(name, 'Total') ASC NULLS LAST
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    name          text,

    blks_zeroed1  bigint,
    blks_hit1     bigint,
    blks_read1    bigint,
    hit_pct1      numeric,
    blks_written1 bigint,
    blks_exists1  bigint,
    flushes1      bigint,
    truncates1    bigint,

    blks_zeroed2  bigint,
    blks_hit2     bigint,
    blks_read2    bigint,
    hit_pct2      numeric,
    blks_written2 bigint,
    blks_exists2  bigint,
    flushes2      bigint,
    truncates2    bigint
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(name, 'Total') AS name,

    NULLIF(SUM(st1.blks_zeroed), 0) AS blks_zeroed1,
    NULLIF(SUM(st1.blks_hit), 0) AS blks_hit1,
    NULLIF(SUM(st1.blks_read), 0) AS blks_read1,
    ROUND(NULLIF(SUM(st1.blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(st1.blks_hit), 0) + COALESCE(SUM(st1.blks_read), 0), 0), 2)
      AS hit_pct1,
    NULLIF(SUM(st1.blks_written), 0) AS blks_written1,
    NULLIF(SUM(st1.blks_exists), 0) AS blks_exists1,
    NULLIF(SUM(st1.flushes), 0) AS flushes1,
    NULLIF(SUM(st1.truncates), 0) AS truncates1,

    NULLIF(SUM(st2.blks_zeroed), 0) AS blks_zeroed2,
    NULLIF(SUM(st2.blks_hit), 0) AS blks_hit2,
    NULLIF(SUM(st2.blks_read), 0) AS blks_read2,
    ROUND(NULLIF(SUM(st2.blks_hit), 0)::numeric * 100 /
      NULLIF(COALESCE(SUM(st2.blks_hit), 0) + COALESCE(SUM(st2.blks_read), 0), 0), 2)
      AS hit_pct2,
    NULLIF(SUM(st2.blks_written), 0) AS blks_written2,
    NULLIF(SUM(st2.blks_exists), 0) AS blks_exists2,
    NULLIF(SUM(st2.flushes), 0) AS flushes2,
    NULLIF(SUM(st2.truncates), 0) AS truncates2
    
  FROM cluster_stat_slru(sserver_id, start1_id, end1_id) st1
    FULL OUTER JOIN cluster_stat_slru(sserver_id, start2_id, end2_id) st2
    USING (server_id, name)
  GROUP BY ROLLUP(name)
  ORDER BY NULLIF(name, 'Total') ASC NULLS LAST
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru_resets(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    server_id     integer,
    sample_id     integer,
    name          text,
    stats_reset   timestamp with time zone
)
SET search_path=@extschema@ AS $$
    WITH first_val AS (
      SELECT name, stats_reset
      FROM sample_stat_slru st JOIN (
         SELECT name, MIN(sample_id) AS sample_id
         FROM sample_stat_slru
         WHERE server_id = sserver_id AND
           sample_id BETWEEN start_id AND end_id
         GROUP BY name
       ) f USING (name,sample_id)
     WHERE st.server_id = sserver_id
    )
    SELECT
      server_id,
      min(sample_id),
      name,
      st.stats_reset
    FROM sample_stat_slru st JOIN first_val USING (name)
    WHERE st.server_id = sserver_id AND st.sample_id BETWEEN start_id + 1 AND end_id
      AND st.stats_reset IS DISTINCT FROM first_val.stats_reset
    GROUP BY server_id, name, st.stats_reset
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru_reset_format(IN sserver_id integer,
  IN start_id integer, IN end_id integer)
RETURNS TABLE(
    sample_id     integer,
    name          text,
    stats_reset  timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    name,
    stats_reset
  FROM cluster_stat_slru_resets(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
CREATE FUNCTION cluster_stat_slru_reset_format(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    sample_id     integer,
    name          text,
    stats_reset  timestamp with time zone
) SET search_path=@extschema@ AS $$
  SELECT
    sample_id,
    name,
    stats_reset
  FROM (
    SELECT
      sample_id,
      name,
      stats_reset
    FROM cluster_stat_slru_resets(sserver_id, start1_id, end1_id)
    UNION
    SELECT
      sample_id,
      name,
      stats_reset
    FROM cluster_stat_slru_resets(sserver_id, start2_id, end2_id)
    ) st
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
CREATE FUNCTION dbstats_reset(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    dbname      name,
    stats_reset timestamp with time zone,
    sample_id   integer
  )
SET search_path=@extschema@ AS
$$
    SELECT
        st1.datname as dbname,
        st1.stats_reset,
        st1.sample_id
    FROM sample_stat_database st1
        LEFT JOIN sample_stat_database st0 ON
          (st0.server_id = st1.server_id AND st0.sample_id = st1.sample_id - 1 AND st0.datid = st1.datid)
    WHERE st1.server_id = sserver_id AND NOT st1.datistemplate AND st1.sample_id BETWEEN start_id + 1 AND end_id
      AND st1.stats_reset IS DISTINCT FROM st0.stats_reset
$$ LANGUAGE sql;
CREATE FUNCTION dbstats_reset_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    dbname      name,
    stats_reset text,
    sample_id   integer
  )
    SET search_path=@extschema@
AS
$$
  SELECT
    dbname,
    stats_reset::text as stats_reset,
    sample_id
  FROM dbstats_reset(sserver_id, start_id, end_id)
  ORDER BY sample_id ASC
$$ LANGUAGE sql;
CREATE FUNCTION dbstats_reset_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  interval_num integer,
  dbname       name,
  stats_reset  text,
  sample_id    integer
)
SET search_path=@extschema@
AS
$$
  SELECT
    interval_num,
    dbname,
    stats_reset::text as stats_reset,
    sample_id
  FROM
    (SELECT 1 AS interval_num, dbname, stats_reset, sample_id
      FROM dbstats_reset(sserver_id, start1_id, end1_id)
    UNION
    SELECT 2 AS interval_num, dbname, stats_reset, sample_id
      FROM dbstats_reset(sserver_id, start2_id, end2_id)) AS samples
  ORDER BY interval_num, sample_id ASC;
$$ LANGUAGE sql;
CREATE FUNCTION report_queries_format(IN report_context jsonb, IN sserver_id integer, IN queries_list jsonb,
  IN start1_id integer, IN end1_id integer, IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
  hexqueryid  text,
  query_text1 text,
  query_text2 text,
  query_text3 text
)
SET search_path=@extschema@ AS $$
DECLARE
    c_queries CURSOR(lim integer)
    FOR
    SELECT
      queryid,
      ord,
      rowspan,
      query
    FROM (
      SELECT
      queryid,
      row_number() OVER (PARTITION BY queryid
        ORDER BY
          last_sample_id DESC NULLS FIRST,
          queryid_md5 DESC NULLS FIRST
        ) ord,
      -- Calculate a value for statement rowspan attribute
      least(count(*) OVER (PARTITION BY queryid),lim) rowspan,
      query
      FROM (
        SELECT DISTINCT
          server_id,
          queryid,
          queryid_md5
        FROM
          jsonb_to_recordset(queries_list) ql(
            userid   bigint,
            datid    bigint,
            queryid  bigint
          )
          JOIN sample_statements ss USING (datid, userid, queryid)
        WHERE
          ss.server_id = sserver_id
          AND (
            sample_id BETWEEN start1_id AND end1_id
            OR sample_id BETWEEN start2_id AND end2_id
          )
      ) queryids
      JOIN stmt_list USING (server_id, queryid_md5)
      WHERE query IS NOT NULL
    ) ord_stmt_v
    WHERE ord <= lim
    ORDER BY
      queryid ASC,
      ord ASC;

    qr_result         RECORD;
    qlen_limit        integer;
    query_text        text := '';
    query_text_keys   jsonb;
    queryid_entry     jsonb;
    lim               CONSTANT integer := 3;
BEGIN
    IF NOT has_column_privilege('stmt_list', 'query', 'SELECT') THEN
      -- Return empty set when permissions denied to see query text
      hexqueryid := '';
      query_text1 := 'You must be a member of pg_read_all_stats to access query texts';
      RETURN NEXT;
      RETURN;
    END IF;
    qlen_limit := (report_context #>> '{report_properties,max_query_length}')::integer;
    FOR qr_result IN c_queries(lim)
    LOOP
        -- New query entry
        IF qr_result.ord = 1 THEN
          hexqueryid := to_hex(qr_result.queryid);
          query_text1 := NULL;
          query_text2 := NULL;
          query_text3 := NULL;
        END IF;
        -- Collect query texts
        CASE qr_result.ord
          WHEN 1 THEN
            query_text1 := left(qr_result.query, qlen_limit);
          WHEN 2 THEN
            query_text2 := left(qr_result.query, qlen_limit);
          WHEN 3 THEN
            query_text3 := left(qr_result.query, qlen_limit);
          ELSE
            RAISE 'Unexpected queryid index';
        END CASE;
        -- Return collected texts
        IF qr_result.ord = qr_result.rowspan THEN
          RETURN NEXT;
        END IF;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION top_tables(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    reltoastrelid       oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_tup_newpage_upd   bigint,
    np_upd_pct          numeric,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    growth              bigint,
    relpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        st.reltoastrelid,
        sample_db.datname AS dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.seq_tup_read)::bigint AS seq_tup_read,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(st.n_tup_ins)::bigint AS n_tup_ins,
        sum(st.n_tup_upd)::bigint AS n_tup_upd,
        sum(st.n_tup_del)::bigint AS n_tup_del,
        sum(st.n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(st.n_tup_newpage_upd)::bigint AS n_tup_newpage_upd,
        sum(st.n_tup_newpage_upd)::numeric * 100 /
          NULLIF(sum(st.n_tup_upd)::numeric, 0) AS np_upd_pct,
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        CASE WHEN bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) THEN
          sum(st.seq_scan * st.relsize)::bigint
        ELSE NULL
        END AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
    WHERE st.server_id = sserver_id AND st.relkind IN ('r','m')
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,st.reltoastrelid,sample_db.datname,st.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;
CREATE FUNCTION top_toasts(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid               oid,
    relid               oid,
    dbname              name,
    tablespacename      name,
    schemaname          name,
    relname             name,
    seq_scan            bigint,
    seq_tup_read        bigint,
    idx_scan            bigint,
    idx_tup_fetch       bigint,
    n_tup_ins           bigint,
    n_tup_upd           bigint,
    n_tup_del           bigint,
    n_tup_hot_upd       bigint,
    n_tup_newpage_upd   bigint,
    np_upd_pct          numeric,
    vacuum_count        bigint,
    autovacuum_count    bigint,
    analyze_count       bigint,
    autoanalyze_count   bigint,
    growth              bigint,
    relpagegrowth_bytes bigint,
    seqscan_bytes_relsize bigint,
    seqscan_bytes_relpages bigint
) SET search_path=@extschema@ AS $$
    SELECT
        st.datid,
        st.relid,
        sample_db.datname AS dbname,
        st.tablespacename,
        st.schemaname,
        st.relname,
        sum(st.seq_scan)::bigint AS seq_scan,
        sum(st.seq_tup_read)::bigint AS seq_tup_read,
        sum(st.idx_scan)::bigint AS idx_scan,
        sum(st.idx_tup_fetch)::bigint AS idx_tup_fetch,
        sum(st.n_tup_ins)::bigint AS n_tup_ins,
        sum(st.n_tup_upd)::bigint AS n_tup_upd,
        sum(st.n_tup_del)::bigint AS n_tup_del,
        sum(st.n_tup_hot_upd)::bigint AS n_tup_hot_upd,
        sum(st.n_tup_newpage_upd)::bigint AS n_tup_newpage_upd,
        sum(st.n_tup_newpage_upd)::numeric * 100 /
          NULLIF(sum(st.n_tup_upd)::numeric, 0) AS np_upd_pct,
        sum(st.vacuum_count)::bigint AS vacuum_count,
        sum(st.autovacuum_count)::bigint AS autovacuum_count,
        sum(st.analyze_count)::bigint AS analyze_count,
        sum(st.autoanalyze_count)::bigint AS autoanalyze_count,
        sum(st.relsize_diff)::bigint AS growth,
        sum(st.relpages_bytes_diff)::bigint AS relpagegrowth_bytes,
        CASE WHEN bool_and(COALESCE(st.seq_scan, 0) = 0 OR st.relsize IS NOT NULL) THEN
          sum(st.seq_scan * st.relsize)::bigint
        ELSE NULL
        END AS seqscan_bytes_relsize,
        sum(st.seq_scan * st.relpages_bytes)::bigint AS seqscan_bytes_relpages
    FROM v_sample_stat_tables st
        -- Database name
        JOIN sample_stat_database sample_db
          USING (server_id, sample_id, datid)
    WHERE st.server_id = sserver_id AND st.relkind = 't'
      AND NOT sample_db.datistemplate
      AND st.sample_id BETWEEN start_id + 1 AND end_id
    GROUP BY st.server_id,st.datid,st.relid,sample_db.datname,st.tablespacename,st.schemaname,st.relname
$$ LANGUAGE sql;
CREATE FUNCTION top_tables_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE(
    datid                     oid,
    relid                     oid,
    reltoastrelid             oid,
    dbname                    name,
    tablespacename            name,
    schemaname                name,
    relname                   name,
    toastrelname              text,

    seq_scan                  bigint,
    seq_tup_read              bigint,
    idx_scan                  bigint,
    idx_tup_fetch             bigint,
    n_tup_ins                 bigint,
    n_tup_upd                 bigint,
    n_tup_del                 bigint,
    n_tup_hot_upd             bigint,
    n_tup_newpage_upd         bigint,
    np_upd_pct                numeric,
    vacuum_count              bigint,
    autovacuum_count          bigint,
    analyze_count             bigint,
    autoanalyze_count         bigint,

    toastseq_scan             bigint,
    toastseq_tup_read         bigint,
    toastidx_scan             bigint,
    toastidx_tup_fetch        bigint,
    toastn_tup_ins            bigint,
    toastn_tup_upd            bigint,
    toastn_tup_del            bigint,
    toastn_tup_hot_upd        bigint,
    toastn_tup_newpage_upd    bigint,
    toastnp_upd_pct           numeric,
    toastvacuum_count         bigint,
    toastautovacuum_count     bigint,
    toastanalyze_count        bigint,
    toastautoanalyze_count    bigint,

    growth_pretty             text,
    toastgrowth_pretty        text,
    seqscan_bytes_pretty      text,
    t_seqscan_bytes_pretty    text,
    relsize_pretty            text,
    t_relsize_pretty          text,

    ord_dml                   integer,
    ord_seq_scan              integer,
    ord_upd                   integer,
    ord_upd_np                integer,
    ord_growth                integer,
    ord_vac                   integer,
    ord_anl                   integer
  )
SET search_path=@extschema@ AS $$
  WITH rsa AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start_id + 1 AND end_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    )
  SELECT
    rel.datid,
    rel.relid,
    rel.reltoastrelid,
    rel.dbname,
    rel.tablespacename,
    rel.schemaname,
    rel.relname,
    CASE WHEN COALESCE(rel.reltoastrelid, 0) > 0
        THEN rel.relname || '(TOAST)' ELSE NULL
    END as toastrelname,
    
    NULLIF(rel.seq_scan, 0) AS seq_scan,
    NULLIF(rel.seq_tup_read, 0) AS seq_tup_read,
    NULLIF(rel.idx_scan, 0) AS idx_scan,
    NULLIF(rel.idx_tup_fetch, 0) AS idx_tup_fetch,
    NULLIF(rel.n_tup_ins, 0) AS n_tup_ins,
    NULLIF(rel.n_tup_upd, 0) AS n_tup_upd,
    NULLIF(rel.n_tup_del, 0) AS n_tup_del,
    NULLIF(rel.n_tup_hot_upd, 0) AS n_tup_hot_upd,
    NULLIF(rel.n_tup_newpage_upd, 0) AS n_tup_newpage_upd,
    ROUND(NULLIF(rel.np_upd_pct, 0), 1) AS np_upd_pct,
    NULLIF(rel.vacuum_count, 0) AS vacuum_count,
    NULLIF(rel.autovacuum_count, 0) AS autovacuum_count,
    NULLIF(rel.analyze_count, 0) AS analyze_count,
    NULLIF(rel.autoanalyze_count, 0) AS autoanalyze_count,
    
    NULLIF(toast.seq_scan, 0) AS toastseq_scan,
    NULLIF(toast.seq_tup_read, 0) AS toastseq_tup_read,
    NULLIF(toast.idx_scan, 0) AS toastidx_scan,
    NULLIF(toast.idx_tup_fetch, 0) AS toastidx_tup_fetch,
    NULLIF(toast.n_tup_ins, 0) AS toastn_tup_ins,
    NULLIF(toast.n_tup_upd, 0) AS toastn_tup_upd,
    NULLIF(toast.n_tup_del, 0) AS toastn_tup_del,
    NULLIF(toast.n_tup_hot_upd, 0) AS toastn_tup_hot_upd,
    NULLIF(toast.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd,
    ROUND(NULLIF(toast.np_upd_pct, 0), 1) AS toastnp_upd_pct,
    NULLIF(toast.vacuum_count, 0) AS toastvacuum_count,
    NULLIF(toast.autovacuum_count, 0) AS toastautovacuum_count,
    NULLIF(toast.analyze_count, 0) AS toastanalyze_count,
    NULLIF(toast.autoanalyze_count, 0) AS toastautoanalyze_count,
    
    CASE WHEN relrs.growth_avail THEN
      pg_size_pretty(NULLIF(rel.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty,

    CASE WHEN toastrs.growth_avail THEN
      pg_size_pretty(NULLIF(toast.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(rel.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(toast.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(relrs.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs.relpages_bytes, 0))||']'
    ) AS relsize_pretty,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty,

    CASE WHEN
      COALESCE(rel.n_tup_ins, 0) + COALESCE(rel.n_tup_upd, 0) +
      COALESCE(rel.n_tup_del, 0) + COALESCE(toast.n_tup_ins, 0) +
      COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_ins, 0) + COALESCE(rel.n_tup_upd, 0) +
        COALESCE(rel.n_tup_del, 0) + COALESCE(toast.n_tup_ins, 0) +
        COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_dml,

    CASE WHEN
      COALESCE(rel.seqscan_bytes_relsize, rel.seqscan_bytes_relpages, 0) +
      COALESCE(toast.seqscan_bytes_relsize, toast.seqscan_bytes_relpages, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.seqscan_bytes_relsize, rel.seqscan_bytes_relpages, 0) +
        COALESCE(toast.seqscan_bytes_relsize, toast.seqscan_bytes_relpages, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_seq_scan,

    CASE WHEN
      COALESCE(rel.n_tup_upd, 0) + COALESCE(rel.n_tup_del, 0) +
      COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_upd, 0) + COALESCE(rel.n_tup_del, 0) +
        COALESCE(toast.n_tup_upd, 0) + COALESCE(toast.n_tup_del, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_upd,

    CASE WHEN
      COALESCE(rel.n_tup_newpage_upd, 0) + COALESCE(toast.n_tup_newpage_upd, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.n_tup_newpage_upd, 0) + COALESCE(toast.n_tup_newpage_upd, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_upd_np,

    CASE WHEN
      ((relrs.growth_avail AND rel.growth > 0) OR rel.relpagegrowth_bytes > 0) OR
      ((toastrs.growth_avail AND toast.growth > 0) OR toast.relpagegrowth_bytes > 0)
    THEN
      row_number() OVER (ORDER BY
        CASE WHEN relrs.growth_avail THEN rel.growth ELSE rel.relpagegrowth_bytes END +
        COALESCE(CASE WHEN toastrs.growth_avail THEN toast.growth ELSE toast.relpagegrowth_bytes END, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_growth,

    CASE WHEN
      COALESCE(rel.vacuum_count, 0) + COALESCE(rel.autovacuum_count, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.vacuum_count, 0) + COALESCE(rel.autovacuum_count, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_vac,

    CASE WHEN
      COALESCE(rel.analyze_count, 0) + COALESCE(rel.autoanalyze_count, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel.analyze_count, 0) + COALESCE(rel.autoanalyze_count, 0)
        DESC NULLS LAST,
        rel.datid,
        rel.relid)::integer
    ELSE NULL END AS ord_anl
  FROM (
      top_tables(sserver_id, start_id, end_id) AS rel
      JOIN rsa AS relrs USING (datid, relid)
    )
    LEFT OUTER JOIN (
      top_toasts(sserver_id, start_id, end_id) AS toast
      JOIN rsa AS toastrs USING (datid, relid)
    ) ON (rel.datid, rel.reltoastrelid) = (toast.datid, toast.relid)
$$ LANGUAGE sql;
CREATE FUNCTION top_tables_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE(
    datid                     oid,
    relid                     oid,
    reltoastrelid             oid,
    dbname                    name,
    tablespacename            name,
    schemaname                name,
    relname                   name,
    toastrelname              text,

    seq_scan1                 bigint,
    seq_tup_read1             bigint,
    idx_scan1                 bigint,
    idx_tup_fetch1            bigint,
    n_tup_ins1                bigint,
    n_tup_upd1                bigint,
    n_tup_del1                bigint,
    n_tup_hot_upd1            bigint,
    n_tup_newpage_upd1        bigint,
    np_upd_pct1               numeric,
    vacuum_count1             bigint,
    autovacuum_count1         bigint,
    analyze_count1            bigint,
    autoanalyze_count1        bigint,

    toastseq_scan1            bigint,
    toastseq_tup_read1        bigint,
    toastidx_scan1            bigint,
    toastidx_tup_fetch1       bigint,
    toastn_tup_ins1           bigint,
    toastn_tup_upd1           bigint,
    toastn_tup_del1           bigint,
    toastn_tup_hot_upd1       bigint,
    toastn_tup_newpage_upd1   bigint,
    toastnp_upd_pct1          numeric,
    toastvacuum_count1        bigint,
    toastautovacuum_count1    bigint,
    toastanalyze_count1       bigint,
    toastautoanalyze_count1   bigint,

    growth_pretty1            text,
    toastgrowth_pretty1       text,
    seqscan_bytes_pretty1     text,
    t_seqscan_bytes_pretty1   text,
    relsize_pretty1           text,
    t_relsize_pretty1         text,

    seq_scan2                 bigint,
    seq_tup_read2             bigint,
    idx_scan2                 bigint,
    idx_tup_fetch2            bigint,
    n_tup_ins2                bigint,
    n_tup_upd2                bigint,
    n_tup_del2                bigint,
    n_tup_hot_upd2            bigint,
    n_tup_newpage_upd2        bigint,
    np_upd_pct2               numeric,
    vacuum_count2             bigint,
    autovacuum_count2         bigint,
    analyze_count2            bigint,
    autoanalyze_count2        bigint,

    toastseq_scan2            bigint,
    toastseq_tup_read2        bigint,
    toastidx_scan2            bigint,
    toastidx_tup_fetch2       bigint,
    toastn_tup_ins2           bigint,
    toastn_tup_upd2           bigint,
    toastn_tup_del2           bigint,
    toastn_tup_hot_upd2       bigint,
    toastn_tup_newpage_upd2   bigint,
    toastnp_upd_pct2          numeric,
    toastvacuum_count2        bigint,
    toastautovacuum_count2    bigint,
    toastanalyze_count2       bigint,
    toastautoanalyze_count2   bigint,

    growth_pretty2            text,
    toastgrowth_pretty2       text,
    seqscan_bytes_pretty2     text,
    t_seqscan_bytes_pretty2   text,
    relsize_pretty2           text,
    t_relsize_pretty2         text,

    ord_dml                   integer,
    ord_seq_scan              integer,
    ord_upd                   integer,
    ord_upd_np                integer,
    ord_growth                integer,
    ord_vac                   integer,
    ord_anl                   integer
  )
SET search_path=@extschema@ AS $$
  WITH rsa1 AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start1_id + 1 AND end1_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    ),
    rsa2 AS (
      SELECT
        rs.datid,
        rs.relid,
        rs.growth_avail,
        sst.relsize,
        sst.relpages_bytes
      FROM
        (SELECT
          datid,
          relid,
          max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) AND
          min(sample_id) = min(sample_id) FILTER (WHERE relsize IS NOT NULL) AS growth_avail,
          CASE WHEN max(sample_id) = max(sample_id) FILTER (WHERE relsize IS NOT NULL) THEN
            max(sample_id) FILTER (WHERE relsize IS NOT NULL)
          ELSE
            max(sample_id) FILTER (WHERE relpages_bytes IS NOT NULL)
          END AS sid
        FROM
          sample_stat_tables
        WHERE
          server_id = sserver_id AND
          sample_id BETWEEN start2_id + 1 AND end2_id
        GROUP BY server_id, datid, relid) AS rs
        JOIN sample_stat_tables sst ON
          (sst.server_id, sst.sample_id, sst.datid, sst.relid) =
          (sserver_id, rs.sid, rs.datid, rs.relid)
    )
  SELECT
    COALESCE(rel1.datid, rel2.datid) AS datid,
    COALESCE(rel1.relid, rel2.relid) AS relid,
    COALESCE(rel1.reltoastrelid, rel2.reltoastrelid) as reltoastrelid,
    COALESCE(rel1.dbname, rel2.dbname) AS dbname,
    COALESCE(rel1.tablespacename, rel2.tablespacename) AS tablespacename,
    COALESCE(rel1.schemaname, rel2.schemaname) AS schemaname,
    COALESCE(rel1.relname, rel2.relname) AS relname,
    CASE WHEN COALESCE(rel1.reltoastrelid, rel2.reltoastrelid, 0) > 0
        THEN COALESCE(rel1.relname, rel2.relname) || '(TOAST)' ELSE NULL
    END as toastrelname,
    
    NULLIF(rel1.seq_scan, 0) AS seq_scan1,
    NULLIF(rel1.seq_tup_read, 0) AS seq_tup_read1,
    NULLIF(rel1.idx_scan, 0) AS idx_scan1,
    NULLIF(rel1.idx_tup_fetch, 0) AS idx_tup_fetch1,
    NULLIF(rel1.n_tup_ins, 0) AS n_tup_ins1,
    NULLIF(rel1.n_tup_upd, 0) AS n_tup_upd1,
    NULLIF(rel1.n_tup_del, 0) AS n_tup_del1,
    NULLIF(rel1.n_tup_hot_upd, 0) AS n_tup_hot_upd1,
    NULLIF(rel1.n_tup_newpage_upd, 0) AS n_tup_newpage_upd1,
    ROUND(NULLIF(rel1.np_upd_pct, 0), 1) AS np_upd_pct1,
    NULLIF(rel1.vacuum_count, 0) AS vacuum_count1,
    NULLIF(rel1.autovacuum_count, 0) AS autovacuum_count1,
    NULLIF(rel1.analyze_count, 0) AS analyze_count1,
    NULLIF(rel1.autoanalyze_count, 0) AS autoanalyze_count1,
    
    NULLIF(toast1.seq_scan, 0) AS toastseq_scan1,
    NULLIF(toast1.seq_tup_read, 0) AS toastseq_tup_read1,
    NULLIF(toast1.idx_scan, 0) AS toastidx_scan1,
    NULLIF(toast1.idx_tup_fetch, 0) AS toastidx_tup_fetch1,
    NULLIF(toast1.n_tup_ins, 0) AS toastn_tup_ins1,
    NULLIF(toast1.n_tup_upd, 0) AS toastn_tup_upd1,
    NULLIF(toast1.n_tup_del, 0) AS toastn_tup_del1,
    NULLIF(toast1.n_tup_hot_upd, 0) AS toastn_tup_hot_upd1,
    NULLIF(toast1.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd1,
    ROUND(NULLIF(toast1.np_upd_pct, 0), 1) AS toastnp_upd_pct1,
    NULLIF(toast1.vacuum_count, 0) AS toastvacuum_count1,
    NULLIF(toast1.autovacuum_count, 0) AS toastautovacuum_count1,
    NULLIF(toast1.analyze_count, 0) AS toastanalyze_count1,
    NULLIF(toast1.autoanalyze_count, 0) AS toastautoanalyze_count1,
    
    CASE WHEN relrs1.growth_avail THEN
      pg_size_pretty(NULLIF(rel1.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel1.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty1,

    CASE WHEN toastrs1.growth_avail THEN
      pg_size_pretty(NULLIF(toast1.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast1.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(rel1.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel1.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(toast1.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast1.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(relrs1.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs1.relpages_bytes, 0))||']'
    ) AS relsize_pretty1,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs1.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs1.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty1,

    NULLIF(rel2.seq_scan, 0) AS seq_scan2,
    NULLIF(rel2.seq_tup_read, 0) AS seq_tup_read2,
    NULLIF(rel2.idx_scan, 0) AS idx_scan2,
    NULLIF(rel2.idx_tup_fetch, 0) AS idx_tup_fetch2,
    NULLIF(rel2.n_tup_ins, 0) AS n_tup_ins2,
    NULLIF(rel2.n_tup_upd, 0) AS n_tup_upd2,
    NULLIF(rel2.n_tup_del, 0) AS n_tup_del2,
    NULLIF(rel2.n_tup_hot_upd, 0) AS n_tup_hot_upd2,
    NULLIF(rel2.n_tup_newpage_upd, 0) AS n_tup_newpage_upd2,
    ROUND(NULLIF(rel2.np_upd_pct, 0), 1) AS np_upd_pct2,
    NULLIF(rel2.vacuum_count, 0) AS vacuum_count2,
    NULLIF(rel2.autovacuum_count, 0) AS autovacuum_count2,
    NULLIF(rel2.analyze_count, 0) AS analyze_count2,
    NULLIF(rel2.autoanalyze_count, 0) AS autoanalyze_count2,
    
    NULLIF(toast2.seq_scan, 0) AS toastseq_scan2,
    NULLIF(toast2.seq_tup_read, 0) AS toastseq_tup_read2,
    NULLIF(toast2.idx_scan, 0) AS toastidx_scan2,
    NULLIF(toast2.idx_tup_fetch, 0) AS toastidx_tup_fetch2,
    NULLIF(toast2.n_tup_ins, 0) AS toastn_tup_ins2,
    NULLIF(toast2.n_tup_upd, 0) AS toastn_tup_upd2,
    NULLIF(toast2.n_tup_del, 0) AS toastn_tup_del2,
    NULLIF(toast2.n_tup_hot_upd, 0) AS toastn_tup_hot_upd2,
    NULLIF(toast2.n_tup_newpage_upd, 0) AS toastn_tup_newpage_upd2,
    ROUND(NULLIF(toast2.np_upd_pct, 0), 1) AS toastnp_upd_pct2,
    NULLIF(toast2.vacuum_count, 0) AS toastvacuum_count2,
    NULLIF(toast2.autovacuum_count, 0) AS toastautovacuum_count2,
    NULLIF(toast2.analyze_count, 0) AS toastanalyze_count2,
    NULLIF(toast2.autoanalyze_count, 0) AS toastautoanalyze_count2,
    
    CASE WHEN relrs2.growth_avail THEN
      pg_size_pretty(NULLIF(rel2.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(rel2.relpagegrowth_bytes, 0))||']'
    END AS growth_pretty2,

    CASE WHEN toastrs2.growth_avail THEN
      pg_size_pretty(NULLIF(toast2.growth, 0))
    ELSE
      '['||pg_size_pretty(NULLIF(toast2.relpagegrowth_bytes, 0))||']'
    END AS toastgrowth_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(rel2.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(rel2.seqscan_bytes_relpages, 0))||']'
    ) AS seqscan_bytes_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(toast2.seqscan_bytes_relsize, 0)),
      '['||pg_size_pretty(NULLIF(toast2.seqscan_bytes_relpages, 0))||']'
    ) AS t_seqscan_bytes_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(relrs2.relsize, 0)),
      '['||pg_size_pretty(NULLIF(relrs2.relpages_bytes, 0))||']'
    ) AS relsize_pretty2,

    COALESCE(
      pg_size_pretty(NULLIF(toastrs2.relsize, 0)),
      '['||pg_size_pretty(NULLIF(toastrs2.relpages_bytes, 0))||']'
    ) AS t_relsize_pretty2,

    CASE WHEN
      COALESCE(rel1.n_tup_ins, 0) + COALESCE(rel1.n_tup_upd, 0) +
      COALESCE(rel1.n_tup_del, 0) + COALESCE(toast1.n_tup_ins, 0) +
      COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
      COALESCE(rel2.n_tup_ins, 0) + COALESCE(rel2.n_tup_upd, 0) +
      COALESCE(rel2.n_tup_del, 0) + COALESCE(toast2.n_tup_ins, 0) +
      COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_ins, 0) + COALESCE(rel1.n_tup_upd, 0) +
        COALESCE(rel1.n_tup_del, 0) + COALESCE(toast1.n_tup_ins, 0) +
        COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
        COALESCE(rel2.n_tup_ins, 0) + COALESCE(rel2.n_tup_upd, 0) +
        COALESCE(rel2.n_tup_del, 0) + COALESCE(toast2.n_tup_ins, 0) +
        COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_dml,

    CASE WHEN
      COALESCE(rel1.seqscan_bytes_relsize, rel1.seqscan_bytes_relpages, 0) +
      COALESCE(toast1.seqscan_bytes_relsize, toast1.seqscan_bytes_relpages, 0) +
      COALESCE(rel2.seqscan_bytes_relsize, rel2.seqscan_bytes_relpages, 0) +
      COALESCE(toast2.seqscan_bytes_relsize, toast2.seqscan_bytes_relpages, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.seqscan_bytes_relsize, rel1.seqscan_bytes_relpages, 0) +
        COALESCE(toast1.seqscan_bytes_relsize, toast1.seqscan_bytes_relpages, 0) +
        COALESCE(rel2.seqscan_bytes_relsize, rel2.seqscan_bytes_relpages, 0) +
        COALESCE(toast2.seqscan_bytes_relsize, toast2.seqscan_bytes_relpages, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_seq_scan,

    CASE WHEN
      COALESCE(rel1.n_tup_upd, 0) + COALESCE(rel1.n_tup_del, 0) +
      COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
      COALESCE(rel2.n_tup_upd, 0) + COALESCE(rel2.n_tup_del, 0) +
      COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_upd, 0) + COALESCE(rel1.n_tup_del, 0) +
        COALESCE(toast1.n_tup_upd, 0) + COALESCE(toast1.n_tup_del, 0) +
        COALESCE(rel2.n_tup_upd, 0) + COALESCE(rel2.n_tup_del, 0) +
        COALESCE(toast2.n_tup_upd, 0) + COALESCE(toast2.n_tup_del, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_upd,

    CASE WHEN
      COALESCE(rel1.n_tup_newpage_upd, 0) + COALESCE(toast1.n_tup_newpage_upd, 0) +
      COALESCE(rel2.n_tup_newpage_upd, 0) + COALESCE(toast2.n_tup_newpage_upd, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.n_tup_newpage_upd, 0) + COALESCE(toast1.n_tup_newpage_upd, 0) +
        COALESCE(rel2.n_tup_newpage_upd, 0) + COALESCE(toast2.n_tup_newpage_upd, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_upd_np,

    CASE WHEN
      ((relrs1.growth_avail AND rel1.growth > 0) OR rel1.relpagegrowth_bytes > 0) OR
      ((toastrs1.growth_avail AND toast1.growth > 0) OR toast1.relpagegrowth_bytes > 0) OR
      ((relrs2.growth_avail AND rel2.growth > 0) OR rel2.relpagegrowth_bytes > 0) OR
      ((toastrs2.growth_avail AND toast2.growth > 0) OR toast2.relpagegrowth_bytes > 0)
    THEN
      row_number() OVER (ORDER BY
        CASE WHEN relrs1.growth_avail THEN rel1.growth ELSE rel1.relpagegrowth_bytes END +
        CASE WHEN toastrs1.growth_avail THEN toast1.growth ELSE toast1.relpagegrowth_bytes END +
        CASE WHEN relrs2.growth_avail THEN rel2.growth ELSE rel2.relpagegrowth_bytes END +
        CASE WHEN toastrs2.growth_avail THEN toast2.growth ELSE toast2.relpagegrowth_bytes END
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_growth,

    CASE WHEN
      COALESCE(rel1.vacuum_count, 0) + COALESCE(rel1.autovacuum_count, 0) +
      COALESCE(rel2.vacuum_count, 0) + COALESCE(rel2.autovacuum_count, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.vacuum_count, 0) + COALESCE(rel1.autovacuum_count, 0) +
        COALESCE(rel2.vacuum_count, 0) + COALESCE(rel2.autovacuum_count, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_vac,

    CASE WHEN
      COALESCE(rel1.analyze_count, 0) + COALESCE(rel1.autoanalyze_count, 0) +
      COALESCE(rel2.analyze_count, 0) + COALESCE(rel2.autoanalyze_count, 0)> 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.analyze_count, 0) + COALESCE(rel1.autoanalyze_count, 0) +
        COALESCE(rel2.analyze_count, 0) + COALESCE(rel2.autoanalyze_count, 0)
        DESC NULLS LAST,
        COALESCE(rel1.datid, rel2.datid),
        COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_anl
  FROM (
    -- Interval 1
      (
        top_tables(sserver_id, start1_id, end1_id) AS rel1
        JOIN rsa1 AS relrs1 USING (datid, relid)
      )
      LEFT OUTER JOIN (
        top_toasts(sserver_id, start1_id, end1_id) AS toast1
        JOIN rsa1 AS toastrs1 USING (datid, relid)
      ) ON (rel1.datid, rel1.reltoastrelid) = (toast1.datid, toast1.relid)
    ) FULL OUTER JOIN (
    -- Interval 2
      (
        top_tables(sserver_id, start2_id, end2_id) AS rel2
        JOIN rsa2 AS relrs2 USING (datid, relid)
      )
      LEFT OUTER JOIN (
        top_toasts(sserver_id, start2_id, end2_id) AS toast2
        JOIN rsa2 AS toastrs2 USING (datid, relid)
      ) ON (rel2.datid, rel2.reltoastrelid) = (toast2.datid, toast2.relid)
    ) ON (rel1.datid, rel1.relid) = (rel2.datid, rel2.relid)
$$ LANGUAGE sql;
CREATE FUNCTION top_io_tables_format(IN sserver_id integer, IN start_id integer, IN end_id integer)
RETURNS TABLE (
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,

    heap_blks_read              bigint,
    heap_blks_read_pct          numeric,
    heap_blks_fetch             bigint,
    heap_blks_proc_pct          numeric,
    idx_blks_read               bigint,
    idx_blks_read_pct           numeric,
    idx_blks_fetch              bigint,
    idx_blks_fetch_pct          numeric,
    toast_blks_read             bigint,
    toast_blks_read_pct         numeric,
    toast_blks_fetch            bigint,
    toast_blks_fetch_pct        numeric,
    tidx_blks_read              bigint,
    tidx_blks_read_pct          numeric,
    tidx_blks_fetch             bigint,
    tidx_blks_fetch_pct         numeric,
    seq_scan                    bigint,
    idx_scan                    bigint,
    hit_pct                     numeric,

    ord_read                    integer,
    ord_fetch                   integer
) SET search_path=@extschema@ AS $$
  SELECT
    datid,
    relid,
    dbname,
    tablespacename,
    schemaname,
    relname,

    NULLIF(heap_blks_read, 0) AS heap_blks_read,
    round(NULLIF(heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct,
    NULLIF(heap_blks_fetch, 0) AS heap_blks_fetch,
    round(NULLIF(heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct,
    NULLIF(idx_blks_read, 0) AS idx_blks_read,
    round(NULLIF(idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct,
    NULLIF(idx_blks_fetch, 0) AS idx_blks_fetch,
    round(NULLIF(idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct,
    NULLIF(toast_blks_read, 0) AS toast_blks_read,
    round(NULLIF(toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct,
    NULLIF(toast_blks_fetch, 0) AS toast_blks_fetch,
    round(NULLIF(toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct,
    NULLIF(tidx_blks_read, 0) AS tidx_blks_read,
    round(NULLIF(tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct,
    NULLIF(tidx_blks_fetch, 0) AS tidx_blks_fetch,
    round(NULLIF(tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct,
    NULLIF(seq_scan, 0) AS seq_scan,
    NULLIF(idx_scan, 0) AS idx_scan,
    round(
        100.0 - (COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) +
        COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)) * 100.0 /
        NULLIF(heap_blks_fetch + idx_blks_fetch + toast_blks_fetch + tidx_blks_fetch, 0),2
    ) AS hit_pct,

    CASE WHEN
      COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(heap_blks_read, 0) + COALESCE(idx_blks_read, 0) + COALESCE(toast_blks_read, 0) + COALESCE(tidx_blks_read, 0)
      DESC NULLS LAST,
      datid,
      relid)::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(heap_blks_fetch, 0) + COALESCE(idx_blks_fetch, 0) + COALESCE(toast_blks_fetch, 0) + COALESCE(tidx_blks_fetch, 0)
      DESC NULLS LAST,
      datid,
      relid)::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start_id, end_id)
$$ LANGUAGE sql;
CREATE FUNCTION top_io_tables_format_diff(IN sserver_id integer,
  IN start1_id integer, IN end1_id integer,
  IN start2_id integer, IN end2_id integer)
RETURNS TABLE (
    datid                       oid,
    relid                       oid,
    dbname                      name,
    tablespacename              name,
    schemaname                  name,
    relname                     name,

    heap_blks_read1             bigint,
    heap_blks_read_pct1         numeric,
    heap_blks_fetch1            bigint,
    heap_blks_proc_pct1         numeric,
    idx_blks_read1              bigint,
    idx_blks_read_pct1          numeric,
    idx_blks_fetch1             bigint,
    idx_blks_fetch_pct1         numeric,
    toast_blks_read1            bigint,
    toast_blks_read_pct1        numeric,
    toast_blks_fetch1           bigint,
    toast_blks_fetch_pct1       numeric,
    tidx_blks_read1             bigint,
    tidx_blks_read_pct1         numeric,
    tidx_blks_fetch1            bigint,
    tidx_blks_fetch_pct1        numeric,
    seq_scan1                   bigint,
    idx_scan1                   bigint,
    hit_pct1                    numeric,

    heap_blks_read2             bigint,
    heap_blks_read_pct2         numeric,
    heap_blks_fetch2            bigint,
    heap_blks_proc_pct2         numeric,
    idx_blks_read2              bigint,
    idx_blks_read_pct2          numeric,
    idx_blks_fetch2             bigint,
    idx_blks_fetch_pct2         numeric,
    toast_blks_read2            bigint,
    toast_blks_read_pct2        numeric,
    toast_blks_fetch2           bigint,
    toast_blks_fetch_pct2       numeric,
    tidx_blks_read2             bigint,
    tidx_blks_read_pct2         numeric,
    tidx_blks_fetch2            bigint,
    tidx_blks_fetch_pct2        numeric,
    seq_scan2                   bigint,
    idx_scan2                   bigint,
    hit_pct2                    numeric,

    ord_read                    integer,
    ord_fetch                   integer
) SET search_path=@extschema@ AS $$
  SELECT
    COALESCE(rel1.datid, rel2.datid) AS datid,
    COALESCE(rel1.relid, rel2.relid) AS relid,
    COALESCE(rel1.dbname, rel2.dbname) AS dbname,
    COALESCE(rel1.tablespacename, rel2.tablespacename) AS tablespacename,
    COALESCE(rel1.schemaname, rel2.schemaname) AS schemaname,
    COALESCE(rel1.relname, rel2.relname) AS relname,

    NULLIF(rel1.heap_blks_read, 0) AS heap_blks_read1,
    round(NULLIF(rel1.heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct1,
    NULLIF(rel1.heap_blks_fetch, 0) AS heap_blks_fetch1,
    round(NULLIF(rel1.heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct1,
    NULLIF(rel1.idx_blks_read, 0) AS idx_blks_read1,
    round(NULLIF(rel1.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct1,
    NULLIF(rel1.idx_blks_fetch, 0) AS idx_blks_fetch1,
    round(NULLIF(rel1.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct1,
    NULLIF(rel1.toast_blks_read, 0) AS toast_blks_read1,
    round(NULLIF(rel1.toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct1,
    NULLIF(rel1.toast_blks_fetch, 0) AS toast_blks_fetch1,
    round(NULLIF(rel1.toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct1,
    NULLIF(rel1.tidx_blks_read, 0) AS tidx_blks_read1,
    round(NULLIF(rel1.tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct1,
    NULLIF(rel1.tidx_blks_fetch, 0) AS tidx_blks_fetch1,
    round(NULLIF(rel1.tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct1,
    NULLIF(rel1.seq_scan, 0) AS seq_scan1,
    NULLIF(rel1.idx_scan, 0) AS idx_scan1,
    round(
        100.0 - (COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) +
        COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0)) * 100.0 /
        NULLIF(rel1.heap_blks_fetch + rel1.idx_blks_fetch + rel1.toast_blks_fetch + rel1.tidx_blks_fetch, 0),2
    ) AS hit_pct1,

    NULLIF(rel2.heap_blks_read, 0) AS heap_blks_read2,
    round(NULLIF(rel2.heap_blks_read_pct, 0.0), 2) AS heap_blks_read_pct2,
    NULLIF(rel2.heap_blks_fetch, 0) AS heap_blks_fetch2,
    round(NULLIF(rel2.heap_blks_proc_pct, 0.0), 2) AS heap_blks_proc_pct2,
    NULLIF(rel2.idx_blks_read, 0) AS idx_blks_read2,
    round(NULLIF(rel2.idx_blks_read_pct, 0.0), 2) AS idx_blks_read_pct2,
    NULLIF(rel2.idx_blks_fetch, 0) AS idx_blks_fetch2,
    round(NULLIF(rel2.idx_blks_fetch_pct, 0.0), 2) AS idx_blks_fetch_pct2,
    NULLIF(rel2.toast_blks_read, 0) AS toast_blks_read2,
    round(NULLIF(rel2.toast_blks_read_pct, 0.0), 2) AS toast_blks_read_pct2,
    NULLIF(rel2.toast_blks_fetch, 0) AS toast_blks_fetch2,
    round(NULLIF(rel2.toast_blks_fetch_pct, 0.0), 2) AS toast_blks_fetch_pct2,
    NULLIF(rel2.tidx_blks_read, 0) AS tidx_blks_read2,
    round(NULLIF(rel2.tidx_blks_read_pct, 0.0), 2) AS tidx_blks_read_pct2,
    NULLIF(rel2.tidx_blks_fetch, 0) AS tidx_blks_fetch2,
    round(NULLIF(rel2.tidx_blks_fetch_pct, 0.0), 2) AS tidx_blks_fetch_pct2,
    NULLIF(rel2.seq_scan, 0) AS seq_scan2,
    NULLIF(rel2.idx_scan, 0) AS idx_scan2,
    round(
        100.0 - (COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) +
        COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0)) * 100.0 /
        NULLIF(rel2.heap_blks_fetch + rel2.idx_blks_fetch + rel2.toast_blks_fetch + rel2.tidx_blks_fetch, 0),2
    ) AS hit_pct2,

    CASE WHEN
      COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0) +
      COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) + COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.heap_blks_read, 0) + COALESCE(rel1.idx_blks_read, 0) + COALESCE(rel1.toast_blks_read, 0) + COALESCE(rel1.tidx_blks_read, 0) +
        COALESCE(rel2.heap_blks_read, 0) + COALESCE(rel2.idx_blks_read, 0) + COALESCE(rel2.toast_blks_read, 0) + COALESCE(rel2.tidx_blks_read, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_read,

    CASE WHEN
      COALESCE(rel1.heap_blks_fetch, 0) + COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel1.toast_blks_fetch, 0) + COALESCE(rel1.tidx_blks_fetch, 0) +
      COALESCE(rel2.heap_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0) + COALESCE(rel2.toast_blks_fetch, 0) + COALESCE(rel2.tidx_blks_fetch, 0) > 0
    THEN
      row_number() OVER (ORDER BY
        COALESCE(rel1.heap_blks_fetch, 0) + COALESCE(rel1.idx_blks_fetch, 0) + COALESCE(rel1.toast_blks_fetch, 0) + COALESCE(rel1.tidx_blks_fetch, 0) +
        COALESCE(rel2.heap_blks_fetch, 0) + COALESCE(rel2.idx_blks_fetch, 0) + COALESCE(rel2.toast_blks_fetch, 0) + COALESCE(rel2.tidx_blks_fetch, 0)
      DESC NULLS LAST,
      COALESCE(rel1.datid, rel2.datid),
      COALESCE(rel1.relid, rel2.relid))::integer
    ELSE NULL END AS ord_fetch
  FROM
    top_io_tables(sserver_id, start1_id, end1_id) rel1
    FULL OUTER JOIN
    top_io_tables(sserver_id, start2_id, end2_id) rel2
    USING (datid, relid)
$$ LANGUAGE sql;
CREATE FUNCTION get_report_context(IN sserver_id integer, IN start1_id integer, IN end1_id integer,
  IN description text = NULL,
  IN start2_id integer = NULL, IN end2_id integer = NULL)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  report_context  jsonb;
  r_result    RECORD;

  qlen_limit  integer;
  topn        integer;

  start1_time text;
  end1_time   text;
  start2_time text;
  end2_time   text;
BEGIN
    ASSERT num_nulls(start1_id, end1_id) = 0, 'At least first interval bounds is necessary';

    -- Getting query length limit setting
    BEGIN
        qlen_limit := current_setting('pg_profile.max_query_length')::integer;
    EXCEPTION
        WHEN OTHERS THEN qlen_limit := 20000;
    END;

    -- Getting TopN setting
    BEGIN
        topn := current_setting('pg_profile.topn')::integer;
    EXCEPTION
        WHEN OTHERS THEN topn := 20;
    END;

    -- Populate report settings
    -- Check if all samples of requested interval are available
    IF (
      SELECT count(*) != end1_id - start1_id + 1 FROM samples
      WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
    ) THEN
      RAISE 'Not enough samples between %',
        format('%s AND %s', start1_id, end1_id);
    END IF;

    -- Get report times
    SELECT sample_time::text INTO STRICT start1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,start1_id);
    SELECT sample_time::text INTO STRICT end1_time FROM samples
    WHERE (server_id,sample_id) = (sserver_id,end1_id);

    IF num_nulls(start2_id, end2_id) = 2 THEN
      report_context := jsonb_build_object(
      'report_features',jsonb_build_object(
        'dbstats_reset', profile_checkavail_dbstats_reset(sserver_id, start1_id, end1_id),
        'stmt_cnt_range', profile_checkavail_stmt_cnt(sserver_id, start1_id, end1_id),
        'stmt_cnt_all', profile_checkavail_stmt_cnt(sserver_id, 0, 0),
        'cluster_stats_reset', profile_checkavail_cluster_stats_reset(sserver_id, start1_id, end1_id),
        'wal_stats_reset', profile_checkavail_wal_stats_reset(sserver_id, start1_id, end1_id),
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id),
        'statements_top_temp', profile_checkavail_top_temp(sserver_id, start1_id, end1_id),
        'statements_temp_io_times', profile_checkavail_statements_temp_io_times(sserver_id, start1_id, end1_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id,start1_id,end1_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id,start1_id,end1_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id),
        'top_tables_dead', profile_checkavail_tbl_top_dead(sserver_id,start1_id,end1_id),
        'top_tables_mods', profile_checkavail_tbl_top_mods(sserver_id,start1_id,end1_id),
        'table_new_page_updates', (
          SELECT COALESCE(sum(n_tup_newpage_upd), 0) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
        ),
        'stat_io', (
          SELECT COUNT(*) > 0 FROM (
            SELECT backend_type
            FROM sample_stat_io
            WHERE server_id = sserver_id AND
              sample_id BETWEEN start1_id + 1 AND end1_id LIMIT 1
            ) c
        ),
        'stat_io_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_io
            WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
            GROUP BY backend_type, object, context
          ) gr
        ),
        'stat_slru', (
          SELECT COUNT(*) > 0 FROM (
            SELECT name
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND
              sample_id BETWEEN start1_id + 1 AND end1_id LIMIT 1
            ) c
        ),
        'stat_slru_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND sample_id BETWEEN start1_id AND end1_id
            GROUP BY name
          ) gr
        ),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND sample_id BETWEEN start1_id + 1 AND end1_id
          ), false)
        ),
      'report_properties',jsonb_build_object(
        'interval_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
        'topn', topn,
        'max_query_length', qlen_limit,
        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time
        )
      );
    ELSIF num_nulls(start2_id, end2_id) = 0 THEN
      -- Get report times
      SELECT sample_time::text INTO STRICT start2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,start2_id);
      SELECT sample_time::text INTO STRICT end2_time FROM samples
      WHERE (server_id,sample_id) = (sserver_id,end2_id);
      -- Check if all samples of requested interval are available
      IF (
        SELECT count(*) != end2_id - start2_id + 1 FROM samples
        WHERE server_id = sserver_id AND sample_id BETWEEN start2_id AND end2_id
      ) THEN
        RAISE 'Not enough samples between %',
          format('%s AND %s', start2_id, end2_id);
      END IF;
      report_context := jsonb_build_object(
      'report_features',jsonb_build_object(
        'dbstats_reset', profile_checkavail_dbstats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_dbstats_reset(sserver_id, start2_id, end2_id),
        'stmt_cnt_range', profile_checkavail_stmt_cnt(sserver_id, start1_id, end1_id) OR
          profile_checkavail_stmt_cnt(sserver_id, start2_id, end2_id),
        'stmt_cnt_all', profile_checkavail_stmt_cnt(sserver_id, 0, 0),
        'cluster_stats_reset', profile_checkavail_cluster_stats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_cluster_stats_reset(sserver_id, start2_id, end2_id),
        'wal_stats_reset', profile_checkavail_wal_stats_reset(sserver_id, start1_id, end1_id) OR
          profile_checkavail_wal_stats_reset(sserver_id, start2_id, end2_id),
        'statstatements',profile_checkavail_statstatements(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statstatements(sserver_id, start2_id, end2_id),
        'planning_times',profile_checkavail_planning_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_planning_times(sserver_id, start2_id, end2_id),
        'wait_sampling_tot',profile_checkavail_wait_sampling_total(sserver_id, start1_id, end1_id) OR
          profile_checkavail_wait_sampling_total(sserver_id, start2_id, end2_id),
        'io_times',profile_checkavail_io_times(sserver_id, start1_id, end1_id) OR
          profile_checkavail_io_times(sserver_id, start2_id, end2_id),
        'statement_wal_bytes',profile_checkavail_stmt_wal_bytes(sserver_id, start1_id, end1_id) OR
          profile_checkavail_stmt_wal_bytes(sserver_id, start2_id, end2_id),
        'statements_top_temp', profile_checkavail_top_temp(sserver_id, start1_id, end1_id) OR
            profile_checkavail_top_temp(sserver_id, start2_id, end2_id),
        'statements_temp_io_times', profile_checkavail_statements_temp_io_times(sserver_id, start1_id, end1_id) OR
            profile_checkavail_statements_temp_io_times(sserver_id, start2_id, end2_id),
        'wal_stats',profile_checkavail_walstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_walstats(sserver_id, start2_id, end2_id),
        'sess_stats',profile_checkavail_sessionstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_sessionstats(sserver_id, start2_id, end2_id),
        'function_stats',profile_checkavail_functions(sserver_id, start1_id, end1_id) OR
          profile_checkavail_functions(sserver_id, start2_id, end2_id),
        'trigger_function_stats',profile_checkavail_trg_functions(sserver_id, start1_id, end1_id) OR
          profile_checkavail_trg_functions(sserver_id, start2_id, end2_id),
        'kcachestatements',profile_checkavail_rusage(sserver_id, start1_id, end1_id) OR
          profile_checkavail_rusage(sserver_id, start2_id, end2_id),
        'rusage_planstats',profile_checkavail_rusage_planstats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_rusage_planstats(sserver_id, start2_id, end2_id),
        'statements_jit_stats',profile_checkavail_statements_jit_stats(sserver_id, start1_id, end1_id) OR
          profile_checkavail_statements_jit_stats(sserver_id, start2_id, end2_id),
        'table_new_page_updates', (
          SELECT COALESCE(sum(n_tup_newpage_upd), 0) > 0
          FROM sample_stat_tables_total
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
        ),
        'stat_io', (
          SELECT COUNT(*) > 0 FROM (
            SELECT backend_type
            FROM sample_stat_io
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id + 1 AND end1_id OR
              sample_id BETWEEN start2_id + 1 AND end2_id
              ) LIMIT 1
            ) c
        ),
        'stat_io_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_io
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
              )
            GROUP BY backend_type, object, context
          ) gr
        ),
        'stat_slru', (
          SELECT COUNT(*) > 0 FROM (
            SELECT name
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id + 1 AND end1_id OR
              sample_id BETWEEN start2_id + 1 AND end2_id
              ) LIMIT 1
            ) c
        ),
        'stat_slru_reset', (
          -- We should include both ends here to detect resets performed
          SELECT bool_or(group_reset)
          FROM (
            SELECT COUNT(DISTINCT stats_reset) > 1 AS group_reset
            FROM sample_stat_slru
            WHERE server_id = sserver_id AND (
              sample_id BETWEEN start1_id AND end1_id OR
              sample_id BETWEEN start2_id AND end2_id
            )
            GROUP BY name
          ) gr
        ),
        'checksum_fail_detected', COALESCE((
          SELECT sum(checksum_failures) > 0
          FROM sample_stat_database
          WHERE server_id = sserver_id AND
            (sample_id BETWEEN start1_id + 1 AND end1_id OR
            sample_id BETWEEN start2_id + 1 AND end2_id)
          ), false)
        ),
      'report_properties',jsonb_build_object(
        'interval1_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end1_id and s.sample_id=start1_id
            AND server_id = sserver_id),
        'interval2_duration_sec',
          (SELECT extract(epoch FROM e.sample_time - s.sample_time)
          FROM samples s JOIN samples e USING (server_id)
          WHERE e.sample_id=end2_id and s.sample_id=start2_id
            AND server_id = sserver_id),

        'topn', topn,
        'max_query_length', qlen_limit,

        'start1_id', start1_id,
        'end1_id', end1_id,
        'report_start1', start1_time,
        'report_end1', end1_time,

        'start2_id', start2_id,
        'end2_id', end2_id,
        'report_start2', start2_time,
        'report_end2', end2_time
        )
      );
    ELSE
      RAISE 'Two bounds must be specified for second interval';
    END IF;

    -- Server name and description
    SELECT server_name, server_description INTO STRICT r_result
    FROM servers WHERE server_id = sserver_id;
    report_context := jsonb_set(report_context, '{report_properties,server_name}',
      to_jsonb(r_result.server_name)
    );
    IF r_result.server_description IS NOT NULL AND r_result.server_description != ''
    THEN
      report_context := jsonb_set(report_context, '{report_properties,server_description}',
        to_jsonb(format(
          '<p>%s</p>',
          r_result.server_description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,server_description}',to_jsonb(''::text));
    END IF;
    -- Report description
    IF description IS NOT NULL AND description != '' THEN
      report_context := jsonb_set(report_context, '{report_properties,description}',
        to_jsonb(format(
          '<h2>Report description</h2><p>%s</p>',
          description
        ))
      );
    ELSE
      report_context := jsonb_set(report_context, '{report_properties,description}',to_jsonb(''::text));
    END IF;
    -- Version substitution
    IF (SELECT count(*) = 1 FROM pg_catalog.pg_extension WHERE extname = 'pg_profile') THEN
      SELECT extversion INTO STRICT r_result FROM pg_catalog.pg_extension WHERE extname = 'pg_profile';
      report_context := jsonb_set(report_context, '{report_properties,pgprofile_version}',
        to_jsonb(r_result.extversion)
      );
    END IF;
  RETURN report_context;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION get_report_datasets(IN report_context jsonb, IN sserver_id integer)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
  start1_id   integer = (report_context #>> '{report_properties,start1_id}')::integer;
  start2_id   integer = (report_context #>> '{report_properties,start2_id}')::integer;
  end1_id     integer = (report_context #>> '{report_properties,end1_id}')::integer;
  end2_id     integer = (report_context #>> '{report_properties,end2_id}')::integer;

  datasets    jsonb = '{}';
  dataset     jsonb;
  queries_set jsonb = '[]';
  r_result    RECORD;
  r_dataset   text;
BEGIN
  IF num_nulls(start1_id, end1_id) = 0 AND num_nulls(start2_id, end2_id) > 0 THEN
    -- Regular report
    -- database statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM dbstats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{dbstat}', dataset);

    IF (report_context #> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM dbstats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{dbstats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,stat_io}')::boolean THEN
      -- stat_io dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stat_io_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stat_io}', dataset);
      IF (report_context #> '{report_features,stat_io_reset}')::boolean THEN
        -- IO reset dataset
        dataset := '[]'::jsonb;
        FOR r_result IN (
            SELECT *
            FROM cluster_stat_io_reset_format(sserver_id, start1_id, end1_id)
          ) LOOP
          dataset := dataset || to_jsonb(r_result);
        END LOOP;
        datasets := jsonb_set(datasets, '{stat_io_reset}', dataset);
      END IF;
    END IF;

    IF (report_context #> '{report_features,stat_slru}')::boolean THEN
      -- stat_slru dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stat_slru_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stat_slru}', dataset);
      IF (report_context #> '{report_features,stat_slru_reset}')::boolean THEN
        -- SLRU reset dataset
        dataset := '[]'::jsonb;
        FOR r_result IN (
            SELECT *
            FROM cluster_stat_slru_reset_format(sserver_id, start1_id, end1_id)
          ) LOOP
          dataset := dataset || to_jsonb(r_result);
        END LOOP;
        datasets := jsonb_set(datasets, '{stat_slru_reset}', dataset);
      END IF;
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM statements_dbstats_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{statements_dbstats}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_range}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, 0, 0)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_all}', dataset);
    END IF;

    -- cluster statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM cluster_stats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{cluster_stats}', dataset);

    IF (report_context #> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{cluster_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_reset_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_format(sserver_id, start1_id, end1_id,
            (report_context #>> '{report_properties,interval_duration_sec}')::numeric)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats}', dataset);
    END IF;

    -- Tablespace stats dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM tablespace_stats_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{tablespace_stats}', dataset);

    IF (report_context #> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wait_sampling_total_stats_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_total_stats}', dataset);
      -- Wait events dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_wait_sampling_events_format(sserver_id, start1_id, end1_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_events}', dataset);
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_statements_format(sserver_id, start1_id, end1_id)
          WHERE least(
              ord_total_time,
              ord_plan_time,
              ord_exec_time,
              ord_calls,
              ord_io_time,
              ord_shared_blocks_fetched,
              ord_shared_blocks_read,
              ord_shared_blocks_dirt,
              ord_shared_blocks_written,
              ord_wal,
              ord_temp,
              ord_jit
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{top_statements}', dataset);
    END IF;

    IF (report_context #> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_rusage_statements_format(sserver_id, start1_id, end1_id)
          WHERE least(
              ord_cpu_time,
              ord_io_bytes
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{top_rusage_statements}', dataset);
    END IF;

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tables_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_dml,
          ord_seq_scan,
          ord_upd,
          ord_growth,
          ord_vac,
          ord_anl
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_tables_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_indexes_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_indexes_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_growth,
          ord_unused,
          ord_vac
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_functions_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_time,
          ord_calls,
          ord_trgtime
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_functions}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tbl_last_sample_format(sserver_id, start1_id, end1_id)
        WHERE least(
          ord_dead,
          ord_mod
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tbl_last_sample}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM settings_format(sserver_id, start1_id, end1_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{settings}', dataset);

    -- Now we need to collect queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['queryid'];
      FOR r_result IN (
        SELECT
          userid,
          datid,
          queryid
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              userid  oid,
              datid   oid,
              queryid bigint
            )
        )
      LOOP
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP; -- over dataset entries
    END LOOP; -- over datasets

    -- Query texts dataset should be formed the last
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM report_queries_format(report_context, sserver_id, queries_set, start1_id, end1_id, NULL, NULL)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{queries}', dataset);
  ELSIF num_nulls(start1_id, end1_id, start2_id, end2_id) = 0 THEN
    -- Differential report
    -- database statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM dbstats_format_diff(sserver_id, start1_id, end1_id,
          start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{dbstat}', dataset);

    IF (report_context #> '{report_features,dbstats_reset}')::boolean THEN
      -- dbstats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM dbstats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{dbstats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,stat_io}')::boolean THEN
      -- stat_io dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stat_io_format(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stat_io}', dataset);
      IF (report_context #> '{report_features,stat_io_reset}')::boolean THEN
        -- SLRU reset dataset
        dataset := '[]'::jsonb;
        FOR r_result IN (
            SELECT *
            FROM cluster_stat_io_reset_format(sserver_id,
              start1_id, end1_id, start2_id, end2_id)
          ) LOOP
          dataset := dataset || to_jsonb(r_result);
        END LOOP;
        datasets := jsonb_set(datasets, '{stat_io_reset}', dataset);
      END IF;
    END IF;

    IF (report_context #> '{report_features,stat_slru}')::boolean THEN
      -- stat_slru dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stat_slru_format(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stat_slru}', dataset);
      IF (report_context #> '{report_features,stat_slru_reset}')::boolean THEN
        -- SLRU reset dataset
        dataset := '[]'::jsonb;
        FOR r_result IN (
            SELECT *
            FROM cluster_stat_slru_reset_format(sserver_id,
              start1_id, end1_id, start2_id, end2_id)
          ) LOOP
          dataset := dataset || to_jsonb(r_result);
        END LOOP;
        datasets := jsonb_set(datasets, '{stat_slru_reset}', dataset);
      END IF;
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- statements by database dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM statements_dbstats_format_diff(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{statements_dbstats}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_range}')::boolean THEN
      -- statements count of max for interval
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_range}', dataset);
    END IF;

    IF (report_context #> '{report_features,stmt_cnt_all}')::boolean THEN
      -- statements count of max for all samples
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM stmt_cnt_format(sserver_id, 0, 0)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{stmt_cnt_all}', dataset);
    END IF;

    -- cluster statistics dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM cluster_stats_format_diff(sserver_id, start1_id, end1_id,
                 start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{cluster_stats}', dataset);

    IF (report_context #> '{report_features,cluster_stats_reset}')::boolean THEN
      -- cluster stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM cluster_stats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{cluster_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats_reset}')::boolean THEN
      -- WAL stats reset dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_reset_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats_reset}', dataset);
    END IF;

    IF (report_context #> '{report_features,wal_stats}')::boolean THEN
      -- WAL stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wal_stats_format_diff(sserver_id,
            start1_id, end1_id, start2_id, end2_id,
            (report_context #>> '{report_properties,interval1_duration_sec}')::numeric,
            (report_context #>> '{report_properties,interval2_duration_sec}')::numeric)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wal_stats}', dataset);
    END IF;

    -- Tablespace stats dataset
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM tablespace_stats_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{tablespace_stats}', dataset);

    IF (report_context #> '{report_features,wait_sampling_tot}')::boolean THEN
      -- Wait totals dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM wait_sampling_total_stats_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_total_stats}', dataset);
      -- Wait events dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_wait_sampling_events_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{wait_sampling_events}', dataset);
    END IF;

    IF (report_context #> '{report_features,statstatements}')::boolean THEN
      -- Statement stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_statements_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
          WHERE least(
              ord_total_time,
              ord_plan_time,
              ord_exec_time,
              ord_calls,
              ord_io_time,
              ord_shared_blocks_fetched,
              ord_shared_blocks_read,
              ord_shared_blocks_dirt,
              ord_shared_blocks_written,
              ord_wal,
              ord_temp,
              ord_jit
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{top_statements}', dataset);
    END IF;

    IF (report_context #> '{report_features,kcachestatements}')::boolean THEN
      -- Statement rusage stats dataset
      dataset := '[]'::jsonb;
      FOR r_result IN (
          SELECT *
          FROM top_rusage_statements_format_diff(sserver_id, start1_id, end1_id,
            start2_id, end2_id)
          WHERE least(
              ord_cpu_time,
              ord_io_bytes
            ) <= (report_context #>> '{report_properties,topn}')::numeric
        ) LOOP
        dataset := dataset || to_jsonb(r_result);
      END LOOP;
      datasets := jsonb_set(datasets, '{top_rusage_statements}', dataset);
    END IF;

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_dml,
          ord_seq_scan,
          ord_upd,
          ord_growth,
          ord_vac,
          ord_anl
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_tables_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_tables}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_io_indexes_format_diff(sserver_id, start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_read,
          ord_fetch
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_io_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_indexes_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_growth,
          ord_vac
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_indexes}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM top_functions_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
        WHERE least(
          ord_time,
          ord_calls,
          ord_trgtime
        ) <= (report_context #>> '{report_properties,topn}')::numeric
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{top_functions}', dataset);

    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM settings_format_diff(sserver_id,
          start1_id, end1_id, start2_id, end2_id)
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{settings}', dataset);

    -- Now we need to collect queries over datasets
    FOR r_dataset IN (SELECT jsonb_object_keys(datasets)) LOOP
      -- skip datasets without queries
      CONTINUE WHEN NOT
        (datasets #> ARRAY[r_dataset, '0']) ?| ARRAY['queryid'];
      FOR r_result IN (
        SELECT
          userid,
          datid,
          queryid
        FROM
          jsonb_to_recordset(datasets #> ARRAY[r_dataset]) AS
            entry(
              userid  oid,
              datid   oid,
              queryid bigint
            )
        )
      LOOP
        queries_set := queries_set || jsonb_build_object(
          'userid', r_result.userid,
          'datid', r_result.datid,
          'queryid', r_result.queryid
        );
      END LOOP; -- over dataset entries
    END LOOP; -- over datasets

    -- Query texts dataset should be formed the last
    dataset := '[]'::jsonb;
    FOR r_result IN (
        SELECT *
        FROM report_queries_format(report_context, sserver_id, queries_set,
          start1_id, end1_id, start2_id, end2_id
        )
      ) LOOP
      dataset := dataset || to_jsonb(r_result);
    END LOOP;
    datasets := jsonb_set(datasets, '{queries}', dataset);
  END IF;
  RETURN datasets;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION section_apply_conditions(IN js_hdr jsonb, IN report_context jsonb)
RETURNS jsonb
AS $$
DECLARE
  js_res          jsonb;
  traverse_path   text[];
  resulting_path  text[];
  entry_condition boolean;
BEGIN
  js_res := '[]';
  resulting_path := NULL;
  traverse_path := ARRAY['0'];
  WHILE array_length(traverse_path, 1) > 0 LOOP
    -- Calculate condition
    entry_condition := NOT js_hdr #> traverse_path ? 'condition' OR
      trim(js_hdr #> traverse_path ->> 'condition') = '' OR
        (left(js_hdr #> traverse_path ->> 'condition', 1) = '!' AND
          NOT jsonb_extract_path_text(
              report_context,
              'report_features',
              js_hdr #> traverse_path ->> 'condition'
            )::boolean) OR
        (left(js_hdr #> traverse_path ->> 'condition', 1) != '!' AND
          jsonb_extract_path_text(
            report_context,
            'report_features',
            js_hdr #> traverse_path ->> 'condition'
          )::boolean);

    IF jsonb_typeof(js_hdr #> traverse_path) = 'object' AND entry_condition
    THEN
      -- Return found entry
      ASSERT
        array_length(traverse_path, 1) - COALESCE(array_length(resulting_path, 1), 0) <= 2,
        format('Impossible path length increment during traverse at %s', traverse_path);
      IF COALESCE(array_length(resulting_path, 1), 0) < array_length(traverse_path, 1) THEN
        -- Append 0 value of next level
        -- Special case on top level
        IF resulting_path IS NULL THEN
           resulting_path := ARRAY['0'];
        ELSE
          resulting_path := array_cat(resulting_path,
            ARRAY[traverse_path[array_length(traverse_path, 1) - 1], '0']);
        END IF;
      ELSIF array_length(resulting_path, 1) > array_length(traverse_path, 1) THEN
        -- trim array
        resulting_path := resulting_path[:array_length(traverse_path, 1)];
        resulting_path[array_length(resulting_path, 1)] :=
          (resulting_path[array_length(resulting_path, 1)]::integer + 1)::text;
      ELSIF array_length(resulting_path, 1) = array_length(traverse_path, 1) THEN
        resulting_path[array_length(resulting_path, 1)] :=
          (resulting_path[array_length(resulting_path, 1)]::integer + 1)::text;
      END IF;
      IF array_length(resulting_path, 1) > 1 AND
        resulting_path[array_length(resulting_path, 1)] = '0'
      THEN
        js_res := jsonb_set(
          js_res,
          resulting_path[:array_length(resulting_path, 1) - 1],
          '[]'::jsonb
        );
      END IF;
      js_res := jsonb_set(js_res, resulting_path,
        js_hdr #> traverse_path #- '{columns}' #- '{rows}' #- '{condition}'
      );
    END IF;
    -- Search for next entry
    IF (js_hdr #> traverse_path ? 'columns' OR js_hdr #> traverse_path ? 'rows') AND
      entry_condition
    THEN
      -- Drill down if we have the way
      CASE
        WHEN js_hdr #> traverse_path ? 'columns' THEN
          traverse_path := traverse_path || ARRAY['columns','0'];
        WHEN js_hdr #> traverse_path ? 'rows' THEN
          traverse_path := traverse_path || ARRAY['rows','0'];
        ELSE
          RAISE EXCEPTION 'Missing rows or columns array';
     END CASE;
     ASSERT js_hdr #> traverse_path IS NOT NULL, 'Empty columns list';
    ELSE
      CASE jsonb_typeof(js_hdr #> traverse_path)
        WHEN 'object' THEN
          -- If we are observing an object (i.e. column or row), search next
          IF jsonb_array_length(js_hdr #> traverse_path[:array_length(traverse_path, 1) - 1]) - 1 >
            traverse_path[array_length(traverse_path, 1)]::integer
          THEN
            -- Find sibling if exists
            traverse_path := array_cat(traverse_path[:array_length(traverse_path, 1) - 1],
              ARRAY[(traverse_path[array_length(traverse_path, 1)]::integer + 1)::text]
            );
          ELSE
            -- Or exit on previous array level if there is no siblings
            traverse_path := traverse_path[:array_length(traverse_path, 1) - 1];
          END IF;
        WHEN 'array' THEN
          -- Special case - switch from processing columns to processing rows
          IF array_length(traverse_path, 1) = 2 AND
            traverse_path[array_length(traverse_path, 1)] = 'columns' AND
            js_hdr #> traverse_path[:1] ? 'rows' AND
            jsonb_typeof(js_hdr #> array_cat(traverse_path[:1], ARRAY['rows'])) = 'array' AND
            jsonb_array_length(js_hdr #> array_cat(traverse_path[:1], ARRAY['rows'])) > 0
          THEN
            traverse_path := array_cat(traverse_path[:1], ARRAY['rows', '0']);
            resulting_path := resulting_path[:1];
            CONTINUE;
          END IF;
          -- If we are observing an array, we are searching the next sibling in preevious level
          -- we should check if there are elements left in previous array
          IF jsonb_array_length(js_hdr #> traverse_path[:array_length(traverse_path, 1) - 2]) - 1 >
            traverse_path[array_length(traverse_path, 1) - 1]::integer
          THEN
            -- take the next item on previous level if exists
            traverse_path := array_cat(traverse_path[:array_length(traverse_path, 1) - 2],
              ARRAY[(traverse_path[array_length(traverse_path, 1) - 1]::integer + 1)::text]
            );
          ELSE
            -- Or go one level up if not
            traverse_path := traverse_path[:array_length(traverse_path, 1) - 2];
          END IF;
      END CASE;
    END IF;
  END LOOP;
  RETURN js_res;
END;
$$ LANGUAGE plpgsql;
CREATE FUNCTION sections_jsonb(IN report_context jsonb, IN sserver_id integer,
  IN report_id integer)
RETURNS jsonb SET search_path=@extschema@ AS $$
DECLARE
    -- Recursive sections query with condition checking
    c_sections CURSOR(init_depth integer) FOR
    WITH RECURSIVE sections_tree(report_id, sect_id, parent_sect_id,
      toc_cap, tbl_cap, function_name, href, content, sect_struct, depth,
      path, ordering_path) AS
    (
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.function_name,
          rs.href,
          rs.content,
          rs.sect_struct,
          init_depth,
          ARRAY['sections', (row_number() OVER (ORDER BY s_ord ASC) - 1)::text] path,
          ARRAY[row_number() OVER (ORDER BY s_ord ASC)] as ordering_path
        FROM report_struct rs
        WHERE rs.report_id = sections_jsonb.report_id AND parent_sect_id IS NULL
          AND (
            rs.feature IS NULL OR
            (left(rs.feature,1) = '!' AND NOT jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean) OR
            (left(rs.feature,1) != '!' AND jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean)
          )
      UNION ALL
        SELECT
          rs.report_id,
          rs.sect_id,
          rs.parent_sect_id,
          rs.toc_cap,
          rs.tbl_cap,
          rs.function_name,
          rs.href,
          rs.content,
          rs.sect_struct,
          st.depth + 1,
          st.path || ARRAY['sections', (row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC) - 1)::text] path,
          ordering_path || ARRAY[row_number() OVER (PARTITION BY st.path ORDER BY s_ord ASC)] as ordering_path
        FROM report_struct rs JOIN sections_tree st ON
          (rs.report_id, rs.parent_sect_id) =
          (st.report_id, st.sect_id)
        WHERE (
            rs.feature IS NULL OR
            (left(rs.feature,1) = '!' AND NOT jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean) OR
            (left(rs.feature,1) != '!' AND jsonb_extract_path_text(report_context, 'report_features', rs.feature)::boolean)
          )
    )
    SELECT * FROM sections_tree ORDER BY ordering_path;

    c_new_queryids CURSOR(js_collected jsonb, js_new jsonb) FOR
    SELECT
      userid,
      datid,
      queryid
    FROM
      jsonb_array_elements(js_new) js_data_block,
      jsonb_to_recordset(js_data_block) AS (
        userid   bigint,
        datid    bigint,
        queryid  bigint
      )
    WHERE queryid IS NOT NULL AND datid IS NOT NULL
    EXCEPT
    SELECT
      userid,
      datid,
      queryid
    FROM
      jsonb_to_recordset(js_collected) AS (
        userid   bigint,
        datid    bigint,
        queryid  bigint
      );

    max_depth   CONSTANT integer := 5;

    js_fhdr     jsonb;
    js_fdata    jsonb;
    js_report   jsonb;

    js_queryids jsonb = '[]'::jsonb;
BEGIN
    js_report := jsonb_build_object(
      'type', report_id,
      'properties', report_context #> '{report_properties}'
    );

    -- Prepare report_context queryid array
    report_context := jsonb_insert(
      report_context,
      '{report_properties,queryids}',
      '[]'::jsonb
    );

    <<sections>>
    FOR r_result IN c_sections(1) LOOP

      js_fhdr := NULL;
      js_fdata := NULL;

      ASSERT r_result.depth BETWEEN 1 AND max_depth,
        format('Section depth is not in 1 - %s', max_depth);

      ASSERT js_report IS NOT NULL, format('Report JSON lost at start of section: %s', r_result.sect_id);
      -- Create "sections" array on the current level on first entry
      IF r_result.path[array_length(r_result.path, 1)] = '0' THEN
        js_report := jsonb_set(js_report, r_result.path[:array_length(r_result.path,1)-1],
          '[]'::jsonb
        );
      END IF;
      -- Section entry
      js_report := jsonb_insert(js_report, r_result.path, '{}'::jsonb);

      -- Set section attributes
      IF r_result.href IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'href'), to_jsonb(r_result.href));
      END IF;
      IF r_result.sect_id IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'sect_id'), to_jsonb(r_result.sect_id));
      END IF;
      IF r_result.tbl_cap IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'tbl_cap'), to_jsonb(r_result.tbl_cap));
      END IF;
      IF r_result.toc_cap IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'toc_cap'), to_jsonb(r_result.toc_cap));
      END IF;
      IF r_result.content IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'content'), to_jsonb(r_result.content));
      END IF;

      ASSERT js_report IS NOT NULL, format('Report JSON lost in attributes, section: %s', r_result.sect_id);

      -- Executing function of report section if requested
      -- It has priority over static section structure
      IF r_result.function_name IS NOT NULL THEN
        IF (SELECT count(*) FROM pg_catalog.pg_extension WHERE extname = 'pg_profile') THEN
          -- Fail when requested function doesn't exists in extension
          IF (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f JOIN pg_catalog.pg_depend dep
                ON (f.oid,'e') = (dep.objid, dep.deptype)
              JOIN pg_catalog.pg_extension ext
                ON (ext.oid = dep.refobjid)
            WHERE
              f.proname = r_result.function_name
              AND ext.extname = 'pg_profile'
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            )
          THEN
            RAISE EXCEPTION 'Report requested function % not found', r_result.function_name
              USING HINT = 'This is a bug. Please report to pg_profile developers.';
          END IF;
        ELSE
          -- When not installed as an extension check only the function existance
          IF (
            SELECT count(*) = 1
            FROM
              pg_catalog.pg_proc f
            WHERE
              f.proname = r_result.function_name
              AND pg_catalog.pg_get_function_result(f.oid) =
                'text'
              AND pg_catalog.pg_get_function_arguments(f.oid) =
                'report_context jsonb, sserver_id integer'
            )
          THEN
            RAISE EXCEPTION 'Report requested function % not found', r_result.function_name
              USING HINT = 'This is a bug. Please report to pg_profile developers.';
          END IF;
        END IF;

        -- Set report_context
        IF r_result.href IS NOT NULL THEN
          report_context := jsonb_set(report_context, '{report_properties,href}',
            to_jsonb(r_result.href));
        END IF;

        ASSERT report_context IS NOT NULL, 'Lost report context';
        -- Execute function for a report and get a section
        EXECUTE format('SELECT section_structure, section_data FROM %I($1,$2)',
          r_result.function_name)
        INTO js_fhdr, js_fdata
        USING
          report_context,
          sserver_id
        ;

        IF js_fdata IS NOT NULL AND jsonb_array_length(js_fdata) > 0 THEN
          -- Collect queryids from section data
          FOR r_queryid IN c_new_queryids(
            report_context #> '{report_properties,queryids}',
            js_fdata
          ) LOOP
            report_context := jsonb_insert(
              report_context,
              '{report_properties,queryids,0}',
              to_jsonb(r_queryid)
            );
          END LOOP;
          ASSERT report_context IS NOT NULL, 'Lost report context';
        END IF; -- function returned data

        IF jsonb_array_length(js_fhdr) > 0 THEN
          js_fhdr := section_apply_conditions(js_fhdr, report_context);
        END IF; -- Function returned header
      END IF;-- report section description contains a function

      -- Static section structure is used when there is no function defined
      -- or the function didn't return header
      IF r_result.sect_struct IS NOT NULL AND (js_fhdr IS NULL OR
        jsonb_array_length(js_fhdr) = 0)
      THEN
          js_fhdr := section_apply_conditions(r_result.sect_struct, report_context);
      END IF; -- static sect_struct condition

      IF js_fdata IS NOT NULL THEN
         js_report := jsonb_set(js_report, array_append(r_result.path, 'data'), js_fdata);
         ASSERT js_report IS NOT NULL, format('Report JSON lost in data, section: %s', r_result.sect_id);
      END IF;
      IF js_fhdr IS NOT NULL THEN
        js_report := jsonb_set(js_report, array_append(r_result.path, 'header'), js_fhdr);
        ASSERT js_report IS NOT NULL, format('Report JSON lost in header, section: %s', r_result.sect_id);
      END IF;
    END LOOP; -- Over recursive sections query
    RETURN js_report;
END;
$$ LANGUAGE plpgsql;
INSERT INTO import_queries_version_order VALUES
('pg_profile','4.3','pg_profile','4.2')
;
CREATE TABLE sample_stat_slru
(
    server_id     integer,
    sample_id     integer,
    name          text,
    blks_zeroed   bigint,
    blks_hit      bigint,
    blks_read     bigint,
    blks_written  bigint,
    blks_exists   bigint,
    flushes       bigint,
    truncates     bigint,
    stats_reset   timestamp with time zone,
    CONSTRAINT pk_sample_stat_slru PRIMARY KEY (server_id, sample_id, name),
    CONSTRAINT fk_sample_stat_slru_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_slru IS 'Sample SLRU statistics table (fields from pg_stat_slru)';

CREATE TABLE last_stat_slru (LIKE sample_stat_slru);
ALTER TABLE last_stat_slru ADD CONSTRAINT pk_last_stat_slru_samples
  PRIMARY KEY (server_id, sample_id, name);
ALTER TABLE last_stat_slru ADD CONSTRAINT fk_last_stat_slru_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_slru IS 'Last sample data for calculating diffs in next sample';

CREATE TABLE sample_stat_io
(
    server_id                   integer,
    sample_id                   integer,
    backend_type                text,
    object                      text,
    context                     text,
    reads                       bigint,
    read_time                   double precision,
    writes                      bigint,
    write_time                  double precision,
    writebacks                  bigint,
    writeback_time              double precision,
    extends                     bigint,
    extend_time                 double precision,
    op_bytes                    bigint,
    hits                        bigint,
    evictions                   bigint,
    reuses                      bigint,
    fsyncs                      bigint,
    fsync_time                  double precision,
    stats_reset                 timestamp with time zone,
    CONSTRAINT pk_sample_stat_io PRIMARY KEY (server_id, sample_id, backend_type, object, context),
    CONSTRAINT fk_sample_stat_io_samples FOREIGN KEY (server_id, sample_id)
      REFERENCES samples (server_id, sample_id) ON DELETE CASCADE
);
COMMENT ON TABLE sample_stat_io IS 'Sample IO statistics table (fields from pg_stat_io)';

CREATE TABLE last_stat_io (LIKE sample_stat_io);
ALTER TABLE last_stat_io ADD CONSTRAINT pk_last_stat_io_samples
  PRIMARY KEY (server_id, sample_id, backend_type, object, context);
ALTER TABLE last_stat_io ADD CONSTRAINT fk_last_stat_io_samples
  FOREIGN KEY (server_id, sample_id) REFERENCES samples(server_id, sample_id) ON DELETE RESTRICT;
COMMENT ON TABLE last_stat_io IS 'Last sample data for calculating diffs in next sample';

ALTER TABLE sample_stat_indexes
  ADD COLUMN last_idx_scan       timestamp with time zone;
ALTER TABLE last_stat_indexes
  ADD COLUMN last_idx_scan       timestamp with time zone;

ALTER TABLE sample_stat_tables
  ADD COLUMN last_seq_scan       timestamp with time zone,
  ADD COLUMN last_idx_scan       timestamp with time zone,
  ADD COLUMN n_tup_newpage_upd   bigint;

ALTER TABLE sample_stat_tables_total
  ADD COLUMN n_tup_newpage_upd   bigint;

ALTER TABLE last_stat_tables
  ADD COLUMN last_seq_scan       timestamp with time zone,
  ADD COLUMN last_idx_scan       timestamp with time zone,
  ADD COLUMN n_tup_newpage_upd   bigint;

DROP VIEW v_sample_stat_indexes;
CREATE VIEW v_sample_stat_indexes AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        indexrelid,
        tl.schemaname,
        tl.relname,
        il.indexrelname,
        idx_scan,
        idx_tup_read,
        idx_tup_fetch,
        idx_blks_read,
        idx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        indisunique,
        relpages_bytes,
        relpages_bytes_diff,
        last_idx_scan
    FROM
        sample_stat_indexes s
        JOIN indexes_list il USING (datid, indexrelid, server_id)
        JOIN tables_list tl USING (datid, relid, server_id);
COMMENT ON VIEW v_sample_stat_indexes IS 'Reconstructed stats view with table and index names and schemas';

DROP VIEW v_sample_stat_tables;
CREATE VIEW v_sample_stat_tables AS
    SELECT
        server_id,
        sample_id,
        datid,
        relid,
        tablespacename,
        schemaname,
        relname,
        seq_scan,
        seq_tup_read,
        idx_scan,
        idx_tup_fetch,
        n_tup_ins,
        n_tup_upd,
        n_tup_del,
        n_tup_hot_upd,
        n_live_tup,
        n_dead_tup,
        n_mod_since_analyze,
        n_ins_since_vacuum,
        last_vacuum,
        last_autovacuum,
        last_analyze,
        last_autoanalyze,
        vacuum_count,
        autovacuum_count,
        analyze_count,
        autoanalyze_count,
        heap_blks_read,
        heap_blks_hit,
        idx_blks_read,
        idx_blks_hit,
        toast_blks_read,
        toast_blks_hit,
        tidx_blks_read,
        tidx_blks_hit,
        relsize,
        relsize_diff,
        tablespaceid,
        reltoastrelid,
        relkind,
        relpages_bytes,
        relpages_bytes_diff,
        last_seq_scan,
        last_idx_scan,
        n_tup_newpage_upd
    FROM sample_stat_tables
      JOIN tables_list USING (server_id, datid, relid)
      JOIN tablespaces_list tl USING (server_id, tablespaceid);
COMMENT ON VIEW v_sample_stat_tables IS 'Tables stats view with table names and schemas';

DELETE FROM report_struct;
DELETE FROM report;
DELETE FROM report_static;

UPDATE import_queries
SET query = 'INSERT INTO last_stat_indexes (server_id,sample_id,datid,relid,indexrelid,'
    'schemaname,relname,indexrelname,idx_scan,idx_tup_read,idx_tup_fetch,'
    'idx_blks_read,idx_blks_hit,relsize,relsize_diff,tablespaceid,indisunique,'
    'in_sample,relpages_bytes,relpages_bytes_diff,last_idx_scan)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.indexrelid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.indexrelname, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.indisunique, '
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_idx_scan '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'relid          oid, '
        'indexrelid     oid, '
        'schemaname     name, '
        'relname        name, '
        'indexrelname   name, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'tablespaceid   oid, '
        'indisunique    boolean, '
        'in_sample      boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint, '
        'last_idx_scan  timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id '
      'AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'last_stat_indexes');

UPDATE import_queries
SET query = 'INSERT INTO last_stat_tables (server_id,sample_id,datid,relid,schemaname,relname,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,'
    'last_vacuum,last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,'
    'autovacuum_count,analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,'
    'idx_blks_read,idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,'
    'tidx_blks_hit,relsize,relsize_diff,tablespaceid,reltoastrelid,relkind,in_sample,'
    'relpages_bytes,relpages_bytes_diff,last_seq_scan,last_idx_scan,n_tup_newpage_upd)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.schemaname, '
    'dt.relname, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.n_live_tup, '
    'dt.n_dead_tup, '
    'dt.n_mod_since_analyze, '
    'dt.n_ins_since_vacuum, '
    'dt.last_vacuum, '
    'dt.last_autovacuum, '
    'dt.last_analyze, '
    'dt.last_autoanalyze, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.tablespaceid, '
    'dt.reltoastrelid, '
    'dt.relkind, '
    'COALESCE(dt.in_sample, false), '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_seq_scan, '
    'dt.last_idx_scan, '
    'dt.n_tup_newpage_upd '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'schemaname           name, '
        'relname              name, '
        'seq_scan             bigint, '
        'seq_tup_read         bigint, '
        'idx_scan             bigint, '
        'idx_tup_fetch        bigint, '
        'n_tup_ins            bigint, '
        'n_tup_upd            bigint, '
        'n_tup_del            bigint, '
        'n_tup_hot_upd        bigint, '
        'n_live_tup           bigint, '
        'n_dead_tup           bigint, '
        'n_mod_since_analyze  bigint, '
        'n_ins_since_vacuum   bigint, '
        'last_vacuum          timestamp with time zone, '
        'last_autovacuum      timestamp with time zone, '
        'last_analyze         timestamp with time zone, '
        'last_autoanalyze     timestamp with time zone, '
        'vacuum_count         bigint, '
        'autovacuum_count     bigint, '
        'analyze_count        bigint, '
        'autoanalyze_count    bigint, '
        'heap_blks_read       bigint, '
        'heap_blks_hit        bigint, '
        'idx_blks_read        bigint, '
        'idx_blks_hit         bigint, '
        'toast_blks_read      bigint, '
        'toast_blks_hit       bigint, '
        'tidx_blks_read       bigint, '
        'tidx_blks_hit        bigint, '
        'relsize              bigint, '
        'relsize_diff         bigint, '
        'tablespaceid         oid, '
        'reltoastrelid        oid, '
        'relkind              character(1), '
        'in_sample            boolean, '
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint, '
        'last_seq_scan        timestamp with time zone,'
        'last_idx_scan        timestamp with time zone,'
        'n_tup_newpage_upd    bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'last_stat_tables');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_indexes (server_id,sample_id,datid,indexrelid,tablespaceid,'
    'idx_scan,idx_tup_read,idx_tup_fetch,idx_blks_read,idx_blks_hit,relsize,'
    'relsize_diff,indisunique,relpages_bytes,relpages_bytes_diff,last_idx_scan)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.indexrelid, '
    'dt.tablespaceid, '
    'dt.idx_scan, '
    'dt.idx_tup_read, '
    'dt.idx_tup_fetch, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.indisunique, '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_idx_scan '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'datid          oid, '
        'indexrelid     oid, '
        'tablespaceid   oid, '
        'idx_scan       bigint, '
        'idx_tup_read   bigint, '
        'idx_tup_fetch  bigint, '
        'idx_blks_read  bigint, '
        'idx_blks_hit   bigint, '
        'relsize        bigint, '
        'relsize_diff   bigint, '
        'indisunique    boolean, '
        'relpages_bytes bigint, '
        'relpages_bytes_diff bigint, '
        'last_idx_scan  timestamp with time zone'
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_indexes ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.indexrelid = dt.indexrelid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_indexes');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_tables (server_id,sample_id,datid,relid,tablespaceid,seq_scan,'
    'seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,n_tup_hot_upd,'
    'n_live_tup,n_dead_tup,n_mod_since_analyze,n_ins_since_vacuum,last_vacuum,'
    'last_autovacuum,last_analyze,last_autoanalyze,vacuum_count,autovacuum_count,'
    'analyze_count,autoanalyze_count,heap_blks_read,heap_blks_hit,idx_blks_read,'
    'idx_blks_hit,toast_blks_read,toast_blks_hit,tidx_blks_read,tidx_blks_hit,'
    'relsize,relsize_diff,relpages_bytes,relpages_bytes_diff,last_seq_scan,'
    'last_idx_scan,n_tup_newpage_upd)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.relid, '
    'dt.tablespaceid, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.n_live_tup, '
    'dt.n_dead_tup, '
    'dt.n_mod_since_analyze, '
    'dt.n_ins_since_vacuum, '
    'dt.last_vacuum, '
    'dt.last_autovacuum, '
    'dt.last_analyze, '
    'dt.last_autoanalyze, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize, '
    'dt.relsize_diff, '
    'dt.relpages_bytes, '
    'dt.relpages_bytes_diff, '
    'dt.last_seq_scan, '
    'dt.last_idx_scan, '
    'dt.n_tup_newpage_upd '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id            integer, '
        'sample_id            integer, '
        'datid                oid, '
        'relid                oid, '
        'tablespaceid         oid, '
        'seq_scan             bigint, '
        'seq_tup_read         bigint, '
        'idx_scan             bigint, '
        'idx_tup_fetch        bigint, '
        'n_tup_ins            bigint, '
        'n_tup_upd            bigint, '
        'n_tup_del            bigint, '
        'n_tup_hot_upd        bigint, '
        'n_live_tup           bigint, '
        'n_dead_tup           bigint, '
        'n_mod_since_analyze  bigint, '
        'n_ins_since_vacuum   bigint, '
        'last_vacuum          timestamp with time zone, '
        'last_autovacuum      timestamp with time zone, '
        'last_analyze         timestamp with time zone, '
        'last_autoanalyze     timestamp with time zone, '
        'vacuum_count         bigint, '
        'autovacuum_count     bigint, '
        'analyze_count        bigint, '
        'autoanalyze_count    bigint, '
        'heap_blks_read       bigint, '
        'heap_blks_hit        bigint, '
        'idx_blks_read        bigint, '
        'idx_blks_hit         bigint, '
        'toast_blks_read      bigint, '
        'toast_blks_hit       bigint, '
        'tidx_blks_read       bigint, '
        'tidx_blks_hit        bigint, '
        'relsize              bigint, '
        'relsize_diff         bigint, '
        'relpages_bytes       bigint, '
        'relpages_bytes_diff  bigint, '
        'last_seq_scan        timestamp with time zone,'
        'last_idx_scan        timestamp with time zone,'
        'n_tup_newpage_upd    bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.relid = dt.relid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_tables');

UPDATE import_queries
SET query = 'INSERT INTO sample_stat_tables_total (server_id,sample_id,datid,tablespaceid,relkind,'
    'seq_scan,seq_tup_read,idx_scan,idx_tup_fetch,n_tup_ins,n_tup_upd,n_tup_del,'
    'n_tup_hot_upd,vacuum_count,autovacuum_count,analyze_count,autoanalyze_count,'
    'heap_blks_read,heap_blks_hit,idx_blks_read,idx_blks_hit,toast_blks_read,'
    'toast_blks_hit,tidx_blks_read,tidx_blks_hit,relsize_diff,n_tup_newpage_upd)'
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.datid, '
    'dt.tablespaceid, '
    'dt.relkind, '
    'dt.seq_scan, '
    'dt.seq_tup_read, '
    'dt.idx_scan, '
    'dt.idx_tup_fetch, '
    'dt.n_tup_ins, '
    'dt.n_tup_upd, '
    'dt.n_tup_del, '
    'dt.n_tup_hot_upd, '
    'dt.vacuum_count, '
    'dt.autovacuum_count, '
    'dt.analyze_count, '
    'dt.autoanalyze_count, '
    'dt.heap_blks_read, '
    'dt.heap_blks_hit, '
    'dt.idx_blks_read, '
    'dt.idx_blks_hit, '
    'dt.toast_blks_read, '
    'dt.toast_blks_hit, '
    'dt.tidx_blks_read, '
    'dt.tidx_blks_hit, '
    'dt.relsize_diff, '
    'dt.n_tup_newpage_upd '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id          integer, '
        'sample_id          integer, '
        'datid              oid, '
        'tablespaceid       oid, '
        'relkind            character(1), '
        'seq_scan           bigint, '
        'seq_tup_read       bigint, '
        'idx_scan           bigint, '
        'idx_tup_fetch      bigint, '
        'n_tup_ins          bigint, '
        'n_tup_upd          bigint, '
        'n_tup_del          bigint, '
        'n_tup_hot_upd      bigint, '
        'vacuum_count       bigint, '
        'autovacuum_count   bigint, '
        'analyze_count      bigint, '
        'autoanalyze_count  bigint, '
        'heap_blks_read     bigint, '
        'heap_blks_hit      bigint, '
        'idx_blks_read      bigint, '
        'idx_blks_hit       bigint, '
        'toast_blks_read    bigint, '
        'toast_blks_hit     bigint, '
        'tidx_blks_read     bigint, '
        'tidx_blks_hit      bigint, '
        'relsize_diff       bigint, '
        'n_tup_newpage_upd  bigint '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_tables_total ld ON '
      '(ld.server_id = srv_map.local_srv_id AND ld.sample_id = dt.sample_id AND ld.datid = dt.datid AND ld.tablespaceid = dt.tablespaceid) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
WHERE
  (extension, from_version, exec_order, relname) =
  ('pg_profile', '0.3.1', 1, 'sample_stat_tables_total');

INSERT INTO import_queries VALUES
('pg_profile','4.3', 1,'sample_stat_io',
  'INSERT INTO sample_stat_io (server_id,sample_id,backend_type,object,context,reads,'
    'read_time,writes,write_time,writebacks,writeback_time,extends,extend_time,'
    'op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.backend_type, '
    'dt.object, '
    'dt.context, '
    'dt.reads, '
    'dt.read_time, '
    'dt.writes, '
    'dt.write_time, '
    'dt.writebacks, '
    'dt.writeback_time, '
    'dt.extends, '
    'dt.extend_time, '
    'dt.op_bytes, '
    'dt.hits, '
    'dt.evictions, '
    'dt.reuses, '
    'dt.fsyncs, '
    'dt.fsync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id                   integer, '
        'sample_id                   integer, '
        'backend_type                text, '
        'object                      text, '
        'context                     text, '
        'reads                       bigint, '
        'read_time                   double precision, '
        'writes                      bigint, '
        'write_time                  double precision, '
        'writebacks                  bigint, '
        'writeback_time              double precision, '
        'extends                     bigint, '
        'extend_time                 double precision, '
        'op_bytes                    bigint, '
        'hits                        bigint, '
        'evictions                   bigint, '
        'reuses                      bigint, '
        'fsyncs                      bigint, '
        'fsync_time                  double precision, '
        'stats_reset                 timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_io ld ON '
      '(ld.server_id, ld.sample_id, ld.backend_type, ld.object, ld.context) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.backend_type, dt.object, dt.context) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'last_stat_io',
  'INSERT INTO last_stat_io (server_id,sample_id,backend_type,object,context,reads,'
    'read_time,writes,write_time,writebacks,writeback_time,extends,extend_time,'
    'op_bytes,hits,evictions,reuses,fsyncs,fsync_time,stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.backend_type, '
    'dt.object, '
    'dt.context, '
    'dt.reads, '
    'dt.read_time, '
    'dt.writes, '
    'dt.write_time, '
    'dt.writebacks, '
    'dt.writeback_time, '
    'dt.extends, '
    'dt.extend_time, '
    'dt.op_bytes, '
    'dt.hits, '
    'dt.evictions, '
    'dt.reuses, '
    'dt.fsyncs, '
    'dt.fsync_time, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id                   integer, '
        'sample_id                   integer, '
        'backend_type                text, '
        'object                      text, '
        'context                     text, '
        'reads                       bigint, '
        'read_time                   double precision, '
        'writes                      bigint, '
        'write_time                  double precision, '
        'writebacks                  bigint, '
        'writeback_time              double precision, '
        'extends                     bigint, '
        'extend_time                 double precision, '
        'op_bytes                    bigint, '
        'hits                        bigint, '
        'evictions                   bigint, '
        'reuses                      bigint, '
        'fsyncs                      bigint, '
        'fsync_time                  double precision, '
        'stats_reset                 timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_io ld ON '
      '(ld.server_id, ld.sample_id, ld.backend_type, ld.object, ld.context) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.backend_type, dt.object, dt.context) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'sample_stat_slru',
  'INSERT INTO sample_stat_slru (server_id,sample_id,name,blks_zeroed,'
    'blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,'
    'stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.name, '
    'dt.blks_zeroed, '
    'dt.blks_hit, '
    'dt.blks_read, '
    'dt.blks_written, '
    'dt.blks_exists, '
    'dt.flushes, '
    'dt.truncates, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'name           text, '
        'blks_zeroed    bigint, '
        'blks_hit       bigint, '
        'blks_read      bigint, '
        'blks_written   bigint, '
        'blks_exists    bigint, '
        'flushes        bigint, '
        'truncates      bigint, '
        'stats_reset    timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN sample_stat_slru ld ON '
      '(ld.server_id, ld.sample_id, ld.name) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.name) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
),
('pg_profile','4.3', 1,'last_stat_slru',
  'INSERT INTO last_stat_slru (server_id,sample_id,name,blks_zeroed,'
    'blks_hit,blks_read,blks_written,blks_exists,flushes,truncates,'
    'stats_reset'
    ') '
  'SELECT '
    'srv_map.local_srv_id, '
    'dt.sample_id, '
    'dt.name, '
    'dt.blks_zeroed, '
    'dt.blks_hit, '
    'dt.blks_read, '
    'dt.blks_written, '
    'dt.blks_exists, '
    'dt.flushes, '
    'dt.truncates, '
    'dt.stats_reset '
  'FROM %1$s imp '
    'CROSS JOIN json_to_record(imp.row_data) AS '
      'dt ( '
        'server_id      integer, '
        'sample_id      integer, '
        'name           text, '
        'blks_zeroed    bigint, '
        'blks_hit       bigint, '
        'blks_read      bigint, '
        'blks_written   bigint, '
        'blks_exists    bigint, '
        'flushes        bigint, '
        'truncates      bigint, '
        'stats_reset    timestamp with time zone '
      ') '
    'JOIN tmp_srv_map srv_map ON '
      '(srv_map.imp_srv_id = dt.server_id) '
    'LEFT OUTER JOIN last_stat_slru ld ON '
      '(ld.server_id, ld.sample_id, ld.name) = '
      '(srv_map.local_srv_id, dt.sample_id, dt.name) '
  'WHERE ld.server_id IS NULL AND imp.section_id = $1 '
)
;
/* === report_static table data === */
INSERT INTO report_static(static_name, static_text)
VALUES
('css', $css$
/* ----------- Common styles ----------- */

:root {
    --main-bg-color: #68B8F9;
    --main-font-color: black;
    --secondary-bg-color: #a4d4fb;
    --main-bottom: -20%;
    --main-right: 2%;
    --main-opacity: 1;
    --main-border-radius: 5px;
    --main-box-shadow: rgba(0, 0, 0, 0.35) 0 5px 15px;
    --main-position: fixed;
    --main-width: 500px;
    --main-height: 100px;
    --main-transition-property: top;
    --main-transition-delay: 500ms;
}
html {
    scroll-behavior:smooth;
}
a:hover {
    text-decoration: none;
}
#navigator {
    position: var(--main-position);
    background-color: transparent;
    border-radius: 4px;
    visibility: hidden;
    top: 2%;
    height: 92%;
    right: 0;
    overflow: auto;
    opacity: 0.9;
    transition: opacity 300ms linear, visibility 300ms linear, right 300ms linear;
    display: flex;
    flex-direction: row;
    flex-wrap: nowrap;
}
#navigator div {
    margin-top: 15px;
    height: 100px;
    background-color: royalblue;
    writing-mode: vertical-lr;
    font-size: 14px;
    letter-spacing: 2px;
    text-align: center;
    transition: width 300ms linear;
    color: white;
}
#navigator div:hover {
    cursor: pointer;
}
#navigator ul {
    background-color: white;
}
#navigator div.active {
    width: 14px;
}
#navigator div.active:hover {
    width: 20px;
}
#navigator div.hidden {
    width: 48px;
}
#navigator div.hidden:hover {
    width: 42px;
}
#navigator ul.active {
    box-shadow: var(--main-box-shadow);
    overflow: auto;
    display: block;
}
#navigator ul.hidden {
    display: none;
}
#navigator:hover {
    opacity: 1;
}
#navigator li.current {
    border: 2px solid black;
    border-radius: 3px;
}
#navigator li.active{
    background-color: var(--secondary-bg-color);
    border-radius: 3px;
}
#commonstat {
    display: flex;
    flex-wrap: wrap;
}
#commonstat div {
    margin-right: 10px;
}
table {
    margin-bottom: 10px;
}
table, th, td {
    border: 1px solid black;
    border-collapse: collapse;
    padding: 4px;
}
table tr td.table_obj_value, table tr td.mono {
    font-family: Monospace;
}
table tr td.table_obj_value {
    text-align: right;
}
table p {
    margin: 0.2em;
}

table.stat tr:nth-child(even), table.setlist tr:nth-child(even) {
    background-color: #eee;
}
table.setlist tr.new td.switch_bold {
    font-weight: bold;
}
table th {
    color: black;
    background-color: #ffcc99;
}
table tr:target, td:target {
    border: medium solid limegreen;
}
table tr:target td:first-of-type, table td:target {
    font-weight: bold;
}
table.stat tr.active td {
    background-color: #CCF1FF;
}
table.toast tr.active td {
    background-color: #CCF1FF;
}
table.stmtlist tr.active td {
    background-color: #CCF1FF;
}
table.diff tr.active td {
    background-color: #CCF1FF;
}
table.diff tr.active td:not(.hdr) {
    background-color: #CCF1FF;
}
div.popup {
    background-color: var(--main-bg-color);
    bottom: var(--main-bottom);
    color: var(--main-font-color);
    right: var(--main-right);
    opacity: var(--main-opacity);
    border-radius: var(--main-border-radius);
    box-shadow: var(--main-box-shadow);
    position: var(--main-position);
    max-width: var(--main-width);
    transition: bottom 250ms linear;
    z-index: 500 !important;
    padding: 10px 20px;
}
svg rect:hover, svg circle:hover, svg path:hover, a.copyButton svg:hover>rect{
    stroke:limegreen;
    cursor: pointer;
}
/* ----------- Differential report styles ----------- */

td.int1, .int1 td:not(.hdr), table.setlist tr.new_i1 {
    background-color: #FFEEEE;
}
td.int2, .int2 td:not(.hdr), table.setlist tr.new_i2 {
    background-color: #EEEEFF;
}
table.toast tr.int1:not(.active) td:not(.hdr) {
    background-color: #D8E8C2;
}
table.toast tr.int2:not(.active) td:not(.hdr) {
    background-color: #BBDD97;
}
table.diff tr.int2 td {
    border-top: hidden;
}
table.diff tr:nth-child(4n+1), table.toast tr:nth-child(4n+1){
    background-color: #eee;
}
table.stat tr:nth-child(even), table.setlist tr:nth-child(even):not(.new_i1):not(.new_i2) {
    background-color: #eee;
}
table.setlist tr.new_i1 td.switch_bold, table.setlist tr.new_i2 td.switch_bold, .new td.switch_bold {
    font-weight: bold;
}
table th {
    color: black; background-color: #ffcc99;
}
.label {
    color: grey;
}
table tr:target,td:target {
    border: medium solid limegreen;
}
table tr:target td:first-of-type, table td:target {
    font-weight: bold;
}
table tr.parent td {
    background-color: #D8E8C2;
}
table tr.child td {
    background-color: #BBDD97;
    border-top-style: hidden;
}
table.stat tr.active td {
      background-color: #CCF1FF;
}
{static:css_post}
$css$
),
('version',
  '<p>pg_profile version {properties:pgprofile_version}</p>'),
(
 'script_js', $js$
class Utilities {
    static sort(data, key, direction) {
        return structuredClone(data.sort((a, b) => {
            if (a[key] < b[key]) {
                return -1 * direction;
            } else if (a[key] > b[key]) {
                return direction;
            } else {
                return 0;
            }
        }))
    }
    static sum(data, key) {
        return data.reduce((partialSum, a) => partialSum + a[key], 0);
    }
    static filter(data, key) {
        if (key.type === "exists") {
            if (data.every(obj => key["field"] in obj)) {
                return structuredClone(data.filter(obj => obj[key["field"]]));
            }
        } else if (key.type === "equal") {
            if (data.every(obj => key["field"] in obj)) {
                return structuredClone(data.filter(obj => obj[key["field"]] === key["value"]));
            }
        }
        return data;
    }
    static find(data, key, value) {
        return structuredClone(data.filter(obj => obj[key] === value));
    }
    static limit(data, num) {
        if (num > 0) {
            return structuredClone(data.slice(0, num));
        }
        return data;
    }
}
class BaseChart {
    static drawIntoTable(cls, newRow, column, data) {
        let key = column.ordering[0] === '-' ? column.ordering.substring(1) : column.ordering;
        let value = column.id;
        let direction = column.ordering[0] === '-' ? -1 : 1;
        let newCell = newRow.insertCell(-1);
        if (Utilities.sum(data, value) > 0) {
            let orderedData = Utilities.sort(data, key, direction);
            let svg = cls.drawSVG(orderedData, value, key);
            newCell.appendChild(svg);
        }
    }
}
class PipeChart extends BaseChart {
    static drawSVG(orderedData, value, key) {
        let x = 0; // Start position of nested svg
        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '2em');
        orderedData.forEach(elem => {
            let width = Math.floor(elem[value]);
            let nestedSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
            nestedSvg.setAttribute('x', `${x}%`);
            nestedSvg.setAttribute('height', '2em');
            nestedSvg.setAttribute('width', `${width}%`);
            let title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
            title.innerHTML = `${elem.objname}: ${elem[value]}`;
            let text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
            text.setAttribute('y', '70%');
            text.setAttribute('x', '0.3em');
            text.innerHTML = `${elem.objname}: ${elem[key]}`;
            let rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
            rect.setAttribute('height', '90%');
            rect.setAttribute('x', '0%');
            rect.setAttribute('y', '10%');
            rect.setAttribute('ry', '15%');
            rect.setAttribute('stroke', 'black');
            rect.setAttribute('stroke-width', '1px');
            rect.setAttribute('width', '100%');
            rect.setAttribute('fill', `#${elem.objcolor}`);
            nestedSvg.appendChild(title);
            nestedSvg.appendChild(rect);
            svg.appendChild(nestedSvg);
            nestedSvg.appendChild(text);
            x += width;
        })
        return svg;
    }
    static drawIntoTable(newRow, column, data) {
        BaseChart.drawIntoTable(PipeChart, newRow, column, data);
        return true;
    }
}
class PieChart extends BaseChart {
    static drawPieSlice(startRad, diffRad, radius, center, elem, key) {
        let nestedSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        let title = document.createElementNS('http://www.w3.org/2000/svg', 'title');
        title.innerHTML = `${elem.objname}: ${elem[key]}`;
        if (diffRad >= Math.PI * 1.999) {
            let circle = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
            circle.setAttribute('cx', center[0]);
            circle.setAttribute('cy', center[1]);
            circle.setAttribute('r', radius);
            circle.setAttribute('fill', `#${elem.objcolor}`);
            circle.setAttribute('stroke', 'black');
            circle.setAttribute('stroke-width', '1px');
            nestedSvg.appendChild(title);
            nestedSvg.appendChild(circle);
            return nestedSvg;
        }
        const hoverOffset = 5;
        let startPointX = center[0] + radius * Math.cos(startRad);
        let startPointY = center[1] + radius * Math.sin(startRad);
        let startPoint = `M ${startPointX} ${startPointY}`;
        let arcFinishX = center[0] + radius * Math.cos(startRad + diffRad);// 150;
        let arcFinishY = center[1] + radius * Math.sin(startRad + diffRad); // 0
        let arcAngle = 0;
        let arcType = diffRad <= Math.PI ? 0 : 1;
        let arcClockwise = 1;
        let arc = `A ${radius} ${radius} ${arcAngle} ${arcType} ${arcClockwise} ${arcFinishX} ${arcFinishY}`;
        let lineOne = `L ${center[0]} ${center[1]}`;
        let lineTwo = `L ${startPointX} ${startPointY}`;
        let d = `${startPoint} ${arc} ${lineOne} ${lineTwo} Z`;
        let slice = document.createElementNS('http://www.w3.org/2000/svg', 'path');
        slice.setAttribute('d', d);
        slice.setAttribute('fill', `#${elem.objcolor}`);
        slice.setAttribute('stroke', 'black');
        slice.setAttribute('stroke-width', '1px');
        slice.addEventListener('mouseover', function (){
            let centerOne = center[0] + hoverOffset * Math.cos(startRad + diffRad / 2);
            let centerTwo = center[1] + hoverOffset * Math.sin(startRad + diffRad / 2);
            startPointX = centerOne + radius * Math.cos(startRad);
            startPointY = centerTwo + radius * Math.sin(startRad);
            startPoint = `M ${startPointX} ${startPointY}`;
            arcFinishX = centerOne + radius * Math.cos(startRad + diffRad);// 150;
            arcFinishY = centerTwo + radius * Math.sin(startRad + diffRad); // 0
            arc = `A ${radius} ${radius} ${arcAngle} ${arcType} ${arcClockwise} ${arcFinishX} ${arcFinishY}`;
            lineOne = `L ${centerOne} ${centerTwo}`;
            lineTwo = `L ${startPointX} ${startPointY}`;
            let dHover = `${startPoint} ${arc} ${lineOne} ${lineTwo} Z`;
            slice.setAttribute('d', dHover);
        })
        slice.addEventListener('mouseout', function () {
            slice.setAttribute('d', d);
        })
        nestedSvg.appendChild(title);
        nestedSvg.appendChild(slice);
        return nestedSvg;
    }
    static drawLegendItem(num, elem, key) {
        let legendItem = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        let square = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        let y = num*20 + 20;
        square.setAttribute('x', '350');
        square.setAttribute('y', `${y}`);
        square.setAttribute('height', '15');
        square.setAttribute('width', '15');
        square.setAttribute('stroke', 'black');
        square.setAttribute('stroke-width', '1px');
        square.setAttribute('fill', `#${elem.objcolor}`);
        let text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('y', `${y+12}`);
        text.setAttribute('x', '370');
        text.setAttribute('font-size', '10');
        text.innerHTML = `${elem.objname}: ${elem[key]}`;
        legendItem.appendChild(square);
        legendItem.appendChild(text);
        return legendItem;
    }
    static drawSVG(orderedData, value, key) {
        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        let legend = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.setAttribute('width', '100%');
        svg.setAttribute('height', '400');
        const center = [150, 150];
        const radius = 145;
        let startRad = 0;
        orderedData.forEach((elem, num) => {
            let diffRad = (elem[value] / 100) * Math.PI * 2;
            let slice = PieChart.drawPieSlice(startRad, diffRad, radius, center, elem, key);
            let legendItem = PieChart.drawLegendItem(num, elem, key);
            svg.appendChild(slice);
            legend.appendChild(legendItem);
            startRad += diffRad;
        })
        svg.appendChild(legend);
        return svg;
    }
    static drawIntoTable(newRow, column, data) {
        BaseChart.drawIntoTable(PieChart, newRow, column, data);
        return true;
    }
}
class BaseSection {
    constructor(section) {
        this.section = section;
        this.sectionHasContent = ('content' in section);
        this.sectionHasBlocks = ('header' in section);
        this.sectionHasTitle = ('tbl_cap' in section);
    }
    static buildTitle(section) {
        let title = document.createElement('h3');
        title.innerHTML = section.tbl_cap;
        if (section.href) {
            title.id = section.href;
        }
        return title;
    }
    init() {
        let div = document.createElement('div');
        if (this.section.sect_id) {
            div.setAttribute('id', this.section.sect_id);
        }
        if (this.sectionHasTitle) {
            div.appendChild(BaseSection.buildTitle(this.section));
        }
        if (this.sectionHasBlocks) {
            let table = document.createElement('table');
            table.setAttribute('id', `${this.section.sect_id}_t`);
            for (let i = 0; i < this.section.header.length; i++) {
                let newBlock = structuredClone(this.section);
                newBlock.header = this.section.header[i];
                if (newBlock.header.source) {
                    newBlock.data = data.datasets[newBlock.header.source];
                    if (newBlock.header.filter) {
                        newBlock.data = Utilities.filter(newBlock.data, newBlock.header.filter);
                    }
                    if (newBlock.header.ordering) {
                        let direction = 1;
                        let key = newBlock.header.ordering;
                        if (newBlock.header.ordering.startsWith('-')) {
                            direction = -1;
                            key = newBlock.header.ordering.slice(1);
                        }
                        newBlock.data = Utilities.sort(newBlock.data, key, direction);
                    }
                    if (newBlock.header.limit) {
                        newBlock.data = Utilities.limit(newBlock.data, Number.parseInt(data.properties[newBlock.header.limit]));
                    }
                } else {
                    newBlock.data = this.section.data[i];
                }
                table.setAttribute('class', newBlock.header.class);
                if (newBlock.header.type === 'row_table') {
                    div.appendChild(new HorizontalTable(newBlock, table).init());
                } else if (newBlock.header.type === 'column_table') {
                    div.appendChild(new VerticalTable(newBlock, table).init());
                }
            }
        }
        if (this.sectionHasContent) {
            const divider = '{func_output}';
            let contentOne = this.section.content.split(divider)[0];
            let contentTwo = this.section.content.split(divider)[1];
            let table = div.querySelector('table');
            if (table) {
                if (contentOne) {
                    table.insertAdjacentHTML('beforebegin', contentOne);
                }
                if (contentTwo) {
                    table.insertAdjacentHTML('afterend', contentTwo);
                }
            }
        }
        return div;
    }
}
class BaseTable extends BaseSection {
    static uniqueHeaders = {
        'waitEventsBlock': BaseTable.buildWaitEventsBlockHeader
    }
    static uniqueCells = {
        'queryId': BaseTable.buildQueryIdCell,
        'jitCellId': BaseTable.buildJitCellId,
        'jitTimeCell': BaseTable.buildJitTimeCell,
        'interval': BaseTable.buildIntervalCell,
        'queryTextId': BaseTable.buildQueryTextIdCell,
        'planId': BaseTable.buildPlanIdCell,
        'queryText': BaseTable.buildQueryTextCell,
        'waitEvent': BaseTable.buildWaitEventDetailsCell,
        'pipeChart': PipeChart.drawIntoTable,
        'pieChart': PieChart.drawIntoTable
    };
    static properties = {
        'topn': data.properties.topn,
        'end1_id': data.properties.end1_id,
        'end2_id': data.properties.end2_id,
        'start1_id': data.properties.start1_id,
        'start2_id': data.properties.start2_id,
        'description': data.properties.description,
        'report_end1': data.properties.report_end1,
        'report_end2': data.properties.report_end2,
        'server_name': data.properties.server_name,
        'report_start1': data.properties.report_start1,
        'report_start2': data.properties.report_start2,
        'max_query_length': data.properties.max_query_length,
        'pgprofile_version': data.properties.pgprofile_version,
        'server_description': data.properties.server_description,
        'checksum_fail_detected': data.properties.checksum_fail_detected,
        'interval1_duration_sec': data.properties.interval1_duration_sec,
        'interval2_duration_sec': data.properties.interval2_duration_sec
    }
    constructor(section, table) {
        super(section);
        this.table = table;
    }
    static buildWaitEventsBlockHeader(table, header) {
        let tr0 = document.createElement('tr');
        let tr1 = document.createElement('tr');
        let tr2 = document.createElement('tr');
        let th0 = document.createElement('th');
        let mainColumn = header.columns[0];
        th0.innerHTML = mainColumn.caption;
        th0.setAttribute('colspan', '100%');
        tr0.appendChild(th0);
        mainColumn.columns.forEach(column => {
            let th1 = document.createElement('th');
            th1.innerHTML = column.caption;
            if (column.columns) {
                th1.setAttribute('colspan', column.columns.length);
                column.columns.forEach(column => {
                    let th2 = document.createElement('th');
                    th2.innerHTML = column.caption;
                    if (column.title) {
                        th2.setAttribute('title', column.title);
                    }
                    tr2.appendChild(th2);
                })
            } else {
                th1.setAttribute('rowspan', '2');
            }
            tr1.appendChild(th1);
        })
        table.appendChild(tr0);
        table.appendChild(tr1);
        table.appendChild(tr2);
    }
    static buildJitTimeCell(newRow, column, row) {
        let newCell = newRow.insertCell(-1);
        if (row[column.id]) {
            newCell.setAttribute('class', 'table_obj_value');
            let p = document.createElement('p');
            let a = document.createElement('a');
            a.href = `#jit_${row.hexqueryid}_${row.datid}_${row.userid}_${row.toplevel}`;
            a.innerHTML = row[column.id];
            p.appendChild(a);
            newCell.appendChild(p);
        }
        return !!row[column.id];
    }
    static buildQueryIdCell(newRow, column, row) {
        let newCell = newRow.insertCell(-1);
        newCell.setAttribute('class', column.class);
        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        let p1 = document.createElement('p');
        let a1 = document.createElement('a');
        a1.href = `#${row.hexqueryid}`;
        a1.innerHTML = row.hexqueryid;
        p1.appendChild(a1);
        let p2 = document.createElement('p');
        let small = document.createElement('small');
        p2.appendChild(small);
        small.innerHTML = `[${row.hashed_ids}]`;
        if (!row.toplevel) {
            small = document.createElement('small');
            p2.appendChild(small);
            small.innerHTML = '(N)';
            small.title = 'Nested level';
        }
        newCell.appendChild(p1);
        newCell.appendChild(p2);
        return !!row.hexqueryid;
    }
    static buildJitCellId(newRow, column, row) {
        BaseTable.buildQueryIdCell(newRow, column, row);
        newRow.firstChild.setAttribute('id', `jit_${row.hexqueryid}_${row.datid}_${row.userid}_${row.toplevel}`);
    }
    static buildPlanIdCell(newRow, column, row) {
        let newCell = newRow.insertCell(-1);
        newCell.setAttribute('class', column.class);
        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        let p1 = document.createElement('p');
        let a1 = document.createElement('a');
        a1.setAttribute('href', `#${row.hexqueryid}_${row.hexplanid}`);
        a1.innerHTML = row.hexplanid;
        p1.appendChild(a1);
        newCell.appendChild(p1);
        return !!row.hexplanid;
    }
    static buildQueryTextIdCell(newRow, column, row) {
        let cell = newRow.insertCell(-1);
        let columnId;
        if (row['hexplanid']) {
            columnId = row['hexplanid'];
            newRow.classList.add('plantext');
            cell.setAttribute('id', `${row[column.id]}_${columnId}`);
        } else {
            columnId = row[column.id];
            newRow.classList.add('statement');
            cell.setAttribute('id', columnId);
        }
        let newText = document.createTextNode(columnId);
        cell.setAttribute('class', column.class);
        const countMatches = [...Object.keys(row).join(' ').matchAll(new RegExp('query_text*', 'g'))].length;
        cell.setAttribute('rowspan', `${countMatches}`);
        cell.appendChild(newText);
        return !!columnId;
    }
    static buildQueryTextCell(newRow, column, row) {
        if (row['hexplanid'] || row[column.id]) {
            let newCell = newRow.insertCell(-1);
            let newText = null;
            newCell.setAttribute('class', column.class);
            if (row['hexplanid'] && row[column.id]) {
                newText = `<pre>${row[column.id]}</pre>`;
                newCell.insertAdjacentHTML('afterbegin', newText);
            } else if (row[column.id]) {
                newText = document.createTextNode(row[column.id]);
                newCell.appendChild(newText);
            }
        }
        return !!row[column.id];
    }
    static buildIntervalCell(newRow, column, row) {
        let cell = newRow.insertCell(-1);
        let newText = document.createTextNode(column.id);
        if (column.class) {
            cell.setAttribute('class', 'table_obj_value');
        }
        if (column.rowspan) {
            cell.setAttribute('rowspan', '2');
        }
        if (column.title) {
            cell.setAttribute('title', BaseTable.getTagTitle(column.title));
        }
        cell.appendChild(newText);
        return true;
    }
    static buildWaitEventDetailsCell(newRow, column, row) {
        let newCell = newRow.insertCell(-1);
        newRow.setAttribute('id', `${row.event_type}_${row.hexqueryid}_${row.hexplanid}`);
        if (column.rowspan) {
            newCell.setAttribute('rowspan', row.rowspan);
        }
        if (column.class) {
            newCell.setAttribute('class', column.class);
            newCell.classList.add('table_obj_value');
        }
        let dataExists = Boolean(row[column.id]);
        if (dataExists) {
            for (let i = 0; i < row[column.id].length; i++) {
                let details = row[column.id][i];
                if (details.event) {
                    let p = document.createElement('p');
                    let a = document.createElement('a');
                    if (row.event_type === 'Total') {
                        a.href = `#${details.event}_${row.hexqueryid}_${row.hexplanid}`;
                    }
                    a.innerHTML = details.event;
                    p.appendChild(a);
                    let colons = document.createTextNode(': ');
                    p.appendChild(colons);
                    let strong = document.createElement('strong');
                    strong.innerHTML = details.wait;
                    p.appendChild(strong);
                    newCell.appendChild(p);
                }
            }
        }
        return true;
    }
    static bifurcateObject(column) {
        let objects = [];
        let keys = Object.keys(column);
        for (let i = 0; i < column.id.length; i++) {
            let newObj = structuredClone(column);
            for (let j = 0; j < keys.length; j++) {
                if (typeof newObj[keys[j]] === 'object') {
                    newObj[keys[j]] = column[keys[j]][i];
                }
            }
            objects.push(newObj);
        }
        return objects;
    }
    static getTagTitle(title) {
        const TITLES = {
            'properties.timePeriod1': `(${BaseTable.properties.report_start1} - ${BaseTable.properties.report_end1})`,
            'properties.timePeriod2': `(${BaseTable.properties.report_start2} - ${BaseTable.properties.report_end2})`,
            'properties.timePeriod1,properties.timePeriod2': 'Sample\'s time period'
        }
        if (TITLES[title] !== undefined) {
            return TITLES[title];
        }
        return title;
    }
    static hasSpecialClass(column) {
        if (column.class) {
            let classList = column.class.split(' ');
            for (let i = 0; i < classList.length; i++) {
                let klass = classList[i].trim();
                if (BaseTable.uniqueCells[klass]) {
                    return klass;
                }
            }
        }
        return false;
    }
    static collectHeader(header, deep, resultMatrix) {
        if (resultMatrix === null) {
            resultMatrix = [];
        }
        if (header.caption) {
            if (!resultMatrix[deep]) {
                resultMatrix.push([]);
            }
            let sumCols = 0;
            if (header.columns) {
                deep++;
                header.columns.forEach(column => {
                    BaseTable.collectHeader(column, deep, resultMatrix);
                    if (column.columns) {
                        sumCols += column.columns.length - 1;
                    }
                })
                deep--;
            }
            let th = {
                'caption': header.caption,
                'colspan': header.columns ? header.columns.length + sumCols : 1,
                'rowspan': deep === 0 && !('columns' in header) ? 2 : 1
            };
            if (header.id) {th['id'] = header.id}
            if (header.title) {th['title'] = BaseTable.getTagTitle(header.title)}
            resultMatrix[deep].push(th);
        } else {
            if (header.columns) {
                header.columns.forEach(column => {
                    BaseTable.collectHeader(column, deep, resultMatrix);
                })
            }
        }
        return resultMatrix;
    }
    buildHeader() {
        let classList = this.section.header.class.split(' ');
        for (let i = 0; i < classList.length; i++) {
            let klass = classList[i].trim();
            if (BaseTable.uniqueHeaders[klass]) {
                return BaseTable.uniqueHeaders[klass](this.table, this.section.header);
            }
        }
        let headerMatrix = BaseTable.collectHeader(this.section.header, 0, null);
        headerMatrix.forEach(row => {
            let tr = document.createElement('tr');
            row.forEach(column => {
                let th = document.createElement('th');
                th.innerHTML = column.caption;
                th.setAttribute('rowspan', headerMatrix.length === 1 ? 1 : column.rowspan);
                th.setAttribute('colspan', column.colspan);
                if (column.title) {
                    th.setAttribute('title', column.title);
                }
                if (column.id) {
                    th.setAttribute('id', column.id);
                }
                tr.appendChild(th);
            });
            this.table.appendChild(tr);
        });
        if (headerMatrix.length === 1) {
            let emptyRow = document.createElement('tr');
            this.table.appendChild(emptyRow);
        }
    }
    init() {
        this.buildHeader();
        this.insertRows();
        return this.table;
    }
}
class HorizontalTable extends BaseTable {
    static buildCell(newRow, column, row) {
        let specialClass = BaseTable.hasSpecialClass(column);
        if (specialClass) {
            let notEmpty = BaseTable.uniqueCells[specialClass](newRow, column, row);
            return !notEmpty;
        }
        let newCell = newRow.insertCell(-1);
        if (column.rowspan) {
            newCell.setAttribute('rowspan', '2');
        }
        if (column.class) {
            newCell.setAttribute('class', column.class);
        }
        if (row[column.id]) {
            let newText = document.createTextNode(row[column.id]);
            newCell.appendChild(newText);
            return false;
        }
        return true;
    }
    static collectColumns(header, array, matrix) {
        if (array === null) {
            array = [];
        }
        if (matrix === null) {
            matrix = [];
        }
        if ('columns' in header) {
            for (let i = 0; i < header.columns.length; i++) {
                let column = header.columns[i];
                HorizontalTable.collectColumns(column, array, matrix);
            }
        } else {
            if (typeof header.id === 'string') {
                array.push(header);
            } else if (typeof header.id === 'object') {
                matrix.push(BaseTable.bifurcateObject(header));
            }
        }
        if (matrix.length) {
            matrix = matrix[0].map((_, colIndex) => matrix.map(row => row[colIndex]));
        }
        return [array, matrix];
    }
    static getColumns(columns) {
        let matrixCollection = HorizontalTable.collectColumns(columns, null, null);
        let singeRowColumns = matrixCollection[0];
        let matrixWithVectors = matrixCollection[1];
        let newMatrix = Array.from(matrixCollection[1]);
        if (matrixWithVectors[0]) {
            newMatrix[0] = singeRowColumns.concat(matrixWithVectors[0]);
        } else {
            newMatrix[0] = singeRowColumns;
        }
        return newMatrix;
    }
    static setDataAttrs(newRow, row) {
        let attributesMap = {
            'dbname': 'data-dbname',
            'hexqueryid': 'data-hexqueryid',
            'queryid': 'data-queryid',
            'hexplanid': 'data-hexplanid',
            'toplevel': 'data-toplevel',
            'userid': 'data-userid',
            'event_type': 'data-event_type',
            'event': 'data-event',
            'tablespacename': 'data-tablespacename',
            'schemaname': 'data-schemaname',
            'relname': 'data-relname',
            'indexrelname': 'data-indexrelname',
            'funcname': 'data-funcname'
        }
        for (const [key, value] of Object.entries(attributesMap)) {
            if (row[key]) {
                newRow.setAttribute(value, row[key]);
            }
        }
    }
    insertRows() {
        let columns = HorizontalTable.getColumns(this.section.header);  // Getting columns matrix
        let rows = this.section.data;  // Getting json array with data
        let isParent = true;
        for (let i = 0; i < rows.length; i++) {
            let row = rows[i];
            for (let j = 0; j < columns.length; j++) {
                let newRow = this.table.insertRow(-1);
                if (this.table.classList.contains('diff') || this.table.classList.contains('toast')) {
                    newRow.classList.add(isParent ? 'int1' : 'int2');
                    isParent = !isParent;
                }
                if (row.klass) {
                    newRow.setAttribute('class', row.klass);
                }
                HorizontalTable.setDataAttrs(newRow, row);
                let isEmpty = [];
                for (let k = 0; k < columns[j].length; k++) {
                    let column = columns[j][k];
                    isEmpty.push(HorizontalTable.buildCell(newRow, column, row));
                }
                if (isEmpty.every(Boolean)) {
                    newRow.style.visibility = 'collapse';
                    let prevSib = newRow.previousSibling;
                    prevSib.style.verticalAlign = 'baseline';
                }
            }
        }
    }
}
class VerticalTable extends BaseTable {
    static getColumns(section) {
        let columns = [];
        let rows = section.header.rows;
        for (let i = 0; i < rows.length; i++) {
            if (typeof rows[i].id === 'object') {
                columns.push(BaseTable.bifurcateObject(rows[i]));
            } else {
                columns.push(rows[i]);
            }
        }
        return columns;
    }
    static buildCell(newRow, column, row) {
        let specialClass = BaseTable.hasSpecialClass(column);
        if (specialClass) {
            BaseTable.uniqueCells[specialClass](newRow, column, row);
            return;
        }
        let newCell = newRow.insertCell(-1);
        if ('caption' in column) {
            let newText = document.createTextNode(column['caption']);
            newCell.appendChild(newText);
        }
        if ('title' in column) {
            newCell.setAttribute('title', column.title);
        }
        if ('id' in column) {
            newCell.innerHTML = row[0][column.id];
        }
        if ('class' in column) {
            newCell.setAttribute('class', column.class);
        }
    }
    insertRows() {
        let columns = VerticalTable.getColumns(this.section);
        let rows = this.section.data;
        for (let i = 0; i < columns.length; i++) {
            let newRow = this.table.insertRow(-1);
            VerticalTable.buildCell(newRow, columns[i], rows);
            columns[i].cells.forEach(cell => {
                VerticalTable.buildCell(newRow, cell, rows);
            });
        }
    }
}
class Highlighter {
    static toggleClass(tr, allRows) {
        if (!tr.classList.contains('active')) {
            Highlighter.cleanAllActiveClasses(allRows);
            if (Popup.getInstance()) {
                const notice = document.createElement('div');
                const p = document.createElement('p');
                p.textContent = 'Highlighted row attributes:'
                notice.appendChild(p);
                const table = document.createElement('table');
                Object.keys(tr.dataset).forEach(key => {
                    let _tr = table.insertRow(-1);
                    let tdKey = _tr.insertCell(-1);
                    tdKey.innerHTML = key;
                    let tdVal = _tr.insertCell(-1);
                    tdVal.innerHTML = tr.dataset[key];
                });
                notice.appendChild(table);
                Popup.sendNotice(Popup.STYLE.BANNER, notice);
            }
            allRows.forEach((elem) => {
                let isEqual = Highlighter.isDatasetEqual(tr.dataset, elem.dataset, elem);
                if (isEqual) {
                    elem.classList.add('active');
                    let navId = this.getClosestTag(elem, 0, 'div').firstChild.id;
                    if (navId) {
                        let navLi= document.getElementById(`navigator_${navId}`);
                        if (navLi && !navLi.classList.contains('active')) {
                            navLi.classList.add('active');
                        }
                    }
                }
            });
        } else {
            Highlighter.cleanAllActiveClasses(allRows);
            if (Popup.getInstance()) {
                Popup.popupDisappearing();
            }
        }
    }
    static getAllRows() {
        return document.querySelectorAll('tr');
    }
    static getClosestTag(target, curDeep, targetTag) {
        let tooDeep = curDeep >= 5;
        let headOfTable = target.tagName.toLowerCase() === 'th';
        let stillNotRow = target.tagName.toLowerCase() !== targetTag;
        if (tooDeep) {
            return false;
        } else if (headOfTable) {
            return false;
        } else if (stillNotRow) {
            curDeep++;
            return Highlighter.getClosestTag(target.parentNode, curDeep, targetTag);
        } else {
            return target;
        }
    }
    static cleanAllActiveClasses(rows) {
        rows.forEach(elem => {
            if (elem.classList.contains('active')) {
                elem.classList.remove('active');
            }
        })
        let navigator = document.getElementById('navigator');
        if (navigator) {
            let allItems = document.querySelectorAll('li');
            allItems.forEach(item => {
                if (item.classList.contains('active')) {
                    item.classList.remove('active');
                }
            })
        }
    }
    static isDatasetEqual(targetDataset, rowDataset, elem) {
        if (!Object.keys(targetDataset).length) {
            return false;
        }
        let tableIsSqlList = Highlighter.getClosestTag(elem, 0, 'table').id === 'sqllist_t';
        let isSameQuery = targetDataset.queryid !== undefined
            && targetDataset.hexqueryid === rowDataset.hexqueryid
            && targetDataset.planid === rowDataset.planid;
        if (tableIsSqlList && isSameQuery) {
            return true;
        }
        for (let data in targetDataset) {
            if (targetDataset[data] === '*' && rowDataset[data] !== undefined) {
                continue;
            }
            if (targetDataset[data] !== rowDataset[data]) {
                return false;
            }
        }
        return true;
    }
    static setBackgroundColorToRow(tr, hoverColor, transition) {
        tr.querySelectorAll('td').forEach(td => {
            td.style.backgroundColor = hoverColor;
            td.style.transition = transition;
        })
        let siblings = null;
        if (tr.classList.contains('int1')) {
            siblings = tr.nextSibling.querySelectorAll('td');
        } else if (tr.classList.contains('int2')) {
            siblings = tr.previousSibling.querySelectorAll('td');
        }
        if (siblings) {
            siblings.forEach(elem => {
                elem.style.backgroundColor = hoverColor;
                elem.style.transition = transition;
            })
        }
    }
    static highlight(event, allRows) {
        if (event.target.tagName.toLowerCase() !== 'a') {
            let tr = Highlighter.getClosestTag(event.target, 0, 'tr');
            if (tr && Object.keys(tr.dataset).length) {
                Highlighter.toggleClass(tr, allRows);
            }
        }
    }
    static smartHover(eventType, event) {
        let hoverColor = '#D9FFCC';
        let transition = 'background-color 300ms';
        let tr = Highlighter.getClosestTag(event.target, 0, 'tr');
        if (tr && eventType === 'mouseover') {
            Highlighter.setBackgroundColorToRow(tr, hoverColor, transition);
        } else if (tr && eventType === 'mouseout') {
            Highlighter.setBackgroundColorToRow(tr, '', transition);
        }
    }
    static init() {
        const ALL_ROWS = Highlighter.getAllRows();
        ALL_ROWS.forEach((elem) => {
            elem.addEventListener('click', (event) => {
                Highlighter.highlight(event, ALL_ROWS);
            });
            ['mouseover', 'mouseout'].forEach(eventType => {
                elem.addEventListener(eventType, (event) => {
                    Highlighter.smartHover(eventType, event);
                });
            })
        })
    }
}
class ReportNavigator {
    static buildReportNavigator(CONTENT, NAVIGATOR) {
        window.onscroll = function() {
            if (CONTENT.getBoundingClientRect().bottom <= 0) {
                NAVIGATOR.style.visibility = 'visible';
            } else {
                NAVIGATOR.style.visibility = 'hidden';
            }
            document.querySelectorAll('h3').forEach(title => {
                let position = title.getBoundingClientRect().top;
                if (position >= 0 && position < 100) {
                    let li = `[href*=${title.id}]`;
                    let elem = NAVIGATOR.querySelector(li).closest('li');
                    if (!elem.classList.contains('current')) {
                        NAVIGATOR.querySelectorAll('li').forEach(item => {
                            item.classList.remove('current');
                        });
                        elem.classList.add('current');
                    }
                }
            })
        }
    }
    static buildPageContent(data, parentNode) {
        data.sections.forEach(section => {
            let hasTableCap = ('tbl_cap' in section);
            let hasNestedSections = ('sections' in section);
            let ul = document.createElement('ul');
            let li = document.createElement('li');
            if (hasTableCap) {
                let a = document.createElement('a');
                a.innerHTML = section.tbl_cap;
                a.href = `#${section.href}`;
                li.setAttribute('id', `navigator_${section.href}`);
                li.appendChild(a);
                parentNode.appendChild(li);
            } else {
                ul = li;
            }
            if (hasNestedSections) {
                parentNode.appendChild(this.buildPageContent(section, ul));
            }
        })
        return parentNode;
    }
    static init() {
        const CONTENT = document.getElementById('content');
        const NAVIGATOR = document.createElement('div');
        NAVIGATOR.setAttribute('id', 'navigator');
        let button = document.createElement('div');
        button.innerHTML = 'hide menu';
        button.setAttribute('class', 'active');
        button.setAttribute('title', 'Show / hide content');
        button.innerHTML = 'content';
        NAVIGATOR.appendChild(button);
        let ul = document.createElement('ul');
        ul.setAttribute('class', 'active');
        NAVIGATOR.appendChild(ul);
        button.addEventListener('click', event => {
            if (ul.classList.contains('hidden')) {
                ul.setAttribute('class', 'active');
                button.setAttribute('class', 'active');
            } else {
                ul.setAttribute('class', 'hidden');
                button.setAttribute('class', 'hidden');
            }
        })
        document.querySelector('body').appendChild(NAVIGATOR);
        ReportNavigator.buildPageContent(data, ul);
        ReportNavigator.buildReportNavigator(CONTENT, NAVIGATOR);
    }
}
class Copier {
    static getAllQueryCells() {
        return document.querySelectorAll('.queryId, .jitCellId');
    }
    static drawButton() {
        let button = document.createElement('a');
        button.setAttribute('class', 'copyButton');
        let svg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        svg.style.marginLeft = '10px';
        svg.setAttribute('height', '14px');
        svg.setAttribute('width', '12px');
        let rect = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        rect.setAttribute('x', '2');
        rect.setAttribute('y', '2');
        rect.setAttribute('height', '12px');
        rect.setAttribute('width', '10px');
        rect.setAttribute('rx', '4');
        rect.setAttribute('stroke', 'grey');
        rect.setAttribute('fill', 'transparent');
        let replica = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
        replica.setAttribute('x', '0');
        replica.setAttribute('y', '0');
        replica.setAttribute('height', '12px');
        replica.setAttribute('width', '10px');
        replica.setAttribute('rx', '4');
        replica.setAttribute('stroke', 'grey');
        replica.setAttribute('fill', 'transparent');
        svg.appendChild(rect);
        svg.appendChild(replica);
        button.appendChild(svg);
        return button;
    }
    static sendNotice(text) {
        if (Popup.getInstance()) {
            const notice = document.createElement('p');
            notice.textContent = `queryid value copied to clipboard: ${text}`;
            Popup.sendNotice(Popup.STYLE.MESSAGE, notice);
        }
    }
    static copyQueryId(ev) {
        let text = ev.target.closest('tr').dataset.queryid;
        navigator.clipboard.writeText(text).then(
            function() {
                Copier.sendNotice(text);
            }, function(err) {
            console.error('Async: Could not copy text: ', err);
        });
    }
    static init() {
        const ALL_ID_CELLS = Copier.getAllQueryCells();
        ALL_ID_CELLS.forEach(elem => {
            elem = elem.querySelector('p');
            let button = Copier.drawButton();
            button.addEventListener('click', ev => Copier.copyQueryId(ev));
            elem.appendChild(button);
        })
    }
}
class Popup {
    static hidden = '-20%';
    static appear = '2%';
    static transitionDelay = 250;
    static klass = 'popup';
    static id = 'popup';
    static STYLE = {
        MESSAGE: {
            'bgColor': '#D9FFCC',
            'duration': 3000,
            'fontColor': 'black'
        },
        BANNER: {
            'bgColor': '#CCF1FF',
            'fontColor': 'black'
        },
        ERROR: {
            'color': 'red',
            'duration': 3000
        },
    }
    static #createPopupTag() {
        const POPUP = document.createElement('div');
        POPUP.setAttribute('id', Popup.id);
        POPUP.setAttribute('class', Popup.klass);
        document.getElementById('container').appendChild(POPUP);
        return POPUP;
    }
    static getInstance() {
        return document.getElementById(Popup.id) ? document.getElementById(Popup.id) : false;
    }
    static popupIsHidden() {
        const POPUP = Popup.getInstance();
        return getComputedStyle(POPUP).getPropertyValue('--main-bottom').trim() === Popup.hidden.trim();
    }
    static popupAppearing(noticeProperties, notice) {
        const POPUP = Popup.getInstance();
        return new Promise(result => {
            POPUP.style.setProperty('--main-bottom', Popup.appear);
            POPUP.style.setProperty('--main-bg-color', noticeProperties.bgColor);
            POPUP.style.setProperty('--main-font-color', noticeProperties.fontColor);
            POPUP.innerHTML = ''; 
            let close_link = document.createElement('a');
            close_link.innerHTML = 'x';
            close_link.onclick = function () {
                POPUP.style.display = 'none';
            }
            close_link.style.cursor = 'pointer';
            close_link.style.color = 'gray';
            POPUP.appendChild(close_link);
            POPUP.appendChild(notice);
            if (noticeProperties.duration) {
                setTimeout(result, noticeProperties.duration);
            }
        })
    }
    static popupDisappearing(delay) {
        const POPUP = Popup.getInstance();
        return new Promise(result => {
            POPUP.style.setProperty('--main-bottom', Popup.hidden);
            setTimeout(result, delay);
        })
    }
    static async sendNotice(noticeProperties, message) {
        const POPUP = Popup.getInstance();
        if (!Popup.popupIsHidden(POPUP)) {
            await Popup.popupDisappearing(Popup.transitionDelay);
        }
        await Popup.popupAppearing(noticeProperties, message);
        if (noticeProperties.duration) {
            await Popup.popupDisappearing(noticeProperties.duration + Popup.transitionDelay);
        }
    }
    static init() {
        Popup.#createPopupTag();
    }
}
class Sorter {
    static getAllHeaders() {
        return document.querySelectorAll('th[id]');
    }
    static getClosestTag(target, curDeep, targetTag) {
        let tooDeep = curDeep >= 5;
        let stillNotRow = target.tagName.toLowerCase() !== targetTag;
        if (tooDeep) {
            return false;
        } else if (stillNotRow) {
            curDeep++;
            return Highlighter.getClosestTag(target.parentNode, curDeep, targetTag);
        } else {
            return target;
        }
    }
    static drawTriangle(elem) {
        let div = document.createElement('div');
        div.setAttribute('class', 'triangle-down');
        elem.appendChild(div);
    }
    static sort(tableId, sortingKey, sortingDirection) {
        console.log(tableId);
        console.log(sortingKey);
        console.log(sortingDirection);
    }
    static onClick(event, elem) {
        let triangle = elem.querySelector('div[class*="triangle"]');
        let sortingDirection;
        if (triangle) {
            if (triangle.classList.contains('triangle-down')) {
                triangle.setAttribute('class', 'triangle-up');
                sortingDirection = 1;
            } else {
                triangle.setAttribute('class', 'triangle-down');
                sortingDirection = -1;
            }
            let table = Sorter.getClosestTag(elem, 0, 'table');
            Sorter.sort(table.id, elem.id, sortingDirection);
        }
    }
    static init() {
        const ALL_HEADERS = Sorter.getAllHeaders();
        ALL_HEADERS.forEach(elem => {
            Sorter.drawTriangle(elem);
            elem.addEventListener('click', (event) => {
                Sorter.onClick(event, elem);
            });
        })
    }
}
function buildPageContent(data, parentNode) {
    data.sections.forEach(section => {
        let hasTableCap = ('tbl_cap' in section);
        let hasNestedSections = ('sections' in section);
        let ul = document.createElement('ul');
        let li = document.createElement('li')
        if (hasTableCap) {
            let a = document.createElement('a');
            a.innerHTML = section.tbl_cap;
            a.href = `#${section.href}`;
            li.appendChild(a);
            parentNode.appendChild(li);
        } else {
            ul = li;
        }
        if (hasNestedSections) {
            parentNode.appendChild(buildPageContent(section, ul));
        }
    })
    return parentNode;
}
function buildReport(data, parentNode) {
    data.sections.forEach(section => {
        let sectionHasNestedSections = ('sections' in section);
        let newSection = new BaseSection(section).init();
        if (sectionHasNestedSections) {
            buildReport(section, newSection);
        }
        parentNode.appendChild(newSection);
    })
    return parentNode;
}
function main() {
    const CONTENT = document.getElementById('content');
    buildPageContent(data, CONTENT);
    const CONTAINER = document.getElementById('container');
    buildReport(data, CONTAINER);
    Popup.init();
    Highlighter.init();
    Copier.init();
    Sorter.init();
    ReportNavigator.init();
}
main();$js$
),
('report',
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile report ({properties:start1_id} -'
  ' {properties:end1_id})</title></head><body>'
  '<H1>Postgres profile report ({properties:start1_id} -'
  '{properties:end1_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>Report interval: <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '<ul id="content"></ul>'
  '<div id="container"></div>'
  '<script>{static:script_js}</script>'
  '</body></html>'),
('diffreport',
  '<html lang="en"><head>'
  '<style>{static:css}</style>'
  '<script>const data={dynamic:data1}</script>'
  '<title>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</title></head><body>'
  '<H1>Postgres profile differential report (1): ({properties:start1_id} -'
  ' {properties:end1_id}) with (2): ({properties:start2_id} -'
  ' {properties:end2_id})</H1>'
  '{static:version}'
  '<p>Server name: <strong>{properties:server_name}</strong></p>'
  '{properties:server_description}'
  '<p>First interval (1): <strong>{properties:report_start1} -'
  ' {properties:report_end1}</strong></p>'
  '<p>Second interval (2): <strong>{properties:report_start2} -'
  ' {properties:report_end2}</strong></p>'
  '{properties:description}'
  '<h2>Report sections</h2>'
  '<ul id="content"></ul>'
  '<div id="container"></div>'
  '<script>{static:script_js}</script>'
  '</body></html>')
;
/* === report table data === */
INSERT INTO report(report_id, report_name, report_description, template)
VALUES
(1, 'report', 'Regular single interval report', 'report'),
(2, 'diffreport', 'Differential report on two intervals', 'diffreport')
;
/* === report_struct table data === */
-- Regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'stmt_cmt1', NULL, 100, NULL, NULL, 'stmt_cnt_range', NULL, NULL, '<p><strong>Warning!</strong></p>'
  '<p>This interval contains sample(s) with captured statements count more than 90% of <i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>',
  '[{'
    '"type": "row_table", '
    '"source": "stmt_cnt_range",'
    '"ordering": "sample_id",'
    '"class": "stat", '
    '"columns": ['
        '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample ID"}, '
        '{"id": "sample_time", "class": "table_obj_value", "caption": "Sample Time"}, '
        '{"id": "stmt_cnt", "class": "table_obj_value", "caption": "Stmts Captured"}, '
        '{"id": "max_cnt", "class": "table_obj_value", "caption": "pg_stat_statements.max"}'
    ']}]'::jsonb),
(1, 'srvstat', NULL, 200, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL, NULL),
(1, 'sqlsthdr', NULL, 300, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL, NULL),
(1, 'objects', NULL, 400, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL, NULL),
(1, 'funchdr', NULL, 500, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL, NULL),
(1, 'vachdr', NULL, 600, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL, NULL),
(1, 'settings', NULL, 700, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, NULL, 'pg_settings', NULL, NULL),
(1, 'stmt_warn', NULL, 800, NULL, 'Warning!', 'stmt_cnt_all', NULL, 'stmt_warn', NULL,
  '[{'
    '"type": "row_table", '
    '"source": "stmt_cnt_all",'
    '"ordering": "sample_id",'
    '"class": "stat", '
    '"columns": ['
        '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample ID"}, '
        '{"id": "sample_time", "class": "table_obj_value", "caption": "Sample Time"}, '
        '{"id": "stmt_cnt", "class": "table_obj_value", "caption": "Stmts Captured"}, '
        '{"id": "max_cnt", "class": "table_obj_value", "caption": "pg_stat_statements.max"}'
    ']}]'::jsonb)
;

-- Server section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'dbstat', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL, NULL),
(1, 'dbstatreset', 'dbstat', 200, NULL, NULL, 'dbstats_reset', NULL, NULL,
  '<p><strong>Warning!</strong></p>'
  '<p>Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>',
  '[{'
      '"type": "row_table",'
      '"class": "stat",'
      '"source": "dbstats_reset",'
      '"ordering": "sample_id",'
      '"columns": ['
        '{"caption": "Database", "id": "dbname", "class": "table_obj_name"},'
        '{"caption": "Sample", "id": "sample_id", "class": "table_obj_value"},'
        '{"caption": "Reset time", "id": "stats_reset", "class": "table_obj_value"}'
      ']'
  '}]'::jsonb),
(1, 'dbstatmain', 'dbstat', 300, NULL, NULL, NULL, NULL, NULL, NULL,
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "dbstat",'
    '"ordering": "ord_db",'
    '"columns": ['
      '{"caption": "Database", "id": "dbname", "class": "table_obj_name"},'
      '{"caption": "Transactions", "columns": ['
        '{"caption": "Commits", "id": "xact_commit", "class": "table_obj_value", '
          '"title": "Number of transactions in this database that have been committed"},'
        '{"caption": "Rollbacks", "id": "xact_rollback", "class": "table_obj_value", '
          '"title": "Number of transactions in this database that have been rolled back"},'
        '{"caption": "Deadlocks", "id": "deadlocks", "class": "table_obj_value", '
          '"title": "Number of deadlocks detected in this database"}'
     ']},'
      '{"caption": "Checksums", "condition": "checksum_fail_detected", "columns": ['
        '{"caption": "Failures", "id": "checksum_failures", "class": "table_obj_value", '
          '"title": "Number of block checksum failures detected"},'
        '{"caption": "Last", "id": "checksum_last_failure", "class": "table_obj_value", '
          '"title": "Last checksum failure detected"}'
     ']},'
      '{"caption": "Block statistics", "columns": ['
        '{"caption": "Hit(%)", "id": "blks_hit_pct", "class": "table_obj_value", '
          '"title": "Buffer cache hit ratio"},'
        '{"caption": "Read", "id": "blks_read", "class": "table_obj_value", '
          '"title": "Number of disk blocks read in this database"},'
        '{"caption": "Hit", "id": "blks_hit", "class": "table_obj_value", '
          '"title": "Number of times disk blocks were found already in the buffer cache"}'
     ']},'
      '{"caption": "Block I/O times", "condition": "io_times", "columns": ['
        '{"caption": "Read", "id": "blk_read_time", "class": "table_obj_value", '
          '"title": "Time spent reading data file blocks by backends, in seconds"},'
        '{"caption": "Write", "id": "blk_write_time", "class": "table_obj_value", '
          '"title": "Time spent writing data file blocks by backends, in seconds"}'
     ']},'
      '{"caption": "Tuples", "columns": ['
        '{"caption": "Ret", "id": "tup_returned", "class": "table_obj_value", '
          '"title": "Number of rows returned by queries in this database"},'
        '{"caption": "Fet", "id": "tup_fetched", "class": "table_obj_value", '
          '"title": "Number of rows fetched by queries in this database"},'
        '{"caption": "Ins", "id": "tup_inserted", "class": "table_obj_value", '
          '"title": "Number of rows inserted by queries in this database"},'
        '{"caption": "Upd", "id": "tup_updated", "class": "table_obj_value", '
          '"title": "Number of rows updated by queries in this database"},'
        '{"caption": "Del", "id": "tup_deleted", "class": "table_obj_value", '
          '"title": "Number of rows deleted"}'
     ']},'
      '{"caption": "Temp files", "columns": ['
        '{"caption": "Size", "id": "temp_bytes", "class": "table_obj_value", '
          '"title": "Total amount of data written to temporary files by queries in this database"},'
        '{"caption": "Files", "id": "temp_files", "class": "table_obj_value", '
          '"title": "Number of temporary files created by queries in this database"}'
     ']},'
     '{"caption": "Size", "id": "datsize", "class": "table_obj_value", '
       '"title": "Database size as is was at the moment of last sample in report interval"},'
     '{"caption": "Growth", "id": "datsize_delta", "class": "table_obj_value", '
       '"title": "Database size increment during report interval"}'
    ']'
  '}]'::jsonb),
(1, 'iostat', 'srvstat', 328, 'Cluster I/O statistics', 'Cluster I/O statistics', 'stat_io', NULL, 'stat_io', NULL, NULL),
(1, 'iostatrst', 'srvstat', 329, NULL, NULL, 'stat_io_reset', NULL, 'stat_io_reset',
'<p><b>Warning!</b> IO stats reset was detected during report interval. Statistic values may be affected</p>',
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_io_reset",'
    '"columns": ['
      '{"caption": "Sample ID", "id": "sample_id", "class": "table_obj_name", '
        '"title": "Sample identifier with detected reset"},'
      '{"caption": "Object", "id": "object", "class": "table_obj_name", '
        '"title": "Target object of an I/O operation"},'
      '{"caption": "Backend", "id": "backend_type", "class": "table_obj_name", '
        '"title": "Type of backend (see stat_activity)"},'
      '{"caption": "Context", "id": "context", "class": "table_obj_name", '
        '"title": "The context of an I/O operation"},'
      '{"caption": "Reset time", "id": "stats_reset", "class": "table_obj_value", '
        '"title": "Date and time of the last reset performed in sample"}'
    ']'
    '}]'::jsonb),
(1, 'iostatmain', 'srvstat', 330, NULL, NULL, 'stat_io', NULL, 'stat_io', NULL,
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_io",'
    '"columns": ['
      '{"caption": "Object", "id": "object", "class": "table_obj_name", '
        '"title": "Target object of an I/O operation"},'
      '{"caption": "Backend", "id": "backend_type", "class": "table_obj_name", '
        '"title": "Type of backend (see stat_activity)"},'
      '{"caption": "Context", "id": "context", "class": "table_obj_name", '
        '"title": "The context of an I/O operation"},'
      '{"caption": "Reads", "columns": ['
        '{"caption": "Count", "id": "reads", "class": "table_obj_value", '
          '"title": "Number of read operations"},'
        '{"caption": "Bytes", "id": "read_sz", "class": "table_obj_value", '
          '"title": "Read data amount"},'
        '{"caption": "Time", "id": "read_time", "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in reading operation (seconds)"}'
     ']},'
      '{"caption": "Writes", "columns": ['
        '{"caption": "Count", "id": "writes", "class": "table_obj_value", '
          '"title": "Number of write operations"},'
        '{"caption": "Bytes", "id": "write_sz", "class": "table_obj_value", '
          '"title": "Written data amount"},'
        '{"caption": "Time", "id": "write_time", "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in writing operations (seconds)"}'
     ']},'
      '{"caption": "Writebacks", "columns": ['
        '{"caption": "Count", "id": "writebacks", "class": "table_obj_value", '
          '"title": "Number of blocks which the process requested the kernel write out to permanent storage"},'
        '{"caption": "Bytes", "id": "writeback_sz", "class": "table_obj_value", '
          '"title": "The amount of data requested for write out to permanent storage"},'
        '{"caption": "Time", "id": "writeback_time", "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in writeback operations (seconds)"}'
     ']},'
      '{"caption": "Extends", "columns": ['
        '{"caption": "Count", "id": "extends", "class": "table_obj_value", '
          '"title": "Number of relation extend operations"},'
        '{"caption": "Bytes", "id": "extend_sz", "class": "table_obj_value", '
          '"title": "The amount of space used by extend operations"},'
        '{"caption": "Time", "id": "extend_time", "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in extend operations (seconds)"}'
     ']},'
     '{"caption": "Hits", "id": "hits", "class": "table_obj_value", '
       '"title": "The number of times a desired block was found in a shared buffer"},'
     '{"caption": "Evictions", "id": "evictions", "class": "table_obj_value", '
       '"title": "Number of times a block has been written out from a shared or local buffer in order to make it available for another use"},'
     '{"caption": "Reuses", "id": "reuses", "class": "table_obj_value", '
       '"title": "The number of times an existing buffer in a size-limited ring buffer outside of shared buffers was reused as part of an I/O operation in the bulkread, bulkwrite, or vacuum contexts"},'
      '{"caption": "Fsyncs", "columns": ['
        '{"caption": "Count", "id": "fsyncs", "class": "table_obj_value", '
          '"title": "Number of fsync calls. These are only tracked in context normal"},'
        '{"caption": "Time", "id": "fsync_time", "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in fsync operations (seconds)"}'
     ']}'
    ']'
    '}]'::jsonb),
(1, 'slrustat', 'srvstat', 358, 'Cluster SLRU statistics', 'Cluster SLRU statistics', 'stat_slru', NULL, 'stat_slru', NULL, NULL),
(1, 'slrustatrst', 'slrustat', 359, NULL, NULL, 'stat_slru_reset', NULL, 'stat_slru_reset',
'<p><b>Warning!</b> SLRU stats reset was detected during report interval. Statistic values may be affected</p>',
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_slru_reset",'
    '"columns": ['
      '{"caption": "Sample ID", "id": "sample_id", "class": "table_obj_name", '
        '"title": "Sample identifier with detected reset"},'
      '{"caption": "Name", "id": "name", "class": "table_obj_name", '
        '"title": "Name of the SLRU"},'
      '{"caption": "Reset time", "id": "stats_reset", "class": "table_obj_value", '
        '"title": "Date and time of the last reset performed in sample"}'
    ']'
    '}]'::jsonb),
(1, 'slrustatmain', 'slrustat', 360, NULL, NULL, 'stat_slru', NULL, 'stat_slru', NULL,
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_slru",'
    '"columns": ['
      '{"caption": "Name", "id": "name", "class": "table_obj_name", '
        '"title": "Name of the SLRU"},'
      '{"caption": "Zeroed", "id": "blks_zeroed", "class": "table_obj_value", '
        '"title": "Number of blocks zeroed during initializations"},'
      '{"caption": "Hits", "id": "blks_hit", "class": "table_obj_value", '
        '"title": "Number of times disk blocks were found already in the SLRU, so that a '
        'read was not necessary (this only includes hits in the SLRU, not the operating '
        'system''s file system cache)"},'
      '{"caption": "Reads", "id": "blks_read", "class": "table_obj_value", '
        '"title": "Number of disk blocks read for this SLRU"},'
      '{"caption": "%Hit", "id": "hit_pct", "class": "table_obj_value", '
        '"title": "Number of disk blocks hits for this SLRU as a percentage of reads + hits"},'
      '{"caption": "Writes", "id": "blks_written", "class": "table_obj_value", '
        '"title": "Number of disk blocks written for this SLRU"},'
      '{"caption": "Checked", "id": "blks_exists", "class": "table_obj_value", '
        '"title": "Number of blocks checked for existence for this SLRU (blks_exists field)"},'
      '{"caption": "Flushes", "id": "flushes", "class": "table_obj_value", '
        '"title": "Number of flushes of dirty data for this SLRU"},'
      '{"caption": "Truncates", "id": "truncates", "class": "table_obj_value", '
        '"title": "Number of truncates for this SLRU"}'
    ']'
    '}]'::jsonb),
(1, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', NULL, 'db_stat_sessions', NULL,
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "dbstat",'
    '"ordering": "ord_db",'
    '"columns": ['
      '{"caption": "Database", "id": "dbname", "class": "table_obj_name"},'
      '{"caption": "Timings", "columns": ['
        '{"caption": "Total", "id": "session_time", "class": "table_obj_value", '
          '"title": "Time spent by database sessions in this database (note that statistic are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time will not be included)"},'
        '{"caption": "Active", "id": "active_time", "class": "table_obj_value", '
          '"title": "Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)"},'
        '{"caption": "Idle", "id": "idle_in_transaction_time", "class": "table_obj_value", '
          '"title": "Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)"}'
     ']},'
     '{"caption": "Sessions", "columns": ['
        '{"caption": "Established", "id": "sessions", "class": "table_obj_value", '
          '"title": "Total number of sessions established to this database"},'
        '{"caption": "Abondoned", "id": "sessions_abandoned", "class": "table_obj_value", '
          '"title": "Number of database sessions to this database that were terminated because connection to the client was lost"},'
        '{"caption": "Fatal", "id": "sessions_fatal", "class": "table_obj_value", '
          '"title": "Number of database sessions to this database that were terminated by fatal errors"},'
        '{"caption": "Killed", "id": "sessions_killed", "class": "table_obj_value", '
          '"title": "Number of database sessions to this database that were terminated by operator intervention"}'
     ']}'
    ']'
    '}]'::jsonb),
(1, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', NULL, 'st_stat', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "statements_dbstats",'
      '"ordering": "ord_db",'
      '"columns": ['
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of query executions", "caption": "Calls"}, '
          '{"caption": "Time (s)", "columns": ['
              '{"id": "total_plan_time", "class": "table_obj_value", "title": "Time spent planning queries", "caption": "Plan", "condition": "planning_times"}, '
              '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent executing queries", "caption": "Exec"}, '
              '{"id": "blk_read_time", "class": "table_obj_value", "title": "Time spent reading blocks", "caption": "Read"}, '
              '{"id": "blk_write_time", "class": "table_obj_value", "title": "Time spent writing blocks", "caption": "Write"}, '
              '{"id": "trg_fn_total_time", "class": "table_obj_value", "title": "Time spent in trigger functions", "caption": "Trg"}'
              ']}, '
          '{"caption": "Temp I/O Time", "condition": "statements_temp_io_times", "columns": ['
              '{"id": "temp_blk_read_time", "class": "table_obj_value", "title": "Time spent reading temporary file blocks, in seconds", "caption": "Read"}, '
              '{"id": "temp_blk_write_time", "class": "table_obj_value", "title": "Time spent writing temporary file blocks, in seconds", "caption": "Write"} '
              ']}, '
          '{"title": "Number of blocks fetched (hit + read)", "caption": "Fetched (blk)", "columns": ['
              '{"id": "shared_gets", "class": "table_obj_value", "caption": "Shared"}, '
              '{"id": "local_gets", "class": "table_obj_value", "caption": "Local"}'
              ']}, '
          '{"title": "Number of blocks dirtied", "caption": "Dirtied (blk)", "columns": ['
              '{"id": "shared_blks_dirtied", "class": "table_obj_value", "caption": "Shared"}, '
              '{"id": "local_blks_dirtied", "class": "table_obj_value", "caption": "Local"}'
              ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": "temp_blks_read", "class": "table_obj_value", "caption": "Read"}, '
              '{"id": "temp_blks_written", "class": "table_obj_value", "caption": "Write"}'
              ']}, '
          '{"title": "Number of blocks, used for temporary tables", "caption": "Local (blk)", "columns": ['
              '{"id": "local_blks_read", "class": "table_obj_value", "caption": "Read"}, '
              '{"id": "local_blks_written", "class": "table_obj_value", "caption": "Write"}'
              ']}, '
          '{"id": "statements", "class": "table_obj_value", "caption": "Statements"}, '
          '{"id": "wal_bytes_fmt", "class": "table_obj_value", "caption": "WAL size", "condition": "statement_wal_bytes"}'
      ']}]'::jsonb),
(1, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', NULL, 'dbagg_jit_stat', NULL,
  '[{'
      '"type": "row_table",'
      '"class": "stat", '
      '"source": "statements_dbstats",'
      '"ordering": "ord_db",'
      '"columns": ['
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of query executions", "caption": "Calls"}, '
          '{"caption": "Time", "columns": ['
              '{"id": "total_plan_time", "class": "table_obj_value", "title": "Time spent planning queries", "caption": "Plan"}, '
              '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent executing queries", "caption": "Exec"}'
              ']}, '
          '{"caption": "Generation", "columns": ['
              '{"id": "jit_functions", "class": "table_obj_value", "caption": "Count"}, '
              '{"id": "jit_generation_time", "class": "table_obj_value", "caption": "Gen. time"}'
              ']}, '
          '{"caption": "Inlining", "columns": ['
              '{"id": "jit_inlining_count", "class": "table_obj_value", "caption": "Count"}, '
              '{"id": "jit_inlining_time", "class": "table_obj_value", "caption": "Time"}'
              ']}, '
          '{"caption": "Optimization", "columns": ['
              '{"id": "jit_optimization_count", "class": "table_obj_value", "caption": "Count"}, '
              '{"id": "jit_optimization_time", "class": "table_obj_value", "caption": "Time"}'
              ']}, '
          '{"caption": "Emission", "columns": ['
              '{"id": "jit_emission_count", "class": "table_obj_value", "caption": "Count"}, '
              '{"id": "jit_emission_time", "class": "table_obj_value", "caption": "Time"}'
              ']}'
          ']'
      '}]'::jsonb),
(1, 'commonstat', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(1, 'clusterstat', 'commonstat', 650, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(1, 'clusthdr', 'clusterstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat', NULL, NULL),
(1, 'clustrst', 'clusterstat', 800, NULL, NULL, 'cluster_stats_reset', NULL, NULL,
  '<p><strong>Warning!</strong> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>',
  '[{'
        '"type": "row_table",'
        '"class": "stat", '
        '"source": "cluster_stats_reset",'
        '"columns": ['
            '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample"}, '
            '{"id": "bgwriter_stats_reset", "class": "table_obj_value", "caption": "BGWriter reset time"}, '
            '{"id": "archiver_stats_reset", "class": "table_obj_value", "caption": "Archiver reset time"}'
            ']'
        '}]'::jsonb),
(1, 'clust', 'clusterstat', 900, NULL, NULL, NULL, NULL, NULL, NULL,
  '[{'
    '"type": "column_table", '
    '"class": "stat", '
    '"source": "cluster_stats",'
    '"columns": ['
        '{"caption": "Metric"}, '
        '{"caption": "Value"}'
    '],'
    '"rows": ['
        '{"caption": "Scheduled checkpoints", "cells": ['
            '{"id": "checkpoints_timed", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Requested checkpoints", "cells": ['
            '{"id": "checkpoints_req", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Checkpoint write time (s)", "cells": ['
            '{"id": "checkpoint_write_time", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Checkpoint sync time (s)", "cells": ['
            '{"id": "checkpoint_sync_time", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Checkpoint buffers written", "cells": ['
            '{"id": "buffers_checkpoint", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Background buffers written", "cells": ['
            '{"id": "buffers_clean", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Backend buffers written", "cells": ['
            '{"id": "buffers_backend", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Backend fsync count", "cells": ['
            '{"id": "buffers_backend_fsync", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Bgwriter interrupts (too many buffers)", "cells": ['
            '{"id": "maxwritten_clean", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Number of buffers allocated", "cells": ['
            '{"id": "buffers_alloc", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "WAL generated", "cells": ['
            '{"id": "wal_size_pretty", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "Start LSN", "cells": ['
            '{"id": "start_lsn", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "End LSN", "cells": ['
            '{"id": "end_lsn", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "WAL segments archived", "cells": ['
            '{"id": "archived_count", "class": "table_obj_value"}'
        ']}, '
        '{"caption": "WAL segments archive failed", "cells": ['
            '{"id": "failed_count", "class": "table_obj_value"}'
        ']}'
    ']'
  '}]'::jsonb),
(1, 'walstat', 'commonstat', 950, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(1, 'walsthdr', 'walstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat', NULL, NULL),
(1, 'walstrst', 'walstat', 1100, NULL, NULL, 'wal_stats_reset', NULL, NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "wal_stats_reset",'
    '"columns": ['
        '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample"},'
        '{"id": "wal_stats_reset", "class": "table_obj_value", "caption": "WAL stats reset time"}'
    ']}]'::jsonb),
(1, 'walst', 'walstat', 1200, NULL, NULL, 'wal_stats', NULL, NULL, NULL,
  '[{'
    '"type": "column_table",'
    '"class": "stat",'
    '"source": "wal_stats",'
    '"columns": ['
        '{"caption": "Metric"},'
        '{"caption": "Value"}'
    '],'
    '"rows": ['
        '{"caption": "WAL generated", "title": "Total amount of WAL generated", "cells": ['
            '{"id": "wal_bytes_text", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL per second", "title": "Average amount of WAL generated per second", "cells": ['
            '{"id": "wal_bytes_per_sec", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL records", "title": "Total number of WAL records generated", "cells": ['
            '{"id": "wal_records", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL FPI", "title": "Total number of WAL full page images generated", "cells": ['
            '{"id": "wal_fpi", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL buffers full", "title": "Number of times WAL data was written to disk because WAL buffers became full", "cells": ['
            '{"id": "wal_buffers_full", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL writes", "title": "Number of times WAL buffers were written out to disk via XLogWrite request", "cells": ['
            '{"id": "wal_write", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL writes per second", "title": "Average number of times WAL buffers were written out to disk via XLogWrite request per second", "cells": ['
            '{"id": "wal_write_per_sec", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL sync", "title": "Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)", "cells": ['
            '{"id": "wal_sync", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL syncs per second", "title": "Average number of times WAL files were synced to disk via issue_xlog_fsync request per second", "cells": ['
            '{"id": "wal_sync_per_sec", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL write time (s)", "title": "Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync", "cells": ['
            '{"id": "wal_write_time", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL write duty", "title": "WAL write time as a percentage of the report duration time", "cells": ['
            '{"id": "wal_write_time_per_sec", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL sync time (s)", "title": "Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)", "cells": ['
            '{"id": "wal_sync_time", "class": "table_obj_value"}'
        ']},'
        '{"caption": "WAL sync duty", "title": "WAL sync time as a percentage of the report duration time", "cells": ['
            '{"id": "wal_sync_time_per_sec", "class": "table_obj_value"}'
        ']}'
    ']'
  '}]'::jsonb),
(1, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, NULL, 'tablespace_stat', NULL,
  '[{'
      '"type": "row_table",'
      '"source": "tablespace_stats",'
      '"ordering": "tablespacename",'
      '"class": "stat",'
      '"columns": ['
        '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
        '{"caption": "Path", "id": "tablespacepath", "class": "table_obj_value"},'
        '{"caption": "Size", "id": "size", "class": "table_obj_value", '
        '"title": "Tablespace size as it was at the moment of last sample in report interval"},'
        '{"caption": "Growth", "id": "size_delta", "class": "table_obj_value", '
        '"title": "Tablespace size increment during report interval"}'
      ']'
    '}]'::jsonb),
(1, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL, NULL),
(1, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', NULL, 'wait_sampling_total', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "wait_sampling_total_stats",'
      '"ordering": "event_type_order",'
      '"columns": ['
          '{"id": "event_type", "class": "table_obj_name", "caption": "Wait event type"}, '
          '{"id": "stmt_waited", "class": "table_obj_value", "title": "Time, waited in events of wait event type executing statements in seconds", "caption": "Statements Waited (s)"}, '
          '{"id": "stmt_waited_pct", "class": "table_obj_value", "title": "Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements", "caption": "%Total"}, '
          '{"id": "tot_waited", "class": "table_obj_value", "title": "Time, waited in events of wait event type by all backends (including background activity) in seconds", "caption": "All Waited (s)"}, '
          '{"id": "tot_waited_pct", "class": "table_obj_value", "title": "Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)", "caption": "%Total"}'
      ']}]'::jsonb),
(1, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', NULL, 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>',
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "wait_sampling_events",'
      '"filter": {"type": "exists", "field": "stmt_filter"},'
      '"ordering": "-stmt_waited",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "event_type", "class": "table_obj_name", "caption": "Wait event type"}, '
          '{"id": "event", "class": "table_obj_name", "caption": "Wait event"}, '
          '{"id": "stmt_waited", "class": "table_obj_value", "title": "Time, waited in event executing statements in seconds", "caption": "Waited (s)"}, '
          '{"id": "stmt_waited_pct", "class": "table_obj_value", "title": "Time, waited in event as a percentage of total time waited in a cluster executing statements", "caption": "%Total"}'
  ']}]'::jsonb),
(1, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', NULL, 'wt_smp_all', '<p>Top wait events detected in all backends</p>',
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "wait_sampling_events",'
      '"filter": {"type": "exists", "field": "total_filter"},'
      '"ordering": "-tot_waited",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "event_type", "class": "table_obj_name", "caption": "Wait event type"}, '
          '{"id": "event", "class": "table_obj_name", "caption": "Wait event"}, '
          '{"id": "tot_waited", "class": "table_obj_value", "title": "Time, waited in event by all backends (including background activity) in seconds", "caption": "Waited (s)"}, '
          '{"id": "tot_waited_pct", "class": "table_obj_value", "title": "Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)", "caption": "%Total"}'
  ']}]'::jsonb)
;

-- Query section of regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', NULL, 'top_ela', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_total_time",'
      '"filter": {"type": "exists", "field": "total_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "total_time_pct", "class": "table_obj_value", "title": "Elapsed time as a percentage of total cluster elapsed time", "caption": "%Total"}, '
          '{"caption": "Time (s)", "columns": ['
              '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed"}, '
              '{"id": "total_plan_time", "class": "table_obj_value", "title": "Time spent planning statement", "caption": "Plan"}, '
              '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec"}'
              ']}, '
          '{"id": "jit_total_time", "class": "jitTimeCell", "caption": "JIT<br>time (s)", "condition": "statements_jit_stats"}, '
          '{"class": "table_obj_name", "caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": "blk_read_time", "class": "table_obj_value", "title": "Time spent reading blocks by statement", "caption": "Read"}, '
              '{"id": "blk_write_time", "class": "table_obj_value", "title": "Time spent writing blocks by statement", "caption": "Write"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "CPU time (s)", "condition": "kcachestatements", "columns": ['
              '{"id": "user_time", "class": "table_obj_value", "caption": "Usr"}, '
              '{"id": "system_time", "class": "table_obj_value", "caption": "Sys"}'
              ']}, '
          '{"id": "plans", "class": "table_obj_value", "caption": "Plans", "title": "Number of times the statement was planned"}, '
          '{"id": "calls", "class": "table_obj_value", "caption": "Executions", "title": "Number of times the statement was executed"} '
      ']'
  '}]'::jsonb),
(1, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', NULL, 'top_plan', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_plan_time",'
      '"filter": {"type": "exists", "field": "total_plan_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "total_plan_time", "class": "table_obj_value", "title": "Time spent planning statement", "caption": "Plan elapsed (s)"}, '
          '{"id": "plan_time_pct", "class": "table_obj_value", "title": "Plan elapsed as a percentage of statement elapsed time", "caption": "%Elapsed"}, '
          '{"title": "Planning time statistics", "caption": "Plan times (ms)", "columns": ['
              '{"id": "mean_plan_time", "class": "table_obj_value", "caption": "Mean"}, '
              '{"id": "min_plan_time", "class": "table_obj_value", "caption": "Min"}, '
              '{"id": "max_plan_time", "class": "table_obj_value", "caption": "Max"}, '
              '{"id": "stddev_plan_time", "class": "table_obj_value", "caption": "StdErr"}'
          ']}, '
          '{"id": "plans", "class": "table_obj_value", "title": "Number of times the statement was planned", "caption": "Plans"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
          ']'
      '}]'::jsonb),
(1, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, NULL, 'top_exec', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_exec_time",'
      '"filter": {"type": "exists", "field": "total_exec_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "caption": "Query ID", "class": "mono queryId"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec (s)"}, '
          '{"id": "exec_time_pct", "class": "table_obj_value", "title": "Exec time as a percentage of statement elapsed time", "caption": "%Elapsed", "condition": "planning_times"}, '
          '{"id": "total_exec_time_pct", "class": "table_obj_value", "title": "Exec time as a percentage of total cluster elapsed time", "caption": "%Total"}, '
          '{"id": "jit_total_time", "class": "jitTimeCell", "title": "Exec time as a percentage of statement elapsed time", "caption": "JIT time (s)", "condition": "statements_jit_stats"}, '
          '{"caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": "blk_read_time", "class": "table_obj_value", "caption": "Read"}, '
              '{"id": "blk_write_time", "class": "table_obj_value", "caption": "Write"}'
          ']}, '
          '{"caption": "CPU time (s)", "condition": "kcachestatements", "columns": ['
              '{"id": "user_time", "class": "table_obj_value", "caption": "Usr"}, '
              '{"id": "system_time", "class": "table_obj_value", "caption": "Sys"}'
          ']}, '
          '{"id": "rows", "class": "table_obj_value", "caption": "Rows"}, '
          '{"title": "Execution time statistics", "caption": "Execution times (ms)", "columns": ['
              '{"id": "mean_exec_time", "class": "table_obj_value", "caption": "Mean"}, '
              '{"id": "min_exec_time", "class": "table_obj_value", "caption": "Min"}, '
              '{"id": "max_exec_time", "class": "table_obj_value", "caption": "Max"}, '
              '{"id": "stddev_exec_time", "class": "table_obj_value", "caption": "StdErr"}'
          ']}, '
          '{"id": "calls", "title": "Number of times the statement was executed", "caption": "Executions", "class": "table_obj_value"}'
      ']'
  '}]'::jsonb),
(1, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, NULL, 'top_calls', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_calls",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}, '
          '{"id": "calls_pct", "class": "table_obj_value", "title": "Executions of this statement as a percentage of total executions of all statements in a cluster", "caption": "%Total"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "mean_exec_time", "class": "table_obj_value", "caption": "Mean(ms)"}, '
          '{"id": "min_exec_time", "class": "table_obj_value", "caption": "Min(ms)"}, '
          '{"id": "max_exec_time", "class": "table_obj_value", "caption": "Max(ms)"}, '
          '{"id": "stddev_exec_time", "class": "table_obj_value", "caption": "StdErr(ms)"}, '
          '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}'
      ']'
  '}]'::jsonb),
(1, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', NULL, 'top_iowait', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_io_time",'
      '"filter": {"type": "exists", "field": "io_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "io_time", "class": "table_obj_value", "title": "Time spent by the statement reading and writing blocks", "caption": "IO(s)"}, '
          '{"id": "blk_read_time", "class": "table_obj_value", "title": "Time spent by the statement reading blocks", "caption": "R(s)"}, '
          '{"id": "blk_write_time", "class": "table_obj_value", "title": "Time spent by the statement writing blocks", "caption": "W(s)"}, '
          '{"id": "io_time_pct", "class": "table_obj_value", "title": "I/O time of this statement as a percentage of total I/O time for all statements in a cluster", "caption": "%Total"}, '
          '{"title": "Number of blocks read by the statement", "caption": "Reads", "columns": ['
              '{"id": "shared_blks_read", "title": "Number of shared blocks read by the statement", "caption": "Shr", "class": "table_obj_value"}, '
              '{"id": "local_blks_read", "title": "Number of local blocks read by the statement (usually used for temporary tables)", "caption": "Loc", "class": "table_obj_value"}, '
              '{"id": "temp_blks_read", "title": "Number of temp blocks read by the statement (usually used for operations like sorts and joins)", "caption": "Tmp", "class": "table_obj_value"}'
          ']}, '
          '{"title": "Number of blocks written by the statement", "caption": "Writes", "columns": ['
              '{"id": "shared_blks_written", "title": "Number of shared blocks written by the statement", "caption": "Shr", "class": "table_obj_value"}, '
              '{"id": "local_blks_written", "title": "Number of local blocks written by the statement (usually used for temporary tables)", "caption": "Loc", "class": "table_obj_value"}, '
              '{"id": "temp_blks_written", "title": "Number of temp blocks written by the statement (usually used for operations like sorts and joins)", "caption": "Tmp", "class": "table_obj_value"}'
          ']}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of blocks written by the statement", "caption": "Executions"}'
          ']}]'::jsonb),
(1, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, NULL, 'top_pgs_fetched', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"ordering": "ord_shared_blocks_fetched",'
      '"filter": {"type": "exists", "field": "shared_blks_fetched"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "shared_blks_fetched", "class": "table_obj_value", "title": "Shared blocks fetched (read and hit) by the statement", "caption": "blks fetched"}, '
          '{"id": "shared_blks_fetched_pct", "class": "table_obj_value", "title": "Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster", "caption": "%Total"}, '
          '{"id": "shared_hit_pct", "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']}]'::jsonb),
(1, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, NULL, 'top_shared_reads', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "shared_blks_read"},'
      '"ordering": "ord_shared_blocks_read",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "shared_blks_read", "class": "table_obj_value", "title": "Total number of shared blocks read by the statement", "caption": "Reads"}, '
          '{"id": "read_pct", "class": "table_obj_value", "title": "Shared blocks read by this statement as a percentage of all shared blocks read in a cluster", "caption": "%Total"}, '
          '{"id": "shared_hit_pct", "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']}]'::jsonb),
(1, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, NULL, 'top_shared_dirtied', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat",'
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "shared_blks_dirtied"},'
      '"ordering": "ord_shared_blocks_dirt",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "shared_blks_dirtied", "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Dirtied"}, '
          '{"id": "shared_blks_dirtied", "class": "table_obj_value", "title": "Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster", "caption": "%Total"}, '
          '{"id": "shared_hit_pct", "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Hits(%)"}, '
          '{"id": "dirtied_pct", "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Dirtied"}, '
          '{"id": "wal_bytes_fmt", "class": "table_obj_value", "title": "Total amount of WAL bytes generated by the statement", "caption": "WAL", "condition": "statement_wal_bytes"}, '
          '{"id": "wal_bytes_pct", "class": "table_obj_value", "title": "WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster", "caption": "%Total", "condition": "statement_wal_bytes"}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(1, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, NULL, 'top_shared_written', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "shared_blks_written"},'
      '"ordering": "ord_shared_blocks_written",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "shared_blks_written", "class": "table_obj_value", "title": "Total number of shared blocks written by the statement", "caption": "Written"}, '
          '{"id": "tot_written_pct", "class": "table_obj_value", "title": "Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)", "caption": "%Total"}, '
          '{"id": "backend_written_pct", "class": "table_obj_value", "title": "Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend of pg_stat_bgwriter view)", "caption": "%BackendW"}, '
          '{"id": "shared_hit_pct", "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(1, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', NULL, 'top_wal_bytes', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "wal_bytes"},'
      '"ordering": "ord_wal",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "wal_bytes_fmt", "class": "table_obj_value", "title": "Total amount of WAL bytes generated by the statement", "caption": "WAL"}, '
          '{"id": "wal_bytes_pct", "class": "table_obj_value", "title": "WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster", "caption": "%Total"}, '
          '{"id": "shared_blks_dirtied", "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Dirtied"}, '
          '{"id": "wal_fpi", "class": "table_obj_value", "title": "Total number of WAL full page images generated by the statement", "caption": "WAL FPI"}, '
          '{"id": "wal_records", "class": "table_obj_value", "title": "Total number of WAL records generated by the statement", "caption": "WAL records"}'
      ']'
  '}]'::jsonb),
(1, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', 'statements_top_temp', NULL, 'top_temp', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "sum_tmp_blks"},'
      '"ordering": "ord_temp",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "local_blks_fetched", "class": "table_obj_value", "title": "Number of local blocks fetched (hit + read)", "caption": "Local fetched"}, '
          '{"id": "local_hit_pct", "class": "table_obj_value", "title": "Local blocks hit percentage", "caption": "Hits(%)"}, '
          '{"title": "Number of blocks, used for temporary tables", "caption": "Local (blk)", "columns": ['
              '{"id": "local_blks_written", "class": "table_obj_value", "title": "Number of written local blocks", "caption": "Write"}, '
              '{"id": "local_write_total_pct", "class": "table_obj_value", "title": "Percentage of all local blocks written", "caption": "%Total"}, '
              '{"id": "local_blks_read", "class": "table_obj_value", "title": "Number of read local blocks", "caption": "Read"}, '
              '{"id": "local_read_total_pct", "class": "table_obj_value", "title": "Percentage of all local blocks read", "caption": "%Total"}'
          ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": "temp_blks_written", "class": "table_obj_value", "title": "Number of written temp blocks", "caption": "Write"}, '
              '{"id": "temp_write_total_pct", "class": "table_obj_value", "title": "Percentage of all temp blocks written", "caption": "%Total"}, '
              '{"id": "temp_blks_read", "class": "table_obj_value", "title": "Number of read temp blocks", "caption": "Read"}, '
              '{"id": "temp_read_total_pct", "class": "table_obj_value", "title": "Percentage of all temp blocks read", "caption": "%Total"}'
          ']}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(1, 'sqltmpiotime', 'sqlsthdr', 1125, 'Top SQL by temp I/O time', 'Top SQL by temp I/O time', 'statements_temp_io_times', NULL, 'top_temp_io_time', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_temp_io_time"},'
      '"ordering": "ord_temp_io_time",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"title": "Time the statement spent on temporary file blocks I/O", "caption": "Temp I/O time (s)", "columns": ['
              '{"id": "temp_blk_read_time", "class": "table_obj_value", "title": "Time the statement spent reading temporary file blocks, in seconds", "caption": "Read"}, '
              '{"id": "temp_blk_write_time", "class": "table_obj_value", "title": "Time the statement spent reading temporary file blocks, in seconds", "caption": "Write"}, '
              '{"id": "temp_io_time_pct", "class": "table_obj_value", "title": "Time spent on temporary file blocks I/O of this statement as a percentage of total time spent on temporary file blocks I/O by all statements", "caption": "%Total"} '
          ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": "temp_blks_written", "class": "table_obj_value", "title": "Number of written temp blocks", "caption": "Write"}, '
              '{"id": "temp_write_total_pct", "class": "table_obj_value", "title": "Percentage of all temp blocks written", "caption": "%Total"}, '
              '{"id": "temp_blks_read", "class": "table_obj_value", "title": "Number of read temp blocks", "caption": "Read"}, '
              '{"id": "temp_read_total_pct", "class": "table_obj_value", "title": "Percentage of all temp blocks read", "caption": "%Total"}'
          ']}, '
          '{"id": "total_time", "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": "rows", "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": "calls", "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(1, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', NULL, 'top_jit', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "sum_jit_time"},'
      '"ordering": "ord_jit",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono jitCellId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"id": "jit_total_time", "class": "table_obj_value", "title": "Time spent on JIT in seconds", "caption": "JIT total (s)"}, '
          '{"caption": "Generation", "columns": ['
              '{"id": "jit_functions", "class": "table_obj_value", "title": "Total number of functions JIT-compiled by the statement.", "caption": "Count"}, '
              '{"id": "jit_generation_time", "class": "table_obj_value", "title": "Time spent by the statement on generating JIT code, in seconds.", "caption": "Time (s)"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "Inlining", "columns": ['
              '{"id": "jit_inlining_count", "class": "table_obj_value", "title": "Number of times functions have been inlined.", "caption": "Count"}, '
              '{"id": "jit_inlining_time", "class": "table_obj_value", "title": "Time spent by the statement on inlining functions, in seconds.", "caption": "Time (s)"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "Optimization", "columns": ['
              '{"id": "jit_optimization_count", "class": "table_obj_value", "title": "Number of times the statement has been optimized.", "caption": "Count"}, '
              '{"id": "jit_optimization_time", "class": "table_obj_value", "title": "Time spent by the statement on optimizing, in seconds.", "caption": "Time (s)"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "Emission", "columns": ['
              '{"id": "jit_emission_count", "class": "table_obj_value", "title": "Number of times code has been emitted.", "caption": "Count"}, '
              '{"id": "jit_emission_time", "class": "table_obj_value", "title": "Total time spent by the statement on emitting code, in seconds.", "caption": "Time (s)"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "Time (s)", "columns": ['
              '{"id": "total_plan_time", "class": "table_obj_value", "title": "Time spent planning statement", "condition": "planning_times", "caption": "Plan"}, '
              '{"id": "total_exec_time", "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": "blk_read_time", "class": "table_obj_value", "title": "Time spent reading blocks by statement", "caption": "Read"}, '
              '{"id": "blk_write_time", "class": "table_obj_value", "title": "Time spent writing blocks by statement", "caption": "Write"}'
          ']}'
      ']}]'::jsonb),
(1, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL, NULL),
(1, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, NULL, 'kcache_time', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stat", '
      '"source": "top_rusage_statements",'
      '"filter": {"type": "exists", "field": "sum_cpu_time"},'
      '"ordering": "ord_cpu_time",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"}, '
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
          '{"id": "username", "class": "table_obj_name", "caption": "User"}, '
          '{"title": "Userspace CPU", "caption": "User Time", "columns": ['
              '{"id": "plan_user_time", "class": "table_obj_value", "title": "User CPU time elapsed during planning", "caption": "Plan (s)", "condition": "rusage_planstats"}, '
              '{"id": "exec_user_time", "class": "table_obj_value", "title": "User CPU time elapsed during execution", "caption": "Exec (s)"}, '
              '{"id": "user_time_pct", "class": "table_obj_value", "title": "User CPU time elapsed by this statement as a percentage of total user CPU time", "caption": "%Total"}'
              ']}, '
          '{"title": "Kernelspace CPU", "caption": "System Time", "columns": ['
              '{"id": "plan_system_time", "class": "table_obj_value", "title": "System CPU time elapsed during planning", "caption": "Plan (s)", "condition": "rusage_planstats"}, '
              '{"id": "exec_system_time", "class": "table_obj_value", "title": "System CPU time elapsed during execution", "caption":"Exec (s)"}, '
              '{"id": "system_time_pct", "class": "table_obj_value", "title": "System CPU time elapsed by this statement as a percentage of total system CPU time", "caption": "%Total"}'
              ']}'
          ']'
      '}]'::jsonb),
(1, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, NULL, 'kcache_reads_writes', NULL,
  '[{'
      '"type": "row_table",'
      '"class": "stat",'
      '"source": "top_rusage_statements",'
      '"filter": {"type": "exists", "field": "sum_io_bytes"},'
      '"ordering": "ord_io_bytes",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryId", "caption": "Query ID"},'
          '{"id": "dbname", "class": "table_obj_name", "caption": "Database"},'
          '{"id": "username", "class": "table_obj_name", "caption": "User"},'
          '{"title": "Filesystem reads", "caption": "Read Bytes", "columns": ['
              '{"id": "plan_reads", "class": "table_obj_value", "title": "Filesystem read amount during planning", "caption": "Plan", "condition": "rusage_planstats"},'
              '{"id": "exec_reads", "class": "table_obj_value", "title": "Filesystem read amount during execution", "caption": "Bytes"},'
              '{"id": "reads_total_pct", "class": "table_obj_value", "title": "Filesystem read amount of this statement as a percentage of all statements FS read amount", "caption": "%Total"}'
          ']},'
          '{"title": "Filesystem writes", "caption": "Write Bytes", "columns": ['
              '{"id": "plan_writes", "class": "table_obj_value", "title": "Filesystem write amount during planning", "caption": "Plan", "condition": "rusage_planstats"},'
              '{"id": "exec_writes", "class": "table_obj_value", "title": "Filesystem write amount during execution", "caption": "Bytes"},'
              '{"id": "writes_total_pct", "class": "table_obj_value", "title": "Filesystem write amount of this statement as a percentage of all statements FS read amount", "caption": "%Total"}'
          ']}'
      ']'
  '}]'::jsonb),
(1, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, NULL, 'sql_list', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stmtlist", '
      '"source": "queries",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryTextId", "caption": "Query ID", "rowspan": true}, '
          '{"id": ["query_text1", "query_text2", "query_text3"], "class": "mono queryText", "caption": "Query Text"}'
      ']'
  '}]'::jsonb)
;

-- Schema objects section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, NULL, 'scanned_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "toast",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_seq_scan"},'
    '"ordering": "ord_seq_scan",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": ["relname", "toastrelname"], "class": "table_obj_name"},'
     '{"caption": "~SeqBytes", "id": ["seqscan_bytes_pretty", "t_seqscan_bytes_pretty"], '
       '"class": "table_obj_value", '
       '"title": "Estimated number of bytes, fetched by sequential scans"}, '
     '{"caption": "SeqScan", "id": ["seq_scan", "toastseq_scan"], '
       '"class": "table_obj_value", '
       '"title": "Number of sequential scans initiated on this table"},'
     '{"caption": "IxScan", "id": ["idx_scan", "toastidx_scan"], '
       '"class": "table_obj_value", '
       '"title": "Number of index scans initiated on this table"},'
     '{"caption": "IxFet", "id": ["idx_tup_fetch", "toastidx_tup_fetch"], '
       '"class": "table_obj_value", '
       '"title": "Number of live rows fetched by index scans"},'
     '{"caption": "Ins", "id": ["n_tup_ins", "toastn_tup_ins"], '
       '"class": "table_obj_value", '
       '"title": "Number of rows inserted"},'
     '{"caption": "Upd", "id": ["n_tup_upd", "toastn_tup_upd"], '
       '"class": "table_obj_value", '
       '"title": "Number of rows updated (includes HOT updated rows)"},'
     '{"caption": "Del", "id": ["n_tup_del", "toastn_tup_del"], '
       '"class": "table_obj_value", '
       '"title": "Number of rows deleted"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd", "toastn_tup_hot_upd"], '
       '"class": "table_obj_value", '
       '"title": "Number of rows HOT updated (i.e., with no separate index update required)"}'
    ']'
  '}]'::jsonb),
(1, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, NULL, 'fetch_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_io_tables",'
    '"filter": {"type": "exists", "field": "ord_fetch"},'
    '"ordering": "ord_fetch",'
    '"limit": "topn",'
    '"columns": ['
    '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
    '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
    '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
    '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
    '{"caption": "Heap", "columns": ['
         '{"caption": "Blks", "id": "heap_blks_fetch", "title": "Number of blocks fetched (read+hit) from this table", "class": "table_obj_value"},'
         '{"caption": "%Total", "id": "heap_blks_proc_pct", "title": "Heap blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']},'
    '{"caption": "Ix", "columns": ['
         '{"caption": "Blks", "id": "idx_blks_fetch", "title": "Number of blocks fetched (read+hit) from all indexes on this table", "class": "table_obj_value"},'
         '{"caption": "%Total", "id": "idx_blks_fetch_pct", "title": "Indexes of blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']},'
    '{"caption": "TOAST", "columns": ['
         '{"caption": "Blks", "id": "toast_blks_fetch", "title": "Number of blocks fetched (read+hit) from this table''s TOAST table (if any)", "class": "table_obj_value"},'
         '{"caption": "%Total", "id": "toast_blks_fetch_pct", "title": "TOAST blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']},'
    '{"caption": "TOAST-Ix", "columns": ['
         '{"caption": "Blks", "id": "tidx_blks_fetch", "title": "Number of blocks fetched (read+hit) from this table''s TOAST table indexes (if any)", "class": "table_obj_value"},'
         '{"caption": "%Total", "id": "tidx_blks_fetch_pct", "title": "TOAST table index blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']}'
   ']'
  '}]'::jsonb),
(1, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, NULL, 'read_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_io_tables",'
    '"filter": {"type": "exists", "field": "ord_read"},'
    '"ordering": "ord_read",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Heap", "columns": ['
          '{"caption": "Blks", "id": "heap_blks_read", "title": "Number of blocks read from this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": "heap_blks_read_pct", "title": "Heap blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Ix", "columns": ['
          '{"caption": "Blks", "id": "idx_blks_read", "title": "Number of blocks read from all indexes on this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": "idx_blks_read_pct", "title": "Indexes of blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST", "columns": ['
          '{"caption": "Blks", "id": "toast_blks_read", "title": "Number of blocks read from this table''s TOAST table (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": "toast_blks_read_pct", "title": "TOAST blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST-Ix", "columns": ['
          '{"caption": "Blks", "id": "tidx_blks_read", "title": "Number of blocks read from this table''s TOAST table indexes (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": "tidx_blks_read_pct", "title": "TOAST table index blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Hit(%)", "id": "hit_pct", "class": "table_obj_value", "title": "Number of heap, indexes, toast and toast index blocks fetched from shared buffers as a percentage of all their blocks fetched from shared buffers and file system"}'
    ']'
  '}]'::jsonb),
(1, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, NULL, 'dml_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "toast",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_dml"},'
    '"ordering": "ord_dml",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": ["relname", "toastrelname"], "class": "table_obj_name"},'
     '{"caption": "Ins", "id": ["n_tup_ins", "toastn_tup_ins"], "title": "Number of rows inserted", "class": "table_obj_value"},'
     '{"caption": "Upd", "id": ["n_tup_upd", "toastn_tup_upd"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Del", "id": ["n_tup_del", "toastn_tup_del"], "title": "Number of rows deleted", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd", "toastn_tup_hot_upd"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"},'
     '{"caption": "SeqScan", "id": ["seq_scan", "toastseq_scan"], "title": "Number of live rows fetched by sequential scans", "class": "table_obj_value"},'
     '{"caption": "SeqFet", "id": ["seq_tup_read", "toastseq_tup_read"], "title": "Number of live rows fetched by sequential scans", "class": "table_obj_value"},'
     '{"caption": "IxScan", "id": ["idx_scan", "toastidx_scan"], "title": "Number of index scans initiated on this table", "class": "table_obj_value"},'
     '{"caption": "IxFet", "id": ["idx_tup_fetch", "toastidx_tup_fetch"], "title": "Number of live rows fetched by index scans", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, NULL, 'upd_del_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "toast",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_upd"},'
    '"ordering": "ord_upd",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": ["relname", "toastrelname"], "class": "table_obj_name"},'
     '{"caption": "Upd", "id": ["n_tup_upd", "toastn_tup_upd"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd", "toastn_tup_hot_upd"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"},'
     '{"caption": "Del", "id": ["n_tup_del", "toastn_tup_del"], "title": "Number of rows deleted", "class": "table_obj_value"},'
     '{"caption": "Vacuum", "id": ["vacuum_count", "toastvacuum_count"], "title": "Number of times this table has been manually vacuumed (not counting VACUUM FULL)", "class": "table_obj_value"},'
     '{"caption": "AutoVacuum", "id": ["autovacuum_count", "toastautovacuum_count"], "title": "Number of times this table has been vacuumed by the autovacuum daemon", "class": "table_obj_value"},'
     '{"caption": "Analyze", "id": ["analyze_count", "toastanalyze_count"], "title": "Number of times this table has been manually analyzed", "class": "table_obj_value"},'
     '{"caption": "AutoAnalyze", "id": ["autoanalyze_count", "toastautoanalyze_count"], "title": "Number of times this table has been analyzed by the autovacuum daemon", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'tblupd_np', 'objects', 550, 'Top tables by new-page updated tuples', 'Top tables by new-page updated tuples', 'table_new_page_updates', NULL, 'upd_np_tbl',
  '<p>Top tables by number of rows updated where the successor version goes onto a new heap page, '
  'leaving behind an original version with a <i>t_ctid</i> field that points to a different heap page. '
  'These are always non-HOT updates.</p>',
  '[{'
    '"type": "row_table",'
    '"class": "toast",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_upd_np"},'
    '"ordering": "ord_upd_np",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": ["relname", "toastrelname"], "class": "table_obj_name"},'
     '{"caption": "NP Upd", "id": ["n_tup_newpage_upd", "toastn_tup_newpage_upd"], "title": "Number of rows updated to a new heap page", "class": "table_obj_value"},'
     '{"caption": "%Upd", "id": ["np_upd_pct", "toastnp_upd_pct"], "title": "Number of new-page updated rows as a percentage of all rows updated", "class": "table_obj_value"},'
     '{"caption": "Upd", "id": ["n_tup_upd", "toastn_tup_upd"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd", "toastn_tup_hot_upd"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, NULL, 'growth_tbl',
  '<ul><li>Sizes in square brackets are based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "toast",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_growth"},'
    '"ordering": "ord_growth",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": ["relname", "toastrelname"], "class": "table_obj_name"},'
     '{"caption": "Size", "id": ["relsize_pretty", "t_relsize_pretty"], "title": "Table size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
     '{"caption": "Growth", "id": ["growth_pretty", "toastgrowth_pretty"], "title": "Table size increment during report interval", "class": "table_obj_value"},'
     '{"caption": "Ins", "id": ["n_tup_ins", "toastn_tup_ins"], "title": "Number of rows inserted", "class": "table_obj_value"},'
     '{"caption": "Upd", "id": ["n_tup_upd", "toastn_tup_upd"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Del", "id": ["n_tup_del", "toastn_tup_del"], "title": "Number of rows deleted", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd", "toastn_tup_hot_upd"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, NULL, 'fetch_idx', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_io_indexes",'
    '"filter": {"type": "exists", "field": "ord_fetch"},'
    '"ordering": "ord_fetch",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Index", "id": "indexrelname", "class": "table_obj_name"},'
     '{"caption": "Scans", "id": "idx_scan", "title": "Number of scans performed on index", "class": "table_obj_value"},'
     '{"caption": "Blks", "id": "idx_blks_fetch", "title": "Number of blocks fetched (read+hit) from this index", "class": "table_obj_value"},'
     '{"caption": "%Total", "id": "idx_blks_fetch_pct", "title": "Blocks fetched from this index as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, NULL, 'read_idx', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_io_indexes",'
    '"filter": {"type": "exists", "field": "ord_read"},'
    '"ordering": "ord_read",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Index", "id": "indexrelname", "class": "table_obj_name"},'
     '{"caption": "Scans", "id": "idx_scan", "title": "Number of scans performed on index", "class": "table_obj_value"},'
     '{"caption": "Blks Reads", "id": "idx_blks_read", "title": "Number of disk blocks read from this index", "class": "table_obj_value"},'
     '{"caption": "%Total", "id": "idx_blks_read_pct", "title": "Blocks fetched from this index as a percentage of all blocks read in a cluster", "class": "table_obj_value"}, '
     '{"caption": "Hits(%)", "id": "idx_blks_hit_pct", "title": "Index blocks buffer cache hit percentage", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(1, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, NULL, 'growth_idx',
  '<ul><li>Sizes in square brackets are based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_indexes",'
    '"filter": {"type": "exists", "field": "ord_growth"},'
    '"ordering": "ord_growth",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Index", "id": "indexrelname", "class": "table_obj_name"},'
     '{"caption": "Index", "columns": ['
          '{"id": "indexrelsize_pretty", "caption": "Size", "title": "Index size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
          '{"id": "growth_pretty", "caption": "Growth", "title": "Index size increment during report interval", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Table", "columns": ['
          '{"id": "tbl_n_tup_ins", "caption": "Ins", "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"id": "tbl_n_tup_upd", "caption": "Upd", "title": "Number of rows updated (without HOT updated rows)", "class": "table_obj_value"},'
          '{"id": "tbl_n_tup_del", "caption": "Del", "title": "Number of rows deleted", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb),
(1, 'ixunused', 'objects', 1000, 'Unused indexes', 'Unused indexes', NULL, NULL, 'ix_unused',
  '<p>This table contains non-scanned indexes (during report interval), ordered by number of DML '
  'operations on underlying tables. Constraint indexes are excluded.</p>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_indexes",'
    '"filter": {"type": "exists", "field": "ord_unused"},'
    '"ordering": "ord_unused",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Index", "id": "indexrelname", "class": "table_obj_name"},'
     '{"caption": "Index", "columns": ['
          '{"id": "indexrelsize_pretty", "caption": "Size", "title": "Index size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
          '{"id": "growth_pretty", "caption": "Growth", "title": "Index size increment during report interval", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Table", "columns": ['
          '{"id": "tbl_n_tup_ins", "caption": "Ins", "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"id": "tbl_n_tup_upd", "caption": "Upd", "title": "Number of rows updated (without HOT updated rows)", "class": "table_obj_value"},'
          '{"id": "tbl_n_tup_del", "caption": "Del", "title": "Number of rows deleted", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb)
;

-- Functions section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, NULL, 'funcs_time_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_time"},'
    '"ordering": "ord_time",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Function", "id": "funcname", "class": "table_obj_name"},'
     '{"caption": "Executions", "id": "calls", "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": "total_time", "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": "self_time", "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": "m_time", "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": "m_stime", "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb),
(1, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, NULL, 'funcs_calls_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_calls"},'
    '"ordering": "ord_calls",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Function", "id": "funcname", "class": "table_obj_name"},'
     '{"caption": "Executions", "id": "calls", "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": "total_time", "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": "self_time", "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": "m_time", "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": "m_stime", "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb),
(1, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', NULL, 'trg_funcs_time_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_trgtime"},'
    '"ordering": "ord_trgtime",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr"},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr"},'
     '{"caption": "Function", "id": "funcname", "class": "hdr"},'
     '{"caption": "Executions", "id": "calls", "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": "total_time", "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": "self_time", "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": "m_time", "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": "m_stime", "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(1, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, NULL, 'top_vacuum_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_vac"},'
    '"ordering": "ord_vac",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Vacuum count", "id": "vacuum_count", "class": "table_obj_value", "title": "Number of times this table has been manually vacuumed (not counting VACUUM FULL)"},'
     '{"caption": "Autovacuum count", "id": "autovacuum_count", "class": "table_obj_value", "title": "Number of times this table has been vacuumed by the autovacuum daemon"},'
     '{"caption": "Ins", "id": "n_tup_ins", "class": "table_obj_value", "title": "Number of rows inserted"},'
     '{"caption": "Upd", "id": "n_tup_upd", "class": "table_obj_value", "title": "Number of rows updated (includes HOT updated rows)"},'
     '{"caption": "Del", "id": "n_tup_del", "class": "table_obj_value", "title": "Number of rows deleted"},'
     '{"caption": "Upd(HOT)", "id": "n_tup_hot_upd", "class": "table_obj_value", "title": "Number of rows HOT updated (i.e., with no separate index update required)"}'
    ']'
  '}]'::jsonb),
(1, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, NULL, 'top_analyze_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_anl"},'
    '"ordering": "ord_anl",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Analyze count", "id": "analyze_count", "class": "table_obj_value", "title": "Number of times this table has been manually analyzed"},'
     '{"caption": "Autoanalyze count", "id": "autoanalyze_count", "class": "table_obj_value", "title": "Number of times this table has been analyzed by the autovacuum daemon"},'
     '{"caption": "Ins", "id": "n_tup_ins", "class": "table_obj_value", "title": "Number of rows inserted"},'
     '{"caption": "Upd", "id": "n_tup_upd", "class": "table_obj_value", "title": "Number of rows updated (includes HOT updated rows)"},'
     '{"caption": "Del", "id": "n_tup_del", "class": "table_obj_value", "title": "Number of rows deleted"},'
     '{"caption": "Upd(HOT)", "id": "n_tup_hot_upd", "class": "table_obj_value", "title": "Number of rows HOT updated (i.e., with no separate index update required)"}'
    ']}]'::jsonb),
(1, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, NULL, 'top_ix_vacuum_bytes_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_indexes",'
    '"filter": {"type": "exists", "field": "ord_vac"},'
    '"ordering": "ord_vac",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Index", "id": "indexrelname", "class": "table_obj_name"},'
     '{"id": "vacuum_bytes_pretty", "caption": "Vacuum bytes", "class": "table_obj_value", "title": "Estimated implicit vacuum load caused by table indexes"},'
     '{"id": "vacuum_count", "caption": "Vacuum cnt", "class": "table_obj_value", "title": "Vacuum count on underlying table"},'
     '{"id": "autovacuum_count", "caption": "Autovacuum cnt", "class": "table_obj_value", "title": "Autovacuum count on underlying table"},'
     '{"id": "avg_indexrelsize_pretty", "caption": "IX size", "class": "table_obj_value", "title": "Average index size during report interval"},'
     '{"id": "avg_relsize_pretty", "caption": "Relsize", "class": "table_obj_value", "title": "Average relation size during report interval"}'
    ']'
  '}]'::jsonb),
(1, 'tblbydead', 'vachdr', 400, 'Top tables by dead tuples ratio', 'Top tables by dead tuples ratio', 'top_tables_dead', NULL, 'dead_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_tbl_last_sample",'
    '"filter": {"type": "exists", "field": "ord_dead"},'
    '"ordering": "ord_dead",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Live", "id": "n_live_tup", "class": "table_obj_value", "title": "Estimated number of live rows"},'
     '{"caption": "Dead", "id": "n_dead_tup", "class": "table_obj_value", "title": "Estimated number of dead rows"},'
     '{"caption": "%Dead", "id": "dead_pct", "class": "table_obj_value", "title": "Dead rows count as a percentage of total rows count"},'
     '{"caption": "Last AV", "id": "last_autovacuum", "class": "table_obj_value", "title": "Last autovacuum ran time"},'
     '{"caption": "Size", "id": "relsize_pretty", "class": "table_obj_value", "title": "Table size without indexes and TOAST"}'
    ']'
  '}]'::jsonb),
(1, 'tblbymod', 'vachdr', 500, 'Top tables by modified tuples ratio', 'Top tables by modified tuples ratio', 'top_tables_mods', NULL, 'mod_tbl',
  '<p>Data in this section is not differential. This data is valid for last report sample only.</p>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "top_tbl_last_sample",'
    '"filter": {"type": "exists", "field": "ord_mod"},'
    '"ordering": "ord_mod",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "table_obj_name"},'
     '{"caption": "Schema", "id": "schemaname", "class": "table_obj_name"},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "table_obj_name"},'
     '{"caption": "Table", "id": "relname", "class": "table_obj_name"},'
     '{"caption": "Live", "id": "n_live_tup", "class": "table_obj_value", "title": "Estimated number of live rows"},'
     '{"caption": "Dead", "id": "n_dead_tup", "class": "table_obj_value", "title": "Estimated number of dead rows"},'
     '{"caption": "Mod", "id": "n_mod_since_analyze", "class": "table_obj_value", "title": "Estimated number of rows modified since this table was last analyzed"},'
     '{"caption": "%Mod", "id": "mods_pct", "class": "table_obj_value", "title": "Modified rows of the table as a percentage of all rows in the table"},'
     '{"caption": "Last AA", "id": "last_autoanalyze", "class": "table_obj_value", "title": "Last autoanalyze ran time"},'
     '{"caption": "Size", "id": "relsize_pretty", "class": "table_obj_value", "title": "Table size without indexes and TOAST"}'
    ']'
  '}]'::jsonb)
;

-- Settings sections
INSERT INTO report_struct(
    report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
    content, sect_struct)
VALUES
(1, 'definedset', 'settings', 800, NULL, NULL, NULL, NULL, NULL, NULL,
  '[{'
    '"type": "row_table",'
    '"class": "setlist",'
    '"source": "settings",'
    '"filter": {"type": "exists", "field": "defined_val"},'
    '"columns": ['
     '{"caption": "Defined settings", "columns": ['
          '{"caption": "Setting", "id": "name", "class": "table_obj_value"},'
          '{"caption": "reset_val", "id": "reset_val", "class": "table_obj_value switch_bold"},'
          '{"caption": "Unit", "id": "unit", "class": "table_obj_value"},'
          '{"caption": "Source", "id": "source", "class": "table_obj_value"},'
          '{"caption": "Notes", "id": "notes", "class": "table_obj_value switch_bold"}'
     ']}'
    ']'
  '},'
  '{'
    '"type": "row_table",'
    '"class": "setlist",'
    '"source": "settings",'
    '"filter": {"type": "exists", "field": "default_val"},'
    '"columns": ['
     '{"caption": "Default settings", "columns": ['
          '{"caption": "Setting", "id": "name", "class": "table_obj_value"},'
          '{"caption": "reset_val", "id": "reset_val", "class": "table_obj_value switch_bold"},'
          '{"caption": "Unit", "id": "unit", "class": "table_obj_value"},'
          '{"caption": "Source", "id": "source", "class": "table_obj_value"},'
          '{"caption": "Notes", "id": "notes", "class": "table_obj_value switch_bold"}'
     ']}'
    ']'
  '}]'::jsonb)
;

-- Schema objects section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(2, 'stmt_cmt1', NULL, 100, NULL, NULL, 'stmt_cnt_range', NULL, NULL, '<p><strong>Warning!</strong></p>'
  '<p>Report interval contains sample(s) with captured statements count more than 90% of '
  '<i>pg_stat_statements.max</i> parameter.</p>'
  '{func_output}'
  '<p> Consider increasing <i>pg_stat_statements.max</i> parameter.</p>',
  '[{'
    '"type": "row_table", '
    '"source": "stmt_cnt_range",'
    '"ordering": "ord",'
    '"class": "stat", '
    '"columns": ['
        '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample ID"}, '
        '{"id": "sample_time", "class": "table_obj_value", "caption": "Sample Time"}, '
        '{"id": "stmt_cnt", "class": "table_obj_value", "caption": "Stmts Captured"}, '
        '{"id": "max_cnt", "class": "table_obj_value", "caption": "pg_stat_statements.max"}'
    ']}]'::jsonb),
(2, 'srvstat', NULL, 300, 'Server statistics', 'Server statistics', NULL, NULL, 'cl_stat', NULL, NULL),
(2, 'sqlsthdr', NULL, 400, 'SQL query statistics', 'SQL query statistics', 'statstatements', NULL, 'sql_stat', NULL, NULL),
(2, 'objects', NULL, 500, 'Schema object statistics', 'Schema object statistics', NULL, NULL, 'schema_stat', NULL, NULL),
(2, 'funchdr', NULL, 600, 'User function statistics', 'User function statistics', 'function_stats', NULL, 'func_stat', NULL, NULL),
(2, 'vachdr', NULL, 700, 'Vacuum-related statistics', 'Vacuum-related statistics', NULL, NULL, 'vacuum_stats', NULL, NULL),
(2, 'settings', NULL, 800, 'Cluster settings during the report interval', 'Cluster settings during the report interval', NULL, NULL, 'pg_settings', NULL, NULL),
(2, 'stmt_warn', NULL, 900, NULL, 'Warning!', 'stmt_cnt_all', NULL, 'stmt_warn', NULL,
  '[{'
    '"type": "row_table", '
    '"source": "stmt_cnt_all",'
    '"ordering": "sample_id",'
    '"class": "stat", '
    '"columns": ['
        '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample ID"}, '
        '{"id": "sample_time", "class": "table_obj_value", "caption": "Sample Time"}, '
        '{"id": "stmt_cnt", "class": "table_obj_value", "caption": "Stmts Captured"}, '
        '{"id": "max_cnt", "class": "table_obj_value", "caption": "pg_stat_statements.max"}'
    ']}]'::jsonb)
;


-- Server section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href, content,
  sect_struct)
VALUES
(2, 'dbstat', 'srvstat', 100, 'Database statistics', 'Database statistics', NULL, NULL, 'db_stat', NULL, NULL),
(2, 'dbstatreset', 'dbstat', 200, NULL, NULL, 'dbstats_reset', NULL, NULL,
  '<p><b>Warning!</b> Database statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Statistics for listed databases and contained objects might be affected</p>',
  '[{'
        '"type": "row_table", '
        '"class": "stat", '
        '"source": "dbstats_reset", '
        '"ordering": "sample_id",'
        '"columns": ['
            '{"id": "interval_num", "class": "table_obj_value", "caption": "I"}, '
            '{"id": "dbname", "class": "table_obj_name", "caption": "Database"}, '
            '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample"}, '
            '{"id": "stats_reset", "class": "table_obj_value", "caption": "Reset time"}'
        ']'
  '}]'::jsonb),
(2, 'dbstatmain', 'dbstat', 300, NULL, NULL, NULL, NULL, NULL, NULL,
'[{'
    '"type": "row_table", '
    '"class": "diff", '
    '"source": "dbstat", '
    '"ordering": "ord_db",'
    '"columns": ['
        '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
        '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"}, '
        '{"caption": "Transactions", "columns": ['
            '{"id": ["xact_commit1", "xact_commit2"], "class": "table_obj_value", "title": "Number of transactions in this database that have been committed", "caption": "Commits"}, '
            '{"id": ["xact_rollback1", "xact_rollback2"], "class": "table_obj_value", "title": "Number of transactions in this database that have been rolled back", "caption": "Rollbacks"}, '
            '{"id": ["deadlocks1", "deadlocks2"], "class": "table_obj_value", "title": "Number of deadlocks detected in this database", "caption": "Deadlocks"}'
        ']}, '
        '{"caption": "Checksums", "condition": "checksum_fail_detected", "columns": ['
            '{"id": ["checksum_failures1", "checksum_failures2"], "class": "table_obj_value", "title": "Number of block checksum failures detected", "caption": "Failures"}, '
            '{"id": ["checksum_last_failure1", "checksum_last_failure2"], "class": "table_obj_value", "title": "Last checksum failure detected", "caption": "Last"}
            ]}, '
        '{"caption": "Block statistics", "columns": ['
            '{"id": ["blks_hit_pct1", "blks_hit_pct2"], "class": "table_obj_value", "title": "Buffer cache hit ratio", "caption": "Hit(%)"}, '
            '{"id": ["blks_read1", "blks_read2"], "class": "table_obj_value", "title": "Number of disk blocks read in this database", "caption": "Read"}, '
            '{"id": ["blks_hit1", "blks_hit2"], "class": "table_obj_value", "title": "Number of times disk blocks were found already in the buffer cache", "caption": "Hit"}'
        ']}, '
        '{"caption": "Block I/O times", "condition": "io_times", "columns": ['
            '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "title": "Time spent reading data file blocks by backends", "caption": "Read"}, '
            '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "title": "Time spent writing data file blocks by backends", "caption": "Write"} '
        ']}, '
        '{"caption": "Tuples", "columns": ['
            '{"id": ["tup_returned1", "tup_returned2"], "class": "table_obj_value", "title": "Number of rows returned by queries in this database", "caption": "Ret"}, '
            '{"id": ["tup_fetched1", "tup_fetched2"], "class": "table_obj_value", "title": "Number of rows fetched by queries in this database", "caption": "Fet"}, '
            '{"id": ["tup_inserted1", "tup_inserted2"], "class": "table_obj_value", "title": "Number of rows inserted by queries in this database", "caption": "Ins"}, '
            '{"id": ["tup_updated1", "tup_updated2"], "class": "table_obj_value", "title": "Number of rows updated by queries in this database", "caption": "Upd"}, '
            '{"id": ["tup_deleted1", "tup_deleted2"], "class": "table_obj_value", "title": "Number of rows deleted", "caption": "Del"}'
        ']}, '
        '{"caption": "Temp files", "columns": ['
            '{"id": ["temp_bytes1", "temp_bytes2"], "class": "table_obj_value", "title": "Total amount of data written to temporary files by queries in this database", "caption": "Size"}, '
            '{"id": ["temp_files1", "temp_files2"], "class": "table_obj_value", "title": "Number of temporary files created by queries in this database", "caption": "Files"}'
        ']}, '
        '{"id": ["datsize1", "datsize2"], "class": "table_obj_value", "title": "Database size as is was at the moment of last sample in report interval", "caption": "Size"}, '
        '{"id": ["datsize_delta1", "datsize_delta2"], "class": "table_obj_value", "title": "Database size increment during report interval", "caption": "Growth"} '
']}]'::jsonb),
(2, 'iostat', 'srvstat', 328, 'Cluster I/O statistics', 'Cluster I/O statistics', 'stat_io', NULL, 'stat_io', NULL, NULL),
(2, 'iostatrst', 'srvstat', 329, NULL, NULL, 'stat_io_reset', NULL, 'stat_io_reset',
'<p><b>Warning!</b> IO stats reset was detected during report interval. Statistic values may be affected</p>',
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_io_reset",'
    '"columns": ['
      '{"caption": "Sample ID", "id": "sample_id", "class": "table_obj_name", '
        '"title": "Sample identifier with detected reset"},'
      '{"caption": "Object", "id": "object", "class": "table_obj_name", '
        '"title": "Target object of an I/O operation"},'
      '{"caption": "Backend", "id": "backend_type", "class": "table_obj_name", '
        '"title": "Type of backend (see stat_activity)"},'
      '{"caption": "Context", "id": "context", "class": "table_obj_name", '
        '"title": "The context of an I/O operation"},'
      '{"caption": "Reset time", "id": "stats_reset", "class": "table_obj_value", '
        '"title": "Date and time of the last reset performed in sample"}'
    ']'
    '}]'::jsonb),
(2, 'iostatmain', 'srvstat', 330, NULL, NULL, 'stat_io', NULL, 'stat_io', NULL,
'[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "stat_io",'
    '"columns": ['
      '{"caption": "Object", "id": "object", "class": "hdr", "rowspan": true, '
        '"title": "Target object of an I/O operation"},'
      '{"caption": "Backend", "id": "backend_type", "class": "hdr", "rowspan": true, '
        '"title": "Type of backend (see stat_activity)"},'
      '{"caption": "Context", "id": "context", "class": "hdr", "rowspan": true, '
        '"title": "The context of an I/O operation"},'
      '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"},'
      '{"caption": "Reads", "columns": ['
        '{"caption": "Count", "id": ["reads1", "reads2"], "class": "table_obj_value", '
          '"title": "Number of read operations"},'
        '{"caption": "Bytes", "id": ["read_sz1", "read_sz2"], "class": "table_obj_value", '
          '"title": "Read data amount"},'
        '{"caption": "Time", "id": ["read_time1", "read_time2"], "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in reading operation (seconds)"}'
     ']},'
      '{"caption": "Writes", "columns": ['
        '{"caption": "Count", "id": ["writes1", "writes2"], "class": "table_obj_value", '
          '"title": "Number of write operations"},'
        '{"caption": "Bytes", "id": ["write_sz1", "write_sz2"], "class": "table_obj_value", '
          '"title": "Written data amount"},'
        '{"caption": "Time", "id": ["write_time1", "write_time2"], "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in writing operations (seconds)"}'
     ']},'
      '{"caption": "Writebacks", "columns": ['
        '{"caption": "Count", "id": ["writebacks1", "writebacks2"], "class": "table_obj_value", '
          '"title": "Number of blocks which the process requested the kernel write out to permanent storage"},'
        '{"caption": "Bytes", "id": ["writeback_sz1", "writeback_sz2"], "class": "table_obj_value", '
          '"title": "The amount of data requested for write out to permanent storage"},'
        '{"caption": "Time", "id": ["writeback_time1", "writeback_time2"], "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in writeback operations (seconds)"}'
     ']},'
      '{"caption": "Extends", "columns": ['
        '{"caption": "Count", "id": ["extends1", "extends2"], "class": "table_obj_value", '
          '"title": "Number of relation extend operations"},'
        '{"caption": "Bytes", "id": ["extend_sz1", "extend_sz2"], "class": "table_obj_value", '
          '"title": "The amount of space used by extend operations"},'
        '{"caption": "Time", "id": ["extend_time1", "extend_time2"], "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in extend operations (seconds)"}'
     ']},'
     '{"caption": "Hits", "id": ["hits1", "hits2"], "class": "table_obj_value", '
       '"title": "The number of times a desired block was found in a shared buffer"},'
     '{"caption": "Evictions", "id": ["evictions1", "evictions2"], "class": "table_obj_value", '
       '"title": "Number of times a block has been written out from a shared or local buffer in order to make it available for another use"},'
     '{"caption": "Reuses", "id": ["reuses1", "reuses2"], "class": "table_obj_value", '
       '"title": "The number of times an existing buffer in a size-limited ring buffer outside of shared buffers was reused as part of an I/O operation in the bulkread, bulkwrite, or vacuum contexts"},'
      '{"caption": "Fsyncs", "columns": ['
        '{"caption": "Count", "id": ["fsyncs1", "fsyncs2"], "class": "table_obj_value", '
          '"title": "Number of fsync calls. These are only tracked in context normal"},'
        '{"caption": "Time", "id": ["fsync_time1", "fsync_time2"], "class": "table_obj_value", '
          '"condition": "io_times", '
          '"title": "Time spent in fsync operations (seconds)"}'
     ']}'
    ']'
    '}]'::jsonb),
(2, 'slrustat', 'srvstat', 358, 'Cluster SLRU statistics', 'Cluster SLRU statistics', 'stat_slru', NULL, 'stat_slru', NULL, NULL),
(2, 'slrustatrst', 'slrustat', 359, NULL, NULL, 'stat_slru_reset', NULL, 'stat_slru_reset',
'<p><b>Warning!</b> SLRU stats reset was detected during report interval. Statistic values may be affected</p>',
'[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "stat_slru_reset",'
    '"columns": ['
      '{"caption": "Sample ID", "id": "sample_id", "class": "table_obj_name", '
        '"title": "Sample identifier with detected reset"},'
      '{"caption": "Name", "id": "name", "class": "table_obj_name", '
        '"title": "Name of the SLRU"},'
      '{"caption": "Reset time", "id": "stats_reset", "class": "table_obj_value", '
        '"title": "Date and time of the last reset performed in sample"}'
    ']'
    '}]'::jsonb),
(2, 'slrustatmain', 'slrustat', 360, NULL, NULL, 'stat_slru', NULL, 'stat_slru', NULL,
'[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "stat_slru",'
    '"columns": ['
      '{"caption": "Name", "id": "name", "class": "hdr", "rowspan": true, '
        '"title": "Name of the SLRU"},'
      '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"},'
      '{"caption": "Zeroed", "id": ["blks_zeroed1", "blks_zeroed2"], "class": "table_obj_value", '
        '"title": "Number of blocks zeroed during initializations"},'
      '{"caption": "Hits", "id": ["blks_hit1", "blks_hit2"], "class": "table_obj_value", '
        '"title": "Number of times disk blocks were found already in the SLRU, so that a '
        'read was not necessary (this only includes hits in the SLRU, not the operating '
        'system''s file system cache)"},'
      '{"caption": "Reads", "id": ["blks_read1", "blks_read2"], "class": "table_obj_value", '
        '"title": "Number of disk blocks read for this SLRU"},'
      '{"caption": "%Hit", "id": ["hit_pct1", "hit_pct2"], "class": "table_obj_value", '
        '"title": "Number of disk blocks hits for this SLRU as a percentage of reads + hits"},'
      '{"caption": "Writes", "id": ["blks_written1", "blks_written2"], "class": "table_obj_value", '
        '"title": "Number of disk blocks written for this SLRU"},'
      '{"caption": "Checked", "id": ["blks_exists1", "blks_exists2"], "class": "table_obj_value", '
        '"title": "Number of blocks checked for existence for this SLRU (blks_exists field)"},'
      '{"caption": "Flushes", "id": ["flushes1", "flushes2"], "class": "table_obj_value", '
        '"title": "Number of flushes of dirty data for this SLRU"},'
      '{"caption": "Truncates", "id": ["truncates1", "truncates2"], "class": "table_obj_value", '
        '"title": "Number of truncates for this SLRU"}'
    ']'
    '}]'::jsonb),
(2, 'sesstat', 'srvstat', 400, 'Session statistics by database', 'Session statistics by database', 'sess_stats', NULL, 'db_stat_sessions', NULL,
'[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "dbstat", '
    '"ordering": "ord_db",'
    '"columns": ['
        '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true},'
        '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"},'
        '{"caption": "Timings (s)", "columns": ['
          '{"id": ["session_time1", "session_time2"], "class": "table_obj_value", "title": "Time spent by database sessions in this database (note that statistics are only updated when the state of a session changes, so if sessions have been idle for a long time, this idle time wont be included)", "caption": "Total"},'
          '{"id": ["active_time1", "active_time2"], "class": "table_obj_value", "title": "Time spent executing SQL statements in this database (this corresponds to the states active and fastpath function call in pg_stat_activity)", "caption": "Active"},'
          '{"id": ["idle_in_transaction_time1", "idle_in_transaction_time2"], "class": "table_obj_value", "title": "Time spent idling while in a transaction in this database (this corresponds to the states idle in transaction and idle in transaction (aborted) in pg_stat_activity)", "caption": "Idle(T)"}'
        ']},'
        '{"caption": "Sessions", "columns": ['
          '{"id": ["sessions1", "sessions2"], "class": "table_obj_value", "title": "Total number of sessions established to this database", "caption": "Established"},'
          '{"id": ["sessions_abandoned1", "sessions_abandoned2"], "class": "table_obj_value", "title": "Number of database sessions to this database that were terminated because connection to the client was lost", "caption": "Abondoned"},'
          '{"id": ["sessions_fatal1", "sessions_fatal2"], "class": "table_obj_value", "title": "Number of database sessions to this database that were terminated by fatal errors", "caption": "Fatal"},'
          '{"id": ["sessions_killed1", "sessions_killed2"], "class": "table_obj_value", "title": "Number of database sessions to this database that were terminated by operator intervention", "caption": "Killed"}'
        ']}'
    ']}]'::jsonb),
(2, 'stmtstat', 'srvstat', 500, 'Statement statistics by database', 'Statement statistics by database', 'statstatements', NULL, 'st_stat', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "statements_dbstats",'
      '"ordering": "ord_db",'
      '"columns": ['
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": "true"}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of query executions", "caption": "Calls"}, '
          '{"caption": "Time (s)", "columns": ['
              '{"id": ["total_plan_time1", "total_plan_time2"], "class": "table_obj_value", "title": "Time spent planning queries", "caption": "Plan", "condition": "planning_times"}, '
              '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "TTime spent executing queries", "caption": "Exec"}, '
              '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "title": "Time spent reading blocks", "caption": "Read"}, '
              '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "title": "Time spent writing blocks", "caption": "Write"}, '
              '{"id": ["trg_fn_total_time1", "trg_fn_total_time2"], "class": "table_obj_value", "title": "Time spent in trigger functions", "caption": "Trg"}'
              ']}, '
          '{"caption": "Temp I/O Time", "condition": "statements_temp_io_times", "columns": ['
              '{"id": ["temp_blk_read_time1", "temp_blk_read_time2"], "class": "table_obj_value", "title": "Time spent reading temporary file blocks, in seconds", "caption": "Read"}, '
              '{"id": ["temp_blk_write_time1", "temp_blk_write_time2"], "class": "table_obj_value", "title": "Time spent writing temporary file blocks, in seconds", "caption": "Write"} '
              ']}, '
          '{"title": "Number of blocks fetched (hit + read)", "caption": "Fetched (blk)", "columns": ['
              '{"id": ["shared_gets1", "shared_gets2"], "class": "table_obj_value", "caption": "Shared"}, '
              '{"id": ["local_gets1", "local_gets2"], "class": "table_obj_value", "caption": "Local"}'
              ']}, '
          '{"title": "Number of blocks dirtied", "caption": "Dirtied (blk)", "columns": ['
              '{"id": ["shared_blks_dirtied1", "shared_blks_dirtied2"], "class": "table_obj_value", "caption": "Shared"}, '
              '{"id": ["local_blks_dirtied1", "local_blks_dirtied2"], "class": "table_obj_value", "caption": "Local"}'
              ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": ["temp_blks_read1", "temp_blks_read2"], "class": "table_obj_value", "caption": "Read"}, '
              '{"id": ["temp_blks_written1", "temp_blks_written2"], "class": "table_obj_value", "caption": "Write"}'
              ']}, '
          '{"title": "Number of blocks, used for temporary tables", "caption": "Local (blk)", "columns": ['
              '{"id": ["local_blks_read1", "local_blks_read2"], "class": "table_obj_value", "caption": "Read"}, '
              '{"id": ["local_blks_written1", "local_blks_written2"], "class": "table_obj_value", "caption": "Write"}'
              ']}, '
          '{"id": ["statements1", "statements2"], "class": "table_obj_value", "caption": "Statements"}, '
          '{"id": ["wal_bytes1", "wal_bytes2"], "class": "table_obj_value", "caption": "WAL size", "condition": "statement_wal_bytes"}'
      ']}]'::jsonb),
(2, 'dbjitstat', 'srvstat', 550, 'JIT statistics by database', 'JIT statistics by database', 'statements_jit_stats', NULL, 'dbagg_jit_stat', NULL,
  '[{'
      '"type": "row_table",'
      '"class": "diff",'
      '"source": "statements_dbstats",'
      '"ordering": "ord_db",'
      '"columns": ['
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": "true"},'
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"},'
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of query executions", "caption": "Calls"},'
          '{"caption": "Time (s)", "columns": ['
              '{"id": ["total_plan_time1", "total_plan_time2"], "class": "table_obj_value", "title": "Time spent planning queries", "caption": "Plan", "condition": "planning_times"},'
              '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "Time spent planning queries", "caption": "Exec"}'
              ']},'
          '{"caption": "Generation", "columns": ['
              '{"id": ["jit_functions1", "jit_functions2"], "class": "table_obj_value", "title": "Total number of functions JIT-compiled by the statements", "caption": "Count"},'
              '{"id": ["jit_generation_time1", "jit_generation_time2"], "class": "table_obj_value", "title": "Time spent by the statements on generating JIT code", "caption": "Gen. time"}'
              ']},'
          '{"caption": "Inlining", "columns": ['
              '{"id": ["jit_inlining_count1", "jit_inlining_count2"], "class": "table_obj_value", "title": "Number of times functions have been inlined", "caption": "Count"},'
              '{"id": ["jit_inlining_time1", "jit_inlining_time2"], "class": "table_obj_value", "title": "Time spent by statements on inlining functions", "caption": "Time"}'
              ']},'
          '{"caption": "Optimization", "columns": ['
              '{"id": ["jit_optimization_count1", "jit_optimization_count2"], "class": "table_obj_value", "title": "Number of times statements hasbeen optimized", "caption": "Count"},'
              '{"id": ["jit_optimization_time1", "jit_optimization_time2"], "class": "table_obj_value", "title": "Time spent by statements on optimizing", "caption": "Time"}'
              ']},'
          '{"caption": "Emission", "columns": ['
              '{"id": ["jit_emission_count1", "jit_emission_count2"], "class": "table_obj_value", "title": "Number of times code has been emitted", "caption": "Count"},'
              '{"id": ["jit_emission_time1", "jit_emission_time2"], "class": "table_obj_value", "title": "Time spent executing queries", "caption": "Exec"}'
              ']}'
          ']}]'::jsonb),
(2, 'commonstat', 'srvstat', 600, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, 'clusterstat', 'commonstat', 600, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, 'clusthdr', 'clusterstat', 700, 'Cluster statistics', 'Cluster statistics', NULL, NULL, 'clu_stat', NULL, NULL),
(2, 'clustrst', 'clusterstat', 800, NULL, NULL, 'cluster_stats_reset', NULL, NULL,
  '<p><b>Warning!</b> Cluster statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>Cluster statistics might be affected</p>',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "cluster_stats_reset",'
    '"columns": ['
      '{"id": "interval_num", "class": "table_obj_value", "caption": "I"},'
      '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample"},'
      '{"id": "bgwriter_stats_reset", "class": "table_obj_value", "caption": "BGWriter reset time"},'
      '{"id": "archiver_stats_reset", "class": "table_obj_value", "caption": "Archiver reset time"}'
    ']}]'::jsonb),
(2, 'clust', 'clusterstat', 900, NULL, NULL, NULL, NULL, NULL, NULL,
  '[{'
    '"type": "column_table", '
    '"source": "cluster_stats",'
    '"class": "stat", '
    '"columns": ['
        '{"caption": "Metric"}, '
        '{"caption": "Value (1)", "title": "properties.timePeriod1"}, '
        '{"caption": "Value (2)", "title": "properties.timePeriod2"}'
    '],'
    '"rows": ['
        '{"caption": "Scheduled checkpoints", "cells": ['
            '{"id": "checkpoints_timed1", "class": "table_obj_value int1"}, '
            '{"id": "checkpoints_timed2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Requested checkpoints", "cells": ['
            '{"id": "checkpoints_req1", "class": "table_obj_value int1"},'
            '{"id": "checkpoints_req2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Checkpoint write time (s)", "cells": ['
            '{"id": "checkpoint_write_time1", "class": "table_obj_value int1"},'
            '{"id": "checkpoint_write_time2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Checkpoint sync time (s)", "cells": ['
            '{"id": "checkpoint_sync_time1", "class": "table_obj_value int1"},'
            '{"id": "checkpoint_sync_time2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Checkpoint buffers written", "cells": ['
            '{"id": "buffers_checkpoint1", "class": "table_obj_value int1"},'
            '{"id": "buffers_checkpoint2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Background buffers written", "cells": ['
            '{"id": "buffers_clean1", "class": "table_obj_value int1"},'
            '{"id": "buffers_clean2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Backend buffers written", "cells": ['
            '{"id": "buffers_backend1", "class": "table_obj_value int1"},'
            '{"id": "buffers_backend2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Backend fsync count", "cells": ['
            '{"id": "buffers_backend_fsync1", "class": "table_obj_value int1"},'
            '{"id": "buffers_backend_fsync2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Bgwriter interrupts (too many buffers)", "cells": ['
            '{"id": "maxwritten_clean1", "class": "table_obj_value int1"},'
            '{"id": "maxwritten_clean2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Number of buffers allocated", "cells": ['
            '{"id": "buffers_alloc1", "class": "table_obj_value int1"},'
            '{"id": "buffers_alloc2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "WAL generated", "cells": ['
            '{"id": "wal_size_pretty1", "class": "table_obj_value int1"},'
            '{"id": "wal_size_pretty2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "Start LSN", "cells": ['
            '{"id": "start_lsn1", "class": "table_obj_value int1"},'
            '{"id": "start_lsn2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "End LSN", "cells": ['
            '{"id": "end_lsn1", "class": "table_obj_value int1"},'
            '{"id": "end_lsn2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "WAL segments archived", "cells": ['
            '{"id": "archived_count1", "class": "table_obj_value int1"},'
            '{"id": "archived_count2", "class": "table_obj_value int2"}'
        ']}, '
        '{"caption": "WAL segments archive failed", "cells": ['
            '{"id": "failed_count1", "class": "table_obj_value int1"},'
            '{"id": "failed_count2", "class": "table_obj_value int2"}'
        ']}'
    ']'
    '}]'::jsonb),
(2, 'walstat', 'commonstat', 950, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
(2, 'walsthdr', 'walstat', 1000, 'WAL statistics', 'WAL statistics', 'wal_stats', NULL, 'wal_stat', NULL, NULL),
(2, 'walstrst', 'walstat', 1100, NULL, NULL, 'wal_stats_reset', NULL, NULL,
  '<p><b>Warning!</b> WAL statistics reset detected during report interval!</p>'
  '{func_output}'
  '<p>WAL statistics might be affected</p>',
  '[{'
    '"type": "row_table",'
    '"class": "stat",'
    '"source": "wal_stats_reset",'
    '"columns": ['
      '{"id": "interval_num", "class": "table_obj_value", "caption": "I"},'
      '{"id": "sample_id", "class": "table_obj_value", "caption": "Sample"},'
      '{"id": "wal_stats_reset", "class": "table_obj_value", "caption": "WAL stats reset time"}'
    ']}]'::jsonb),
(2, 'walst', 'walstat', 1200, NULL, NULL, 'wal_stats', NULL, NULL, '{func_output}</div>',
  '[{'
    '"type": "column_table",'
    '"source": "wal_stats",'
    '"class": "stat",'
    '"columns": ['
        '{"caption": "Metric"},'
        '{"caption": "Value (1)", "title": "properties.timePeriod1"},'
        '{"caption": "Value (2)", "title": "properties.timePeriod2"}'
    '],'
    '"rows": ['
      '{"caption": "WAL generated", "title": "Total amount of WAL generated", "cells": ['
          '{"id": "wal_bytes1", "class": "table_obj_value int1"},'
          '{"id": "wal_bytes2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL per second", "title": "Average amount of WAL generated per second", "cells": ['
          '{"id": "wal_bytes_per_sec1", "class": "table_obj_value int1"}, '
          '{"id": "wal_bytes_per_sec2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL records", "title": "Total number of WAL records generated", "cells": ['
          '{"id": "wal_records1", "class": "table_obj_value int1"},'
          '{"id": "wal_records2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL FPI", "title": "Total number of WAL full page images generated", "cells": ['
          '{"id": "wal_fpi1", "class": "table_obj_value int1"},'
          '{"id": "wal_fpi2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL buffers full", "title": "Number of times WAL data was written to disk because WAL buffers became full", "cells": ['
          '{"id": "wal_buffers_full1", "class": "table_obj_value int1"},'
          '{"id": "wal_buffers_full2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL writes", "title": "Number of times WAL buffers were written out to disk via XLogWrite request", "cells": ['
          '{"id": "wal_write1", "class": "table_obj_value int1"}, '
          '{"id": "wal_write2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL writes per second", "title": "Average number of times WAL buffers were written out to disk via XLogWrite request per .second", "cells": ['
          '{"id": "wal_write_per_sec1", "class": "table_obj_value int1"}, '
          '{"id": "wal_write_per_sec2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL sync", "title": "Number of times WAL files were synced to disk via issue_xlog_fsync request (if fsync is on and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)", "cells": ['
          '{"id": "wal_sync1", "class": "table_obj_value int1"}, '
          '{"id": "wal_sync2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL syncs per second", "title": "Average number of times WAL files were synced to disk via issue_xlog_fsync request per second", "cells": ['
          '{"id": "wal_sync_per_sec1", "class": "table_obj_value int1"}, '
          '{"id": "wal_sync_per_sec2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL write time (s)", "title": "Total amount of time spent writing WAL buffers to disk via XLogWrite request, in milliseconds (if track_wal_io_timing is enabled, otherwise zero). This includes the sync time when wal_sync_method is either open_datasync or open_sync", "cells": ['
          '{"id": "wal_write_time1", "class": "table_obj_value int1"}, '
          '{"id": "wal_write_time2", "class": "table_obj_value int2"}'
      ']},'
      '{"title": "WAL write time as a percentage of the report duration time", "caption": "WAL write duty", "cells": ['
          '{"id": "wal_write_time_per_sec1", "class": "table_obj_value int1"}, '
          '{"id": "wal_write_time_per_sec2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL sync time (s)", "title": "Total amount of time spent syncing WAL files to disk via issue_xlog_fsync request, in milliseconds (if track_wal_io_timing is enabled, fsync is on, and wal_sync_method is either fdatasync, fsync or fsync_writethrough, otherwise zero)", "cells": ['
          '{"id": "wal_sync_time1", "class": "table_obj_value int1"}, '
          '{"id": "wal_sync_time2", "class": "table_obj_value int2"}'
      ']},'
      '{"caption": "WAL sync duty", "title": "WAL sync time as a percentage of the report duration time", "cells": ['
          '{"id": "wal_sync_time_per_sec1", "class": "table_obj_value int1"},'
          '{"id": "wal_sync_time_per_sec2", "class": "table_obj_value int2"}'
      ']}'
    ']'
  '}]'::jsonb),
(2, 'tbspst', 'srvstat', 1400, 'Tablespace statistics', 'Tablespace statistics', NULL, NULL, 'tablespace_stat', NULL,
  '[{'
      '"type": "row_table",'
      '"source": "tablespace_stats",'
      '"ordering": "tablespacename",'
      '"class": "diff",'
      '"columns": ['
        '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
        '{"caption": "Path", "id": "tablespacepath", "class": "hdr", "rowspan": true},'
        '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"}, '
        '{"caption": "Size", "id": ["size1", "size2"], "class": "table_obj_value", '
        '"title": "Tablespace size as it was at the moment of last sample in report interval"},'
        '{"caption": "Growth", "id": ["size_delta1", "size_delta2"], "class": "table_obj_value", '
        '"title": "Tablespace size increment during report interval"}'
      ']'
    '}]'::jsonb),
(2, 'wait_sampling_srvstats', 'srvstat', 1500, 'Wait sampling', 'Wait sampling', 'wait_sampling_tot', NULL, 'wait_sampling', NULL, NULL),
(2, 'wait_sampling_total', 'wait_sampling_srvstats', 100, 'Wait events types', 'Wait events types', 'wait_sampling_tot', NULL, 'wait_sampling_total', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "wait_sampling_total_stats",'
      '"ordering": "event_type_order",'
      '"columns": ['
          '{"id": "event_type", "class": "hdr", "caption": "Wait event type", "rowspan": true}, '
          '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"}, '
          '{"id": ["stmt_waited1", "stmt_waited2"], "class": "table_obj_value", "title": "Time, waited in events of wait event type executing statements in seconds", "caption": "Statements Waited (s)"}, '
          '{"id": ["stmt_waited_pct1", "stmt_waited_pct2"], "class": "table_obj_value", "title": "Time, waited in events of wait event type as a percentage of total time waited in a cluster executing statements", "caption": "%Total"}, '
          '{"id": ["tot_waited1", "tot_waited2"], "class": "table_obj_value", "title": "Time, waited in events of wait event type by all backends (including background activity) in seconds", "caption": "All Waited (s)"}, '
          '{"id": ["tot_waited_pct1", "tot_waited_pct2"], "class": "table_obj_value", "title": "Time, waited in events of wait event type as a percentage of total time waited in a cluster by all backends (including background activity)", "caption": "%Total"}'
      ']}]'::jsonb),
(2, 'wait_sampling_statements', 'wait_sampling_srvstats', 200, 'Top wait events (statements)', 'Top wait events (statements)', 'wait_sampling_tot', NULL, 'wt_smp_stmt', '<p>Top wait events detected in statements execution</p>',
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "wait_sampling_events",'
      '"filter": {"type": "exists", "field": "stmt_filter"},'
      '"ordering": "stmt_ord",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "event_type", "class": "hdr", "caption": "Wait event type", "rowspan": true}, '
          '{"id": "event", "class": "hdr", "caption": "Wait event", "rowspan": true}, '
          '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"}, '
          '{"id": ["stmt_waited1", "stmt_waited2"], "class": "table_obj_value", "title": "Time, waited in event executing statements in seconds", "caption": "Waited (s)"}, '
          '{"id": ["stmt_waited_pct1", "stmt_waited_pct2"], "class": "table_obj_value", "title": "Time, waited in event as a percentage of total time waited in a cluster executing statements", "caption": "%Total"}'
      ']}]'::jsonb),
(2, 'wait_sampling_all', 'wait_sampling_srvstats', 300, 'Top wait events (All)', 'Top wait events (All)', 'wait_sampling_tot', NULL, 'wt_smp_all', '<p>Top wait events detected in all backends</p>',
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "wait_sampling_events",'
      '"filter": {"type": "exists", "field": "total_filter"},'
      '"ordering": "tot_ord",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "event_type", "class": "hdr", "caption": "Wait event type", "rowspan": true}, '
          '{"id": "event", "class": "hdr", "caption": "Wait event", "rowspan": true}, '
          '{"id": ["1", "2"], "class": "interval", "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I"}, '
          '{"id": ["tot_waited1", "tot_waited2"], "class": "table_obj_value", "title": "Time, waited in event by all backends (including background activity) in seconds", "caption": "Waited (s)"}, '
          '{"id": ["tot_waited_pct1", "tot_waited_pct2"], "class": "table_obj_value", "title": "Time, waited in event by all backends as a percentage of total time waited in a cluster by all backends (including background activity)", "caption": "%Total"}'
      ']}]'::jsonb)
;

-- Query section of differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(2, 'sqlela_t', 'sqlsthdr', 100, 'Top SQL by elapsed time', 'Top SQL by elapsed time', 'planning_times', NULL, 'top_ela', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_total_time",'
      '"filter": {"type": "exists", "field": "ord_total_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["total_time_pct1", "total_time_pct2"], "class": "table_obj_value", "title": "Elapsed time as a percentage of total cluster elapsed time", "caption": "%Total"}, '
          '{"caption": "Time (s)", "columns": ['
              '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed"}, '
              '{"id": ["total_plan_time1", "total_plan_time2"], "class": "table_obj_value", "title": "Time spent planning statement", "caption": "Plan"}, '
              '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec"}'
              ']}, '
          '{"id": ["jit_total_time1", "jit_total_time2"], "class": "jitTimeCell", "caption": "JIT<br>time (s)", "condition": "statements_jit_stats"}, '
          '{"class": "table_obj_name", "caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "title": "Time spent reading blocks by statement", "caption": "Read"}, '
              '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "title": "Time spent writing blocks by statement", "caption": "Write"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "CPU time (s)", "condition": "kcachestatements", "columns": ['
              '{"id": ["user_time1", "user_time2"], "class": "table_obj_value", "caption": "Usr"}, '
              '{"id": ["system_time1", "system_time2"], "class": "table_obj_value", "caption": "Sys"}'
              ']}, '
          '{"id": ["plans1", "plans2"], "class": "table_obj_value", "caption": "Plans", "title": "Number of times the statement was planned"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "caption": "Executions", "title": "Number of times the statement was executed"} '
      ']'
  '}]'::jsonb),
(2, 'sqlplan_t', 'sqlsthdr', 200, 'Top SQL by planning time', 'Top SQL by planning time', 'planning_times', NULL, 'top_plan', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_plan_time",'
      '"filter": {"type": "exists", "field": "ord_plan_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["total_plan_time1", "total_plan_time2"], "class": "table_obj_value", "title": "Time spent planning statement", "caption": "Plan elapsed (s)"}, '
          '{"id": ["plan_time_pct1", "plan_time_pct2"], "class": "table_obj_value", "title": "Plan elapsed as a percentage of statement elapsed time", "caption": "%Elapsed"}, '
          '{"title": "Planning time statistics", "caption": "Plan times (ms)", "columns": ['
              '{"id": ["mean_plan_time1", "mean_plan_time2"], "class": "table_obj_value", "caption": "Mean"}, '
              '{"id": ["min_plan_time1", "min_plan_time2"], "class": "table_obj_value", "caption": "Min"}, '
              '{"id": ["max_plan_time1", "max_plan_time2"], "class": "table_obj_value", "caption": "Max"}, '
              '{"id": ["stddev_plan_time1", "stddev_plan_time2"], "class": "table_obj_value", "caption": "StdErr"}'
          ']}, '
          '{"id": ["plans1", "plans2"], "class": "table_obj_value", "title": "Number of times the statement was planned", "caption": "Plans"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqlexec_t', 'sqlsthdr', 300, 'Top SQL by execution time', 'Top SQL by execution time', NULL, NULL, 'top_exec', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_exec_time",'
      '"filter": {"type": "exists", "field": "ord_exec_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "caption": "Query ID", "class": "hdr mono queryId", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec (s)"}, '
          '{"id": ["exec_time_pct1", "exec_time_pct2"], "class": "table_obj_value", "title": "Exec time as a percentage of statement elapsed time", "caption": "%Elapsed", "condition": "planning_times"}, '
          '{"id": ["total_exec_time_pct1", "total_exec_time_pct2"], "class": "table_obj_value", "title": "Exec time as a percentage of total cluster elapsed time", "caption": "%Total"}, '
          '{"id": ["jit_total_time1", "jit_total_time2"], "class": "jitTimeCell", "title": "Exec time as a percentage of statement elapsed time", "caption": "JIT time (s)", "condition": "statements_jit_stats"}, '
          '{"caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "caption": "Read"}, '
              '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "caption": "Write"}'
          ']}, '
          '{"caption": "CPU time (s)", "condition": "kcachestatements", "columns": ['
              '{"id": ["user_time1", "user_time2"], "class": "table_obj_value", "caption": "Usr"}, '
              '{"id": ["system_time1", "system_time2"], "class": "table_obj_value", "caption": "Sys"}'
          ']}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "caption": "Rows"}, '
          '{"title": "Execution time statistics", "caption": "Execution times (ms)", "columns": ['
              '{"id": ["mean_exec_time1", "mean_exec_time2"], "class": "table_obj_value", "caption": "Mean"}, '
              '{"id": ["min_exec_time1", "min_exec_time2"], "class": "table_obj_value", "caption": "Min"}, '
              '{"id": ["max_exec_time1", "max_exec_time2"], "class": "table_obj_value", "caption": "Max"}, '
              '{"id": ["stddev_exec_time1", "stddev_exec_time2"], "class": "table_obj_value", "caption": "StdErr"}'
          ']}, '
          '{"id": ["calls1", "calls2"], "title": "Number of times the statement was executed", "caption": "Executions", "class": "table_obj_value"}'
      ']'
  '}]'::jsonb),
(2, 'sqlcalls', 'sqlsthdr', 400, 'Top SQL by executions', 'Top SQL by executions', NULL, NULL, 'top_calls', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_calls",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}, '
          '{"id": ["calls_pct1", "calls_pct2"], "class": "table_obj_value", "title": "Executions of this statement as a percentage of total executions of all statements in a cluster", "caption": "%Total"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["mean_exec_time1", "mean_exec_time2"], "class": "table_obj_value", "caption": "Mean(ms)"}, '
          '{"id": ["min_exec_time1", "min_exec_time2"], "class": "table_obj_value", "caption": "Min(ms)"}, '
          '{"id": ["max_exec_time1", "max_exec_time2"], "class": "table_obj_value", "caption": "Max(ms)"}, '
          '{"id": ["stddev_exec_time1", "stddev_exec_time2"], "class": "table_obj_value", "caption": "StdErr(ms)"}, '
          '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}'
      ']'
  '}]'::jsonb),
(2, 'sqlio_t', 'sqlsthdr', 500, 'Top SQL by I/O wait time', 'Top SQL by I/O wait time', 'io_times', NULL, 'top_iowait', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_io_time",'
      '"filter": {"type": "exists", "field": "ord_io_time"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["io_time1", "io_time2"], "class": "table_obj_value", "title": "Time spent by the statement reading and writing blocks", "caption": "IO(s)"}, '
          '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "title": "Time spent by the statement reading blocks", "caption": "R(s)"}, '
          '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "title": "Time spent by the statement writing blocks", "caption": "W(s)"}, '
          '{"id": ["io_time_pct1", "io_time_pct2"], "class": "table_obj_value", "title": "I/O time of this statement as a percentage of total I/O time for all statements in a cluster", "caption": "%Total"}, '
          '{"title": "Number of blocks read by the statement", "caption": "Reads", "columns": ['
              '{"id": ["shared_blks_read1", "shared_blks_read2"], "title": "Number of shared blocks read by the statement", "caption": "Shr", "class": "table_obj_value"}, '
              '{"id": ["local_blks_read1", "local_blks_read2"], "title": "Number of local blocks read by the statement (usually used for temporary tables)", "caption": "Loc", "class": "table_obj_value"}, '
              '{"id": ["temp_blks_read1", "temp_blks_read2"], "title": "Number of temp blocks read by the statement (usually used for operations like sorts and joins)", "caption": "Tmp", "class": "table_obj_value"}'
          ']}, '
          '{"title": "Number of blocks written by the statement", "caption": "Writes", "columns": ['
              '{"id": ["shared_blks_written1", "shared_blks_written2"], "title": "Number of shared blocks written by the statement", "caption": "Shr", "class": "table_obj_value"}, '
              '{"id": ["local_blks_written1", "local_blks_written2"], "title": "Number of local blocks written by the statement (usually used for temporary tables)", "caption": "Loc", "class": "table_obj_value"}, '
              '{"id": ["temp_blks_written1", "temp_blks_written2"], "title": "Number of temp blocks written by the statement (usually used for operations like sorts and joins)", "caption": "Tmp", "class": "table_obj_value"}'
          ']}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of blocks written by the statement", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqlfetch', 'sqlsthdr', 600, 'Top SQL by shared blocks fetched', 'Top SQL by shared blocks fetched', NULL, NULL, 'top_pgs_fetched', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"ordering": "ord_shared_blocks_fetched",'
      '"filter": {"type": "exists", "field": "ord_shared_blocks_fetched"},'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["shared_blks_fetched1", "shared_blks_fetched2"], "class": "table_obj_value", "title": "Shared blocks fetched (read and hit) by the statement", "caption": "blks fetched"}, '
          '{"id": ["shared_blks_fetched_pct1", "shared_blks_fetched_pct2"], "class": "table_obj_value", "title": "Shared blocks fetched by this statement as a percentage of all shared blocks fetched in a cluster", "caption": "%Total"}, '
          '{"id": ["shared_hit_pct1", "shared_hit_pct2"], "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']}]'::jsonb),
(2, 'sqlshrd', 'sqlsthdr', 700, 'Top SQL by shared blocks read', 'Top SQL by shared blocks read', NULL, NULL, 'top_shared_reads', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_shared_blocks_read"},'
      '"ordering": "ord_shared_blocks_read",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["shared_blks_read1", "shared_blks_read2"], "class": "table_obj_value", "title": "Total number of shared blocks read by the statement", "caption": "Reads"}, '
          '{"id": ["read_pct1", "read_pct2"], "class": "table_obj_value", "title": "Shared blocks read by this statement as a percentage of all shared blocks read in a cluster", "caption": "%Total"}, '
          '{"id": ["shared_hit_pct1", "shared_hit_pct2"], "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqlshdir', 'sqlsthdr', 800, 'Top SQL by shared blocks dirtied', 'Top SQL by shared blocks dirtied', NULL, NULL, 'top_shared_dirtied', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_shared_blocks_dirt"},'
      '"ordering": "ord_shared_blocks_dirt",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"],  "class": "interval", "caption": "I"}, '
          '{"id": ["shared_blks_dirtied1", "shared_blks_dirtied2"], "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Dirtied"}, '
          '{"id": ["shared_blks_dirtied1", "shared_blks_dirtied2"], "class": "table_obj_value", "title": "Shared blocks dirtied by this statement as a percentage of all shared blocks dirtied in a cluster", "caption": "%Total"}, '
          '{"id": ["shared_hit_pct1", "shared_hit_pct2"], "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Hits(%)"}, '
          '{"id": ["dirtied_pct1", "dirtied_pct2"], "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Dirtied"}, '
          '{"id": ["wal_bytes1", "wal_bytes2"], "class": "table_obj_value", "title": "Total amount of WAL bytes generated by the statement", "caption": "WAL", "condition": "statement_wal_bytes"}, '
          '{"id": ["wal_bytes_pct1", "wal_bytes_pct2"], "class": "table_obj_value", "title": "WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster", "caption": "%Total", "condition": "statement_wal_bytes"}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqlshwr', 'sqlsthdr', 900, 'Top SQL by shared blocks written', 'Top SQL by shared blocks written', NULL, NULL, 'top_shared_written', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_shared_blocks_written"},'
      '"ordering": "ord_shared_blocks_written",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"],  "class": "interval", "caption": "I"}, '
          '{"id": ["shared_blks_written1", "shared_blks_written2"], "class": "table_obj_value", "title": "Total number of shared blocks written by the statement", "caption": "Written"}, '
          '{"id": ["tot_written_pct1", "tot_written_pct2"], "class": "table_obj_value", "title": "Shared blocks written by this statement as a percentage of all shared blocks written in a cluster (sum of pg_stat_bgwriter fields buffers_checkpoint, buffers_clean and buffers_backend)", "caption": "%Total"}, '
          '{"id": ["backend_written_pct1", "backend_written_pct2"], "class": "table_obj_value", "title": "Shared blocks written by this statement as a percentage total buffers written directly by a backends (buffers_backend of pg_stat_bgwriter view)", "caption": "%BackendW"}, '
          '{"id": ["shared_hit_pct1", "shared_hit_pct2"], "class": "table_obj_value", "title": "Shared blocks hits as a percentage of shared blocks fetched (read + hit)", "caption": "Hits(%)"}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqlwalsz', 'sqlsthdr', 1000, 'Top SQL by WAL size', 'Top SQL by WAL size', 'statement_wal_bytes', NULL, 'top_wal_bytes', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_wal"},'
      '"ordering": "ord_wal",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"],  "class": "interval", "caption": "I"}, '
          '{"id": ["wal_bytes1", "wal_bytes2"], "class": "table_obj_value", "title": "Total amount of WAL bytes generated by the statement", "caption": "WAL"}, '
          '{"id": ["wal_bytes_pct1", "wal_bytes_pct2"], "class": "table_obj_value", "title": "WAL bytes of this statement as a percentage of total WAL bytes generated by a cluster", "caption": "%Total"}, '
          '{"id": ["shared_blks_dirtied1", "shared_blks_dirtied2"], "class": "table_obj_value", "title": "Total number of shared blocks dirtied by the statement", "caption": "Dirtied"}, '
          '{"id": ["wal_fpi1", "wal_fpi2"], "class": "table_obj_value", "title": "Total number of WAL full page images generated by the statement", "caption": "WAL FPI"}, '
          '{"id": ["wal_records1", "wal_records2"], "class": "table_obj_value", "title": "Total number of WAL records generated by the statement", "caption": "WAL records"}'
      ']'
  '}]'::jsonb),
(2, 'sqltmp', 'sqlsthdr', 1100, 'Top SQL by temp usage', 'Top SQL by temp usage', 'statements_top_temp', NULL, 'top_temp', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_temp"},'
      '"ordering": "ord_temp",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"],  "class": "interval", "caption": "I"}, '
          '{"id": ["local_blks_fetched1", "local_blks_fetched2"], "class": "table_obj_value", "title": "Number of local blocks fetched (hit + read)", "caption": "Local fetched"}, '
          '{"id": ["local_hit_pct1", "local_hit_pct2"], "class": "table_obj_value", "title": "Local blocks hit percentage", "caption": "Hits(%)"}, '
          '{"title": "Number of blocks, used for temporary tables", "caption": "Local (blk)", "columns": ['
              '{"id": ["local_blks_written1", "local_blks_written2"], "class": "table_obj_value", "title": "Number of written local blocks", "caption": "Write"}, '
              '{"id": ["local_write_total_pct1", "local_write_total_pct2"], "class": "table_obj_value", "title": "Percentage of all local blocks written", "caption": "%Total"}, '
              '{"id": ["local_blks_read1", "local_blks_read2"], "class": "table_obj_value", "title": "Number of read local blocks", "caption": "Read"}, '
              '{"id": ["local_read_total_pct1", "local_read_total_pct2"], "class": "table_obj_value", "title": "Percentage of all local blocks read", "caption": "%Total"}'
          ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": ["temp_blks_written1", "temp_blks_written2"], "class": "table_obj_value", "title": "Number of written temp blocks", "caption": "Write"}, '
              '{"id": ["temp_write_total_pct1", "temp_write_total_pct2"], "class": "table_obj_value", "title": "Percentage of all temp blocks written", "caption": "%Total"}, '
              '{"id": ["temp_blks_read1", "temp_blks_read2"], "class": "table_obj_value", "title": "Number of read temp blocks", "caption": "Read"}, '
              '{"id": ["temp_read_total_pct1", "temp_read_total_pct2"], "class": "table_obj_value", "title": "Percentage of all temp blocks read", "caption": "%Total"}'
          ']}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqltmpiotime', 'sqlsthdr', 1125, 'Top SQL by temp I/O time', 'Top SQL by temp I/O time', 'statements_temp_io_times', NULL, 'top_temp_io_time', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_temp_io_time"},'
      '"ordering": "ord_temp_io_time",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"],  "class": "interval", "caption": "I"}, '
          '{"title": "Time the statement spent on temporary file blocks I/O", "caption": "Temp I/O time (s)", "columns": ['
              '{"id": ["temp_blk_read_time1", "temp_blk_read_time2"], "class": "table_obj_value", "title": "Time the statement spent reading temporary file blocks, in seconds", "caption": "Read"}, '
              '{"id": ["temp_blk_write_time1", "temp_blk_write_time2"], "class": "table_obj_value", "title": "Time the statement spent reading temporary file blocks, in seconds", "caption": "Write"}, '
              '{"id": ["temp_io_time_pct1", "temp_io_time_pct2"], "class": "table_obj_value", "title": "Time spent on temporary file blocks I/O of this statement as a percentage of total time spent on temporary file blocks I/O by all statements", "caption": "%Total"} '
          ']}, '
          '{"title": "Number of blocks, used in operations (like sorts and joins)", "caption": "Temp (blk)", "columns": ['
              '{"id": ["temp_blks_written1", "temp_blks_written2"], "class": "table_obj_value", "title": "Number of written temp blocks", "caption": "Write"}, '
              '{"id": ["temp_write_total_pct1", "temp_write_total_pct2"], "class": "table_obj_value", "title": "Percentage of all temp blocks written", "caption": "%Total"}, '
              '{"id": ["temp_blks_read1", "temp_blks_read2"], "class": "table_obj_value", "title": "Number of read temp blocks", "caption": "Read"}, '
              '{"id": ["temp_read_total_pct1", "temp_read_total_pct2"], "class": "table_obj_value", "title": "Percentage of all temp blocks read", "caption": "%Total"}'
          ']}, '
          '{"id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent by the statement", "caption": "Elapsed(s)"}, '
          '{"id": ["rows1", "rows2"], "class": "table_obj_value", "title": "Total number of rows retrieved or affected by the statement", "caption": "Rows"}, '
          '{"id": ["calls1", "calls2"], "class": "table_obj_value", "title": "Number of times the statement was executed", "caption": "Executions"}'
      ']'
  '}]'::jsonb),
(2, 'sqljit', 'sqlsthdr', 1150, 'Top SQL by JIT elapsed time', 'Top SQL by JIT elapsed time', 'statements_jit_stats', NULL, 'top_jit', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "diff", '
      '"source": "top_statements",'
      '"filter": {"type": "exists", "field": "ord_jit"},'
      '"ordering": "ord_jit",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono jitCellId", "caption": "Query ID", "rowspan": true}, '
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
          '{"id": ["jit_total_time1", "jit_total_time2"], "class": "table_obj_value", "title": "Time spent on JIT in seconds", "caption": "JIT total (s)"}, '
          '{"caption": "Generation", "columns": ['
              '{"id": ["jit_functions1", "jit_functions2"], "class": "table_obj_value", "title": "Total number of functions JIT-compiled by the statement.", "caption": "Count"}, '
              '{"id": ["jit_generation_time1", "jit_generation_time2"], "class": "table_obj_value", "title": "Time spent by the statement on generating JIT code, in seconds.", "caption": "Time (s)"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "Inlining", "columns": ['
              '{"id": ["jit_inlining_count1", "jit_inlining_count2"], "class": "table_obj_value", "title": "Number of times functions have been inlined.", "caption": "Count"}, '
              '{"id": ["jit_inlining_time1", "jit_inlining_time2"], "class": "table_obj_value", "title": "Time spent by the statement on inlining functions, in seconds.", "caption": "Time (s)"}'
              ']}, '
          '{"class": "table_obj_name", "caption": "Optimization", "columns": ['
              '{"id": ["jit_optimization_count1", "jit_optimization_count2"], "class": "table_obj_value", "title": "Number of times the statement has been optimized.", "caption": "Count"}, '
              '{"id": ["jit_optimization_time1", "jit_optimization_time2"], "class": "table_obj_value", "title": "Time spent by the statement on optimizing, in seconds.", "caption": "Time (s)"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "Emission", "columns": ['
              '{"id": ["jit_emission_count1", "jit_emission_count2"], "class": "table_obj_value", "title": "Number of times code has been emitted.", "caption": "Count"}, '
              '{"id": ["jit_emission_time1", "jit_emission_time2"], "class": "table_obj_value", "title": "Time spent by the statement on emitting code, in seconds.", "caption": "Time (s)"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "Time (s)", "columns": ['
              '{"id": ["total_plan_time1", "total_plan_time2"], "class": "table_obj_value", "title": "Time spent planning statement", "condition": "planning_times", "caption": "Plan"}, '
              '{"id": ["total_exec_time1", "total_exec_time2"], "class": "table_obj_value", "title": "Time spent executing statement", "caption": "Exec"}'
          ']}, '
          '{"class": "table_obj_name", "caption": "I/O time (s)", "condition": "io_times", "columns": ['
              '{"id": ["blk_read_time1", "blk_read_time2"], "class": "table_obj_value", "title": "Time spent reading blocks by statement", "caption": "Read"}, '
              '{"id": ["blk_write_time1", "blk_write_time2"], "class": "table_obj_value", "title": "Time spent writing blocks by statement", "caption": "Write"}'
          ']}'
      ']}]'::jsonb),
(2, 'sqlkcachehdr', 'sqlsthdr', 1200, 'rusage statistics', 'rusage statistics', 'kcachestatements', NULL, 'kcache_stat', NULL, NULL),
(2, 'sqlrusgcpu_t', 'sqlkcachehdr', 100, 'Top SQL by system and user time', 'Top SQL by system and user time', NULL, NULL, 'kcache_time', NULL,
  '[{'
    '"type": "row_table", '
    '"class": "diff", '
    '"source": "top_rusage_statements",'
    '"filter": {"type": "exists", "field": "ord_cpu_time"},'
    '"ordering": "ord_cpu_time",'
    '"limit": "topn",'
    '"columns": ['
        '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true}, '
        '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true}, '
        '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true}, '
        '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"}, '
        '{"title": "Userspace CPU", "caption": "User Time", "columns": ['
            '{"id": ["plan_user_time1", "plan_user_time2"], "class": "table_obj_value", "title": "User CPU time elapsed during planning", "caption": "Plan (s)", "condition": "rusage_planstats"}, '
            '{"id": ["exec_user_time1", "exec_user_time2"], "class": "table_obj_value", "title": "User CPU time elapsed during execution", "caption": "Exec (s)"}, '
            '{"id": ["user_time_pct1", "user_time_pct2"], "class": "table_obj_value", "title": "User CPU time elapsed by this statement as a percentage of total user CPU time", "caption": "%Total"}'
            ']}, '
        '{"title": "Kernelspace CPU", "caption": "System Time", "columns": ['
            '{"id": ["plan_system_time1", "plan_system_time2"], "class": "table_obj_value", "title": "System CPU time elapsed during planning", "caption": "Plan (s)", "condition": "rusage_planstats"}, '
            '{"id": ["exec_system_time1", "exec_system_time2"], "class": "table_obj_value", "title": "System CPU time elapsed during execution", "caption":"Exec (s)"}, '
            '{"id": ["system_time_pct1", "system_time_pct2"], "class": "table_obj_value", "title": "System CPU time elapsed by this statement as a percentage of total system CPU time", "caption": "%Total"}'
            ']}'
        ']'
    '}]'::jsonb),
(2, 'sqlrusgio', 'sqlkcachehdr', 200, 'Top SQL by reads/writes done by filesystem layer', 'Top SQL by reads/writes done by filesystem layer', NULL, NULL, 'kcache_reads_writes', NULL,
  '[{'
      '"type": "row_table",'
      '"class": "diff",'
      '"source": "top_rusage_statements",'
      '"filter": {"type": "exists", "field": "ord_io_bytes"},'
      '"ordering": "ord_io_bytes",'
      '"limit": "topn",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "hdr mono queryId", "caption": "Query ID", "rowspan": true},'
          '{"id": "dbname", "class": "hdr", "caption": "Database", "rowspan": true},'
          '{"id": "username", "class": "hdr", "caption": "User", "rowspan": true},'
          '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval", "caption": "I"},'
          '{"title": "Filesystem reads", "caption": "Read Bytes", "columns": ['
              '{"id": ["plan_reads1", "plan_reads2"], "class": "table_obj_value", "title": "Filesystem read amount during planning", "caption": "Plan", "condition": "rusage_planstats"},'
              '{"id": ["exec_reads1", "exec_reads2"], "class": "table_obj_value", "title": "Filesystem read amount during execution", "caption": "Bytes"},'
              '{"id": ["reads_total_pct1", "reads_total_pct2"], "class": "table_obj_value", "title": "Filesystem read amount of this statement as a percentage of all statements FS read amount", "caption": "%Total"}'
          ']},'
          '{"title": "Filesystem writes", "caption": "Write Bytes", "columns": ['
              '{"id": ["plan_writes1", "plan_writes2"], "class": "table_obj_value", "title": "Filesystem write amount during planning", "caption": "Plan", "condition": "rusage_planstats"},'
             '{"id": ["exec_writes1", "exec_writes2"], "class": "table_obj_value", "title": "Filesystem write amount during execution", "caption": "Bytes"},'
             '{"id": ["writes_total_pct1", "writes_total_pct2"], "class": "table_obj_value", "title": "Filesystem write amount of this statement as a percentage of all statements FS read amount", "caption": "%Total"}'
          ']}'
      ']'
    '}]'::jsonb),
(2, 'sqllist', 'sqlsthdr', 1300, 'Complete list of SQL texts', 'Complete list of SQL texts', NULL, NULL, 'sql_list', NULL,
  '[{'
      '"type": "row_table", '
      '"class": "stmtlist", '
      '"source": "queries",'
      '"columns": ['
          '{"id": "hexqueryid", "class": "mono queryTextId", "caption": "Query ID", "rowspan": true}, '
          '{"id": ["query_text1", "query_text2", "query_text3"], "class": "mono queryText", "caption": "Query Text"}'
      ']'
  '}]'::jsonb)
;

-- Schema objects section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(2, 'tblscan', 'objects', 100, 'Top tables by estimated sequentially scanned volume', 'Top tables by estimated sequentially scanned volume', NULL, NULL, 'scanned_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_seq_scan"},'
    '"ordering": "ord_seq_scan",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "rowspan":true, "class": "hdr"},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Table", "class": "table_obj_value", "columns": ['
       '{"caption": "~SeqBytes", "id": ["seqscan_bytes_pretty1", "seqscan_bytes_pretty2"], '
         '"class": "table_obj_value", '
         '"title": "Estimated number of bytes, fetched by sequential scans on this table"}, '
       '{"caption": "SeqScan", "id": ["seq_scan1", "seq_scan2"], '
         '"class": "table_obj_value", '
         '"title": "Number of sequential scans initiated on this table"}, '
       '{"caption": "IxScan", "id": ["idx_scan1", "idx_scan2"], '
         '"class": "table_obj_value", '
         '"title": "Number of index scans initiated on this table"}, '
       '{"caption": "IxFet", "id": ["idx_tup_fetch1", "idx_tup_fetch2"], '
         '"class": "table_obj_value", '
         '"title": "Number of live rows fetched by index scans from this table"} '
     ']},'
     '{"caption": "TOAST", "class": "table_obj_value", "columns": ['
       '{"caption": "~SeqBytes", "id": ["t_seqscan_bytes_pretty1", "t_seqscan_bytes_pretty2"], '
         '"class": "table_obj_value", '
         '"title": "Estimated number of bytes, fetched by sequential scans on TOAST table"}, '
       '{"caption": "SeqScan", "id": ["toastseq_scan1", "toastseq_scan2"], '
         '"class": "table_obj_value", '
         '"title": "Number of sequential scans initiated on TOAST table"}, '
       '{"caption": "IxScan", "id": ["toastidx_scan1", "toastidx_scan2"], '
         '"class": "table_obj_value", '
         '"title": "Number of index scans initiated on TOAST table"}, '
       '{"caption": "IxFet", "id": ["toastidx_tup_fetch1", "toastidx_tup_fetch2"], '
         '"class": "table_obj_value", '
         '"title": "Number of live rows fetched by index scans from TOAST table"} '
     ']}'
    ']'
  '}]'::jsonb),
(2, 'tblfetch', 'objects', 200, 'Top tables by blocks fetched', 'Top tables by blocks fetched', NULL, NULL, 'fetch_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_io_tables",'
    '"filter": {"type": "exists", "field": "ord_fetch"},'
    '"ordering": "ord_fetch",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Heap", "columns": ['
          '{"caption": "Blks", "id": ["heap_blks_fetch1", "heap_blks_fetch2"], "title": "Number of blocks fetched (read+hit) from this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["heap_blks_proc_pct1", "heap_blks_proc_pct2"], "title": "Heap blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Ix", "columns": ['
          '{"caption": "Blks", "id": ["idx_blks_fetch1", "idx_blks_fetch2"], "title": "Number of blocks fetched (read+hit) from all indexes on this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["idx_blks_fetch_pct1", "idx_blks_fetch_pct2"], "title": "Indexes of blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST", "columns": ['
          '{"caption": "Blks", "id": ["toast_blks_fetch1", "toast_blks_fetch2"], "title": "Number of blocks fetched (read+hit) from this table''s TOAST table (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["toast_blks_fetch_pct1", "toast_blks_fetch_pct2"], "title": "TOAST blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST-Ix", "columns": ['
          '{"caption": "Blks", "id": ["tidx_blks_fetch1", "tidx_blks_fetch2"], "title": "Number of blocks fetched (read+hit) from this table''s TOAST table indexes (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["tidx_blks_fetch_pct1", "tidx_blks_fetch_pct2"], "title": "TOAST table index blocks fetched for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb),
(2, 'tblrd', 'objects', 300, 'Top tables by blocks read', 'Top tables by blocks read', NULL, NULL, 'read_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_io_tables",'
    '"filter": {"type": "exists", "field": "ord_read"},'
    '"ordering": "ord_read",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Heap", "columns": ['
          '{"caption": "Blks", "id": ["heap_blks_read1", "heap_blks_read2"], "title": "Number of blocks read from this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["heap_blks_read_pct1", "heap_blks_read_pct2"], "title": "Heap blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Ix", "columns": ['
          '{"caption": "Blks", "id": ["idx_blks_read1", "idx_blks_read2"], "title": "Number of blocks read from all indexes on this table", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["idx_blks_read_pct1", "idx_blks_read_pct2"], "title": "Indexes of blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST", "columns": ['
          '{"caption": "Blks", "id": ["toast_blks_read1", "toast_blks_read2"], "title": "Number of blocks read from this table''s TOAST table (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["toast_blks_read_pct1", "toast_blks_read_pct2"], "title": "TOAST blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST-Ix", "columns": ['
          '{"caption": "Blks", "id": ["tidx_blks_read1", "tidx_blks_read2"], "title": "Number of blocks read from this table''s TOAST table indexes (if any)", "class": "table_obj_value"},'
          '{"caption": "%Total", "id": ["tidx_blks_read_pct1", "tidx_blks_read_pct2"], "title": "TOAST table index blocks read for this table as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Hit(%)", "id": ["hit_pct1", "hit_pct2"], "class": "table_obj_value", "title": "Number of heap, indexes, toast and toast index blocks fetched from shared buffers as a percentage of all their blocks fetched from shared buffers and file system"}'
    ']'
  '}]'::jsonb),
(2, 'tbldml', 'objects', 400, 'Top DML tables', 'Top DML tables', NULL, NULL, 'dml_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_dml"},'
    '"ordering": "ord_dml",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Table", "columns": ['
          '{"caption": "Ins", "id": ["n_tup_ins1", "n_tup_ins2"], "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
          '{"caption": "Del", "id": ["n_tup_del1", "n_tup_del2"], "title": "Number of rows deleted", "class": "table_obj_value"},'
          '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST", "columns": ['
          '{"caption": "Ins", "id": ["toastn_tup_ins1", "toastn_tup_ins2"], "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"caption": "Upd", "id": ["toastn_tup_upd1", "toastn_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
          '{"caption": "Del", "id": ["toastn_tup_del1", "toastn_tup_del2"], "title": "Number of rows deleted", "class": "table_obj_value"},'
          '{"caption": "Upd(HOT)", "id": ["toastn_tup_hot_upd1", "toastn_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb),
(2, 'tblud', 'objects', 500, 'Top tables by updated/deleted tuples', 'Top tables by updated/deleted tuples', NULL, NULL, 'upd_del_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_upd"},'
    '"ordering": "ord_upd",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"},'
     '{"caption": "Del", "id": ["n_tup_del1", "n_tup_del2"], "title": "Number of rows deleted", "class": "table_obj_value"},'
     '{"caption": "Vacuum", "id": ["vacuum_count1", "vacuum_count2"], "title": "Number of times this table has been manually vacuumed (not counting VACUUM FULL)", "class": "table_obj_value"},'
     '{"caption": "AutoVacuum", "id": ["autovacuum_count1", "autovacuum_count2"], "title": "Number of times this table has been vacuumed by the autovacuum daemon", "class": "table_obj_value"},'
     '{"caption": "Analyze", "id": ["analyze_count1", "analyze_count2"], "title": "Number of times this table has been manually analyzed", "class": "table_obj_value"},'
     '{"caption": "AutoAnalyze", "id": ["autoanalyze_count1", "autoanalyze_count2"], "title": "Number of times this table has been analyzed by the autovacuum daemon", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(2, 'tblupd_np', 'objects', 550, 'Top tables by new-page updated tuples', 'Top tables by new-page updated tuples', 'table_new_page_updates', NULL, 'upd_np_tbl',
  '<p>Top tables by number of rows updated where the successor version goes onto a new heap page, '
  'leaving behind an original version with a <i>t_ctid</i> field that points to a different heap page. '
  'These are always non-HOT updates.</p>',
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_upd_np"},'
    '"ordering": "ord_upd_np",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "NP Upd", "id": ["n_tup_newpage_upd1", "n_tup_newpage_upd2"], "title": "Number of rows updated to a new heap page", "class": "table_obj_value"},'
     '{"caption": "%Upd", "id": ["np_upd_pct1", "np_upd_pct2"], "title": "Number of new-page updated rows as a percentage of all rows updated", "class": "table_obj_value"},'
     '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(2, 'tblgrw', 'objects', 600, 'Top growing tables', 'Top growing tables', NULL, NULL, 'growth_tbl',
  '<ul><li>Sizes in square brackets are based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_growth"},'
    '"ordering": "ord_growth",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Table", "columns": ['
          '{"caption": "Size", "id": ["relsize_pretty1", "relsize_pretty2"], "title": "Table size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
          '{"caption": "Growth", "id": ["growth_pretty1", "growth_pretty2"], "title": "Table size increment during report interval", "class": "table_obj_value"},'
          '{"caption": "Ins", "id": ["n_tup_ins1", "n_tup_ins2"], "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
          '{"caption": "Del", "id": ["n_tup_del1", "n_tup_del2"], "title": "Number of rows deleted", "class": "table_obj_value"},'
          '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
     ']},'
     '{"caption": "TOAST", "columns": ['
          '{"caption": "Size", "id": ["t_relsize_pretty1", "t_relsize_pretty2"], "title": "Table size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
          '{"caption": "Growth", "id": ["toastgrowth_pretty1", "toastgrowth_pretty2"], "title": "Table size increment during report interval", "class": "table_obj_value"},'
          '{"caption": "Ins", "id": ["toastn_tup_ins1", "toastn_tup_ins2"], "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"caption": "Upd", "id": ["toastn_tup_upd1", "toastn_tup_upd2"], "title": "Number of rows updated (includes HOT updated rows)", "class": "table_obj_value"},'
          '{"caption": "Del", "id": ["toastn_tup_del1", "toastn_tup_del2"], "title": "Number of rows deleted", "class": "table_obj_value"},'
          '{"caption": "Upd(HOT)", "id": ["toastn_tup_hot_upd1", "toastn_tup_hot_upd2"], "title": "Number of rows HOT updated (i.e., with no separate index update required)", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb),
(2, 'ixfetch', 'objects', 700, 'Top indexes by blocks fetched', 'Top indexes by blocks fetched', NULL, NULL, 'fetch_idx', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_io_indexes",'
    '"filter": {"type": "exists", "field": "ord_fetch"},'
    '"ordering": "ord_fetch",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "Index", "id": "indexrelname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Scans", "id": ["idx_scan1", "idx_scan2"], "title": "Number of scans performed on index", "class": "table_obj_value"},'
     '{"caption": "Blks", "id": ["idx_blks_fetch1", "idx_blks_fetch2"], "title": "Number of blocks fetched (read+hit) from this index", "class": "table_obj_value"},'
     '{"caption": "%Total", "id": ["idx_blks_fetch_pct1", "idx_blks_fetch_pct2"], "title": "Blocks fetched from this index as a percentage of all blocks fetched in a cluster", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(2, 'ixrd', 'objects', 800, 'Top indexes by blocks read', 'Top indexes by blocks read', NULL, NULL, 'read_idx', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_io_indexes",'
    '"filter": {"type": "exists", "field": "ord_read"},'
    '"ordering": "ord_read",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "Index", "id": "indexrelname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Scans", "id": ["idx_scan1", "idx_scan2"], "title": "Number of scans performed on index", "class": "table_obj_value"},'
     '{"caption": "Blks Reads", "id": ["idx_blks_read1", "idx_blks_read2"], "title": "Number of disk blocks read from this index", "class": "table_obj_value"},'
     '{"caption": "%Total", "id": ["idx_blks_read_pct1", "idx_blks_read_pct2"], "title": "Blocks fetched from this index as a percentage of all blocks read in a cluster", "class": "table_obj_value"}, '
     '{"caption": "Hits(%)", "id": ["idx_blks_hit_pct1", "idx_blks_hit_pct2"], "title": "Index blocks buffer cache hit percentage", "class": "table_obj_value"}'
    ']'
  '}]'::jsonb),
(2, 'ixgrw', 'objects', 900, 'Top growing indexes', 'Top growing indexes', NULL, NULL, 'growth_idx',
  '<ul><li>Sizes in square brackets are based on <i>pg_class.relpages</i> data instead of <i>pg_relation_size()</i> function</li></ul>{func_output}',
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_indexes",'
    '"filter": {"type": "exists", "field": "ord_growth"},'
    '"ordering": "ord_growth",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "Index", "id": "indexrelname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Index", "columns": ['
          '{"id": ["indexrelsize_pretty1", "indexrelsize_pretty2"], "caption": "Size", "title": "Index size, as it was at the moment of last sample in report interval", "class": "table_obj_value"},'
          '{"id": ["growth_pretty1", "growth_pretty2"], "caption": "Growth", "title": "Index size increment during report interval", "class": "table_obj_value"}'
     ']},'
     '{"caption": "Table", "columns": ['
          '{"id": ["tbl_n_tup_ins1", "tbl_n_tup_ins2"], "caption": "Ins", "title": "Number of rows inserted", "class": "table_obj_value"},'
          '{"id": ["tbl_n_tup_upd1", "tbl_n_tup_upd2"], "caption": "Upd", "title": "Number of rows updated (without HOT updated rows)", "class": "table_obj_value"},'
          '{"id": ["tbl_n_tup_del1", "tbl_n_tup_del2"], "caption": "Del", "title": "Number of rows deleted", "class": "table_obj_value"}'
     ']}'
    ']'
  '}]'::jsonb)
;

-- Functions section of a differential report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(2, 'func_t', 'funchdr', 100, 'Top functions by total time', 'Top functions by total time', NULL, NULL, 'funcs_time_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_time"},'
    '"ordering": "ord_time",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Function", "id": "funcname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Executions", "id": ["calls1", "calls2"], "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": ["self_time1", "self_time2"], "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": ["m_time1", "m_time2"], "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": ["m_stime1", "m_stime2"], "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb),
(2, 'func_c', 'funchdr', 200, 'Top functions by executions', 'Top functions by executions', NULL, NULL, 'funcs_calls_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_calls"},'
    '"ordering": "ord_calls",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Function", "id": "funcname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Executions", "id": ["calls1", "calls2"], "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": ["self_time1", "self_time2"], "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": ["m_time1", "m_time2"], "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": ["m_stime1", "m_stime2"], "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb),
(2, 'func_trg', 'funchdr', 300, 'Top trigger functions by total time', 'Top trigger functions by total time', 'trigger_function_stats', NULL, 'trg_funcs_time_stat', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_functions",'
    '"filter": {"type": "exists", "field": "ord_trgtime"},'
    '"ordering": "ord_trgtime",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Function", "id": "funcname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Executions", "id": ["calls1", "calls2"], "title": "Number of times this function has been called", "class": "table_obj_value"},'
     '{"caption": "Time (s)", "title": "Function execution timing statistics", "columns": ['
          '{"caption": "Total", "id": ["total_time1", "total_time2"], "class": "table_obj_value", "title": "Time spent in this function and all other functions called by it"},'
          '{"caption": "Self", "id": ["self_time1", "self_time2"], "class": "table_obj_value", "title": "Time spent in this function itself, not including other functions called by it"},'
          '{"caption": "Mean", "id": ["m_time1", "m_time2"], "class": "table_obj_value", "title": "Mean total time per execution"},'
          '{"caption": "Mean self", "id": ["m_stime1", "m_stime2"], "class": "table_obj_value", "title": "Mean self time per execution"}'
     ']}'
    ']'
  '}]'::jsonb)
;

-- Vacuum section of a regular report
INSERT INTO report_struct (
  report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
  content, sect_struct)
VALUES
(2, 'vacops', 'vachdr', 100, 'Top tables by vacuum operations', 'Top tables by vacuum operations', NULL, NULL, 'top_vacuum_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_vac"},'
    '"ordering": "ord_vac",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Vacuum count", "id": ["vacuum_count1", "vacuum_count2"], "class": "table_obj_value", "title": "Number of times this table has been manually vacuumed (not counting VACUUM FULL)"},'
     '{"caption": "Autovacuum count", "id": ["autovacuum_count1", "autovacuum_count2"], "class": "table_obj_value", "title": "Number of times this table has been vacuumed by the autovacuum daemon"},'
     '{"caption": "Ins", "id": ["n_tup_ins1", "n_tup_ins2"], "class": "table_obj_value", "title": "Number of rows inserted"},'
     '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "class": "table_obj_value", "title": "Number of rows updated (includes HOT updated rows)"},'
     '{"caption": "Del", "id": ["n_tup_del1", "n_tup_del2"], "class": "table_obj_value", "title": "Number of rows deleted"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "class": "table_obj_value", "title": "Number of rows HOT updated (i.e., with no separate index update required)"}'
    ']'
  '}]'::jsonb),
(2, 'anops', 'vachdr', 200, 'Top tables by analyze operations', 'Top tables by analyze operations', NULL, NULL, 'top_analyze_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_tables",'
    '"filter": {"type": "exists", "field": "ord_anl"},'
    '"ordering": "ord_anl",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "I", "id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "class": "interval"},'
     '{"caption": "Analyze count", "id": ["analyze_count1", "analyze_count2"], "class": "table_obj_value", "title": "Number of times this table has been manually analyzed"},'
     '{"caption": "Autoanalyze count", "id": ["autoanalyze_count1", "autoanalyze_count2"], "class": "table_obj_value", "title": "Number of times this table has been analyzed by the autovacuum daemon"},'
     '{"caption": "Ins", "id": ["n_tup_ins1", "n_tup_ins2"], "class": "table_obj_value", "title": "Number of rows inserted"},'
     '{"caption": "Upd", "id": ["n_tup_upd1", "n_tup_upd2"], "class": "table_obj_value", "title": "Number of rows updated (includes HOT updated rows)"},'
     '{"caption": "Del", "id": ["n_tup_del1", "n_tup_del2"], "class": "table_obj_value", "title": "Number of rows deleted"},'
     '{"caption": "Upd(HOT)", "id": ["n_tup_hot_upd1", "n_tup_hot_upd2"], "class": "table_obj_value", "title": "Number of rows HOT updated (i.e., with no separate index update required)"}'
    ']'
  '}]'::jsonb),
(2, 'ixvacest', 'vachdr', 300, 'Top indexes by estimated vacuum load', 'Top indexes by estimated vacuum load', NULL, NULL, 'top_ix_vacuum_bytes_cnt_tbl', NULL,
  '[{'
    '"type": "row_table",'
    '"class": "diff",'
    '"source": "top_indexes",'
    '"filter": {"type": "exists", "field": "ord_vac"},'
    '"ordering": "ord_vac",'
    '"limit": "topn",'
    '"columns": ['
     '{"caption": "DB", "id": "dbname", "class": "hdr", "rowspan": true},'
     '{"caption": "Tablespace", "id": "tablespacename", "class": "hdr", "rowspan": true},'
     '{"caption": "Schema", "id": "schemaname", "class": "hdr", "rowspan": true},'
     '{"caption": "Table", "id": "relname", "class": "hdr", "rowspan": true},'
     '{"caption": "Index", "id": "indexrelname", "class": "hdr", "rowspan": true},'
     '{"id": ["1", "2"], "title":["properties.timePeriod1", "properties.timePeriod2"], "caption": "I", "class": "interval"},'
     '{"id": ["vacuum_bytes_pretty1", "vacuum_bytes_pretty2"], "caption": "Vacuum bytes", "class": "table_obj_value", "title": "Estimated implicit vacuum load caused by table indexes"},'
     '{"id": ["vacuum_count1", "vacuum_count2"], "caption": "Vacuum cnt", "class": "table_obj_value", "title": "Vacuum count on underlying table"},'
     '{"id": ["autovacuum_count1", "autovacuum_count2"], "caption": "Autovacuum cnt", "class": "table_obj_value", "title": "Autovacuum count on underlying table"},'
     '{"id": ["avg_indexrelsize_pretty1", "avg_indexrelsize_pretty2"], "caption": "IX size", "class": "table_obj_value", "title": "Average index size during report interval"},'
     '{"id": ["avg_relsize_pretty1", "avg_relsize_pretty2"], "caption": "Relsize", "class": "table_obj_value", "title": "Average relation size during report interval"}'
    ']'
  '}]'::jsonb)
;

-- Settings sections
INSERT INTO report_struct(
    report_id, sect_id, parent_sect_id, s_ord, toc_cap, tbl_cap, feature, function_name, href,
    content, sect_struct)
VALUES
(2, 'definedset', 'settings', 800, NULL, NULL, NULL, NULL, NULL, NULL,
  '[{'
    '"type": "row_table",'
    '"class": "setlist",'
    '"source": "settings",'
    '"filter": {"type": "exists", "field": "defined_val"},'
    '"columns": ['
     '{"caption": "Defined settings", "columns": ['
          '{"caption": "Setting", "id": "name", "class": "table_obj_value"},'
          '{"caption": "reset_val", "id": "reset_val", "class": "table_obj_value switch_bold"},'
          '{"caption": "Unit", "id": "unit", "class": "table_obj_value"},'
          '{"caption": "Source", "id": "source", "class": "table_obj_value"},'
          '{"caption": "Notes", "id": "notes", "class": "table_obj_value switch_bold"}'
     ']}'
    ']'
  '},'
  '{'
    '"type": "row_table",'
    '"class": "setlist",'
    '"source": "settings",'
    '"filter": {"type": "exists", "field": "default_val"},'
    '"columns": ['
     '{"caption": "Default settings", "columns": ['
          '{"caption": "Setting", "id": "name", "class": "table_obj_value"},'
          '{"caption": "reset_val", "id": "reset_val", "class": "table_obj_value switch_bold"},'
          '{"caption": "Unit", "id": "unit", "class": "table_obj_value"},'
          '{"caption": "Source", "id": "source", "class": "table_obj_value"},'
          '{"caption": "Notes", "id": "notes", "class": "table_obj_value switch_bold"}'
     ']}'
    ']'
  '}]'::jsonb)
;
GRANT USAGE ON SCHEMA @extschema@ TO public;
GRANT SELECT ON sample_stat_cluster TO public;
GRANT SELECT ON sample_stat_slru TO public;
GRANT SELECT ON sample_stat_wal TO public;
GRANT SELECT ON sample_stat_io TO public;
GRANT SELECT ON sample_stat_archiver TO public;
GRANT SELECT ON indexes_list TO public;
GRANT SELECT ON sample_stat_indexes TO public;
GRANT SELECT ON sample_stat_indexes_total TO public;
GRANT SELECT ON tablespaces_list TO public;
GRANT SELECT ON sample_stat_tablespaces TO public;
GRANT SELECT ON tables_list TO public;
GRANT SELECT ON sample_stat_tables TO public;
GRANT SELECT ON sample_stat_tables_total TO public;
GRANT SELECT ON sample_settings TO public;
GRANT SELECT ON funcs_list TO public;
GRANT SELECT ON sample_stat_user_functions TO public;
GRANT SELECT ON sample_stat_user_func_total TO public;
GRANT SELECT ON sample_stat_database TO public;
GRANT SELECT ON sample_statements TO public;
GRANT SELECT ON sample_statements_total TO public;
GRANT SELECT ON sample_kcache TO public;
GRANT SELECT ON sample_kcache_total TO public;
GRANT SELECT ON roles_list TO public;
GRANT SELECT ON wait_sampling_total TO public;
GRANT SELECT (server_id, server_name, server_description, server_created, db_exclude,
  enabled, max_sample_age, last_sample_id, size_smp_wnd_start, size_smp_wnd_dur, size_smp_interval)
  ON servers TO public;
GRANT SELECT ON samples TO public;
GRANT SELECT ON baselines TO public;
GRANT SELECT ON bl_samples TO public;
GRANT SELECT ON report_static TO public;
GRANT SELECT ON report TO public;
GRANT SELECT ON report_struct TO public;
GRANT SELECT ON v_sample_stat_indexes TO public;
GRANT SELECT ON v_sample_stat_tablespaces TO public;
GRANT SELECT ON v_sample_timings TO public;
GRANT SELECT ON v_sample_stat_tables TO public;
GRANT SELECT ON v_sample_settings TO public;
GRANT SELECT ON v_sample_stat_user_functions TO public;

-- pg_read_all_stats can see the query texts
GRANT SELECT ON stmt_list TO pg_read_all_stats;
