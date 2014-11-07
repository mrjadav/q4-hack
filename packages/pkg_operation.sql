CREATE OR REPLACE PACKAGE pkg_operation
IS
   PROCEDURE martPerformanceReport;

END pkg_operation;
/

CREATE OR REPLACE PACKAGE BODY pkg_operation
IS
   PROCEDURE martPerformanceReport
   AS
      l_sla_date                 NUMBER;
      l_current_hour             VARCHAR2 (5) := TO_CHAR (SYSDATE, 'HH24');
      --l_date                     VARCHAR2 (30) := TO_CHAR (SYSDATE, 'YYYYMMDD');
      l_dim_pending_jobs         NUMBER := 0;
      l_ext_pending_jobs         NUMBER := 0;
      l_trf_pending_jobs         NUMBER := 0;
      l_pending_jobs             NUMBER := 0;
      l_jobs_launched            NUMBER := 0;
      l_failed_job               NUMBER := 0;
      l_failed_job_threshold     NUMBER := 10;
      l_mail_text                CLOB;
      l_mail_text_wait_time      CLOB;
      l_mail_text_active         CLOB;
      l_mail_text_enq_waits      CLOB;
      l_mail_text_cpu_util       CLOB;
      l_mail_text_resp_time      CLOB;
      l_mail_text_read_per_sec   CLOB;
      l_mail_text_temp_space     CLOB;
      l_mail_text_failed         CLOB;
      l_mail_gs_status           CLOB;
      l_top_query                CLOB;
      l_mail_logons              CLOB;
      l_wait_events              CLOB;
      l_subject                  VARCHAR2 (4000);
      l_data                     snapshot_tab := snapshot_tab ();
      l_sql_id                   sql_id_tab := sql_id_tab ();
      l_sql_text                 CLOB;
      l_dbid                     NUMBER;
      l_top_snap                 PLS_INTEGER;
      l_txt                      CLOB;
      l_cnt                      PLS_INTEGER := 0;
      l_date                     DATE := SYSDATE - (14 / 24);
      l_begin_time               DATE;
      l_end_time                 DATE;
   BEGIN
      SELECT MIN (BEGIN_INTERVAL_TIME), MAX (END_INTERVAL_TIME)
        INTO l_begin_time, l_end_time
        FROM dba_hist_snapshot
       WHERE END_INTERVAL_TIME > l_date;

      l_subject :=
            'Mart Performance Report - ['
         || TO_CHAR (l_begin_time, 'DD/MM:HH24')
         || ' - '
         || TO_CHAR (l_end_time, 'DD/MM:HH24')
         || ']';

      SELECT dbid INTO l_dbid FROM v$database;

      DBMS_LOB.createtemporary (l_mail_text, TRUE);
      DBMS_LOB.createtemporary (l_mail_logons, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_wait_time, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_active, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_enq_waits, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_cpu_util, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_resp_time, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_read_per_sec, TRUE);
      DBMS_LOB.createtemporary (l_mail_text_temp_space, TRUE);
      DBMS_LOB.createtemporary (l_top_query, TRUE);
      DBMS_LOB.createtemporary (l_wait_events, TRUE);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (SELECT a.SNAP_ID,
                              a.BEGIN_INTERVAL_TIME,
                              a.END_INTERVAL_TIME,
                              a.IST_TIME,
                              a.PST_TIME,
                              "1" - LAG ("1") OVER (ORDER BY a.SNAP_ID) node1,
                              "2" - LAG ("2") OVER (ORDER BY a.SNAP_ID) node2,
                              "3" - LAG ("3") OVER (ORDER BY a.SNAP_ID) node3,
                              "4" - LAG ("4") OVER (ORDER BY a.SNAP_ID) node4,
                              "5" - LAG ("5") OVER (ORDER BY a.SNAP_ID) node5,
                              "6" - LAG ("6") OVER (ORDER BY a.SNAP_ID) node6
                         FROM (WITH tab_snaps
                                       AS (  SELECT DBID,
                                                    INSTANCE_NUMBER,
                                                    SNAP_ID,
                                                    BEGIN_INTERVAL_TIME,
                                                    END_INTERVAL_TIME,
                                                      end_interval_time
                                                    + (5 / 24)
                                                    + (30 / 1440)
                                                       IST_TIME,
                                                      end_interval_time
                                                    - (8 / 24)
                                                    - (30 / 1440)
                                                       PST_TIME,
                                                    ROW_NUMBER ()
                                                       OVER (
                                                          PARTITION BY instance_number
                                                          ORDER BY
                                                             BEGIN_INTERVAL_TIME DESC)
                                                       rnk
                                               FROM dba_hist_snapshot
                                              WHERE END_INTERVAL_TIME > l_date
                                           ORDER BY instance_number,
                                                    BEGIN_INTERVAL_TIME DESC),
                                    tab_b
                                       AS (SELECT a.INSTANCE_NUMBER,
                                                  a.SNAP_ID,
                                                  a.BEGIN_INTERVAL_TIME,
                                                  a.END_INTERVAL_TIME,
                                                  a.IST_TIME,
                                                  a.PST_TIME,
                                                  VALUE
                                             FROM    tab_snaps A
                                                  INNER JOIN
                                                     DBA_HIST_SYSSTAT b
                                                  ON (a.dbid = b.dbid
                                                      AND b.snap_id =
                                                            a.snap_id
                                                      AND b.instance_number =
                                                            a.instance_number)
                                            WHERE stat_name =
                                                     'logons cumulative')
                                 SELECT a.SNAP_ID,
                                        TO_CHAR (a.BEGIN_INTERVAL_TIME,
                                                 'DD/MM:HH24')
                                           BEGIN_INTERVAL_TIME,
                                        TO_CHAR (a.END_INTERVAL_TIME,
                                                 'DD/MM:HH24')
                                           END_INTERVAL_TIME,
                                        TO_CHAR (a.IST_TIME, 'DD/MM:HH24')
                                           IST_TIME,
                                        TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                                           PST_TIME,
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 1
                                                     THEN
                                                        VALUE
                                                  END))
                                           "1",
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 2
                                                     THEN
                                                        VALUE
                                                  END))
                                           "2",
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 3
                                                     THEN
                                                        VALUE
                                                  END))
                                           "3",
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 4
                                                     THEN
                                                        VALUE
                                                  END))
                                           "4",
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 5
                                                     THEN
                                                        VALUE
                                                  END))
                                           "5",
                                        ROUND(SUM(CASE
                                                     WHEN instance_number = 6
                                                     THEN
                                                        VALUE
                                                  END))
                                           "6"
                                   FROM tab_b A
                               GROUP BY a.SNAP_ID,
                                        TO_CHAR (a.BEGIN_INTERVAL_TIME,
                                                 'DD/MM:HH24'),
                                        TO_CHAR (a.END_INTERVAL_TIME,
                                                 'DD/MM:HH24'),
                                        TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                        TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                               ORDER BY a.SNAP_ID) A)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Logons </b>'
               || '</CAPTION>';

            DBMS_LOB.writeappend (l_mail_logons, LENGTH (l_txt), l_txt);

            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';

            DBMS_LOB.writeappend (l_mail_logons, LENGTH (l_txt), l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('logons cumulative', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);

         DBMS_LOB.writeappend (l_mail_logons, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_logons, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Average Active Sessions"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name =
                                             'Average Active Sessions'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Average Active Sessions"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Average Active Sessions </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_active, LENGTH (l_txt), l_txt);

            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';

            DBMS_LOB.writeappend (l_mail_text_active, LENGTH (l_txt), l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Average Active Sessions', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_active, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_active, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Database Wait Time Ratio"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name =
                                             'Database Wait Time Ratio'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Database Wait Time Ratio"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Database Wait Time Ratio </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_wait_time,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_wait_time,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Database Wait Time Ratio', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_wait_time, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_wait_time, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Host CPU Utilization (%)"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name =
                                             'Host CPU Utilization (%)'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Host CPU Utilization (%)"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Host CPU Utilization(%) </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_cpu_util,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_cpu_util,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Host CPU Utilization (%)', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_cpu_util, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_cpu_util, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Enqueue Waits Per Sec"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name = 'Enqueue Waits Per Sec'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Enqueue Waits Per Sec"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Enqueue Waits Per Sec </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_enq_waits,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_enq_waits,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Enqueue Waits Per Sec', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_enq_waits, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_enq_waits, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      node1,
                      node2,
                      node3,
                      node4,
                      node5,
                      node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "SQL Service Response Time"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name =
                                             'SQL Service Response Time'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "SQL Service Response Time"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> SQL Service Response Time </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_resp_time,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_resp_time,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('SQL Service Response Time', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_resp_time, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_resp_time, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      ROUND (node1 / (1024 * 1024), 0) node1,
                      ROUND (node2 / (1024 * 1024), 0) node2,
                      ROUND (node3 / (1024 * 1024), 0) node3,
                      ROUND (node4 / (1024 * 1024), 0) node4,
                      ROUND (node5 / (1024 * 1024), 0) node5,
                      ROUND (node6 / (1024 * 1024), 0) node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                              end_interval_time
                                            - (8 / 24)
                                            - (30 / 1440)
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Phy Read Total Bytes Per Sec"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name =
                                             'Physical Read Total Bytes Per Sec'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Phy Read Total Bytes Per Sec"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY a.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Phy Read Total Bytes Per Sec (MB) </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_read_per_sec,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_read_per_sec,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Phy Read Total Bytes Per Sec', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_read_per_sec,
                               LENGTH (l_txt),
                               l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_read_per_sec, LENGTH (l_txt), l_txt);

      l_cnt := 0;

      FOR i
         IN (  SELECT SNAP_ID,
                      BEGIN_INTERVAL_TIME,
                      END_INTERVAL_TIME,
                      IST_TIME,
                      PST_TIME,
                      ROUND (node1 / (1024 * 1024), 0) node1,
                      ROUND (node2 / (1024 * 1024), 0) node2,
                      ROUND (node3 / (1024 * 1024), 0) node3,
                      ROUND (node4 / (1024 * 1024), 0) node4,
                      ROUND (node5 / (1024 * 1024), 0) node5,
                      ROUND (node6 / (1024 * 1024), 0) node6,
                      ROW_NUMBER () OVER (ORDER BY node1 DESC NULLS LAST)
                         node1_rank,
                      ROW_NUMBER () OVER (ORDER BY node2 DESC NULLS LAST)
                         node2_rank,
                      ROW_NUMBER () OVER (ORDER BY node3 DESC NULLS LAST)
                         node3_rank,
                      ROW_NUMBER () OVER (ORDER BY node4 DESC NULLS LAST)
                         node4_rank,
                      ROW_NUMBER () OVER (ORDER BY node5 DESC NULLS LAST)
                         node5_rank,
                      ROW_NUMBER () OVER (ORDER BY node6 DESC NULLS LAST)
                         node6_rank
                 FROM (WITH tab_snaps
                               AS (  SELECT DBID,
                                            INSTANCE_NUMBER,
                                            SNAP_ID,
                                            BEGIN_INTERVAL_TIME,
                                            END_INTERVAL_TIME,
                                              end_interval_time
                                            + (5 / 24)
                                            + (30 / 1440)
                                               IST_TIME,
                                            NEW_TIME (end_interval_time,
                                                      'GMT',
                                                      'PST')
                                               PST_TIME,
                                            ROW_NUMBER ()
                                               OVER (
                                                  PARTITION BY instance_number
                                                  ORDER BY
                                                     BEGIN_INTERVAL_TIME DESC)
                                               rnk
                                       FROM dba_hist_snapshot
                                      WHERE END_INTERVAL_TIME > l_date
                                            AND dbid = l_dbid
                                   ORDER BY instance_number,
                                            BEGIN_INTERVAL_TIME DESC),
                            tab_b
                               AS (SELECT a.INSTANCE_NUMBER,
                                          a.SNAP_ID,
                                          a.BEGIN_INTERVAL_TIME,
                                          a.END_INTERVAL_TIME,
                                          a.IST_TIME,
                                          a.PST_TIME,
                                          ROUND (b.average, 2)
                                             "Temp Space Used"
                                     FROM    tab_snaps A
                                          INNER JOIN
                                             DBA_HIST_SYSMETRIC_SUMMARY b
                                          ON (a.dbid = b.dbid
                                              AND b.snap_id = a.snap_id
                                              AND b.instance_number =
                                                    a.instance_number)
                                    WHERE metric_name = 'Temp Space Used'
                                          AND b.dbid = l_dbid)
                         SELECT a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24')
                                   BEGIN_INTERVAL_TIME,
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24')
                                   END_INTERVAL_TIME,
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24') IST_TIME,
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24') PST_TIME,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 1
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node1,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 2
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node2,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 3
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node3,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 4
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node4,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 5
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node5,
                                ROUND(SUM(CASE
                                             WHEN instance_number = 6
                                             THEN
                                                "Temp Space Used"
                                          END))
                                   node6
                           FROM tab_b A
                       GROUP BY a.SNAP_ID,
                                TO_CHAR (a.BEGIN_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.END_INTERVAL_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.IST_TIME, 'DD/MM:HH24'),
                                TO_CHAR (a.PST_TIME, 'DD/MM:HH24')
                       ORDER BY A.SNAP_ID)
             ORDER BY SNAP_ID)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Temp Space Used (MB) </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_mail_text_temp_space,
                                  LENGTH (l_txt),
                                  l_txt);
            l_txt :=
               '<TR>'
               || '<TD style="background-color:#C6DEFF;width=100;">Snap ID</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">Begin Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">End Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">IST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;width=100;">PST Time (DD/MM HH)</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#1</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#2</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#3</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#4</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#5</TD>'
               || CHR (10)
               || '<TD style="background-color:#C6DEFF;text-align:right;width=150;">Node#6</TD>'
               || CHR (10)
               || '</TR>';
            DBMS_LOB.writeappend (l_mail_text_temp_space,
                                  LENGTH (l_txt),
                                  l_txt);
         END IF;

         l_top_snap := 0;

         IF i.node1_rank IN (1, 2) AND NVL (i.node1, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 1, i.SNAP_ID);
         END IF;

         IF i.node2_rank IN (1, 2) AND NVL (i.node2, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 2, i.SNAP_ID);
         END IF;

         IF i.node3_rank IN (1, 2) AND NVL (i.node3, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 3, i.SNAP_ID);
         END IF;

         IF i.node4_rank IN (1, 2) AND NVL (i.node4, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 4, i.SNAP_ID);
         END IF;

         IF i.node5_rank IN (1, 2) AND NVL (i.node5, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 5, i.SNAP_ID);
         END IF;

         IF i.node6_rank IN (1, 2) AND NVL (i.node6, 0) > 0
         THEN
            l_top_snap := 1;
            l_data.EXTEND;
            l_data (l_data.COUNT) :=
               snapshot_typ ('Temp Space Used', 6, i.SNAP_ID);
         END IF;

         l_txt :=
            '<TR>'
            || CASE
                  WHEN l_top_snap = 1
                  THEN
                     '<TD style="background-color:#636f7f;text-align:right;color:#FFFFFF;width=100;">'
                  ELSE
                     '<TD style="text-align:right;width=100;">'
               END
            || i.SNAP_ID
            || '</TD>'
            || '<TD width=100>'
            || i.BEGIN_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.END_INTERVAL_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.IST_TIME
            || '</TD>'
            || '<TD width=100>'
            || i.PST_TIME
            || '</TD>'
            || CASE
                  WHEN i.node1_rank = 1 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node1_rank = 2 AND NVL (i.node1, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node1
            || '</TD>'
            || CASE
                  WHEN i.node2_rank = 1 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node2_rank = 2 AND NVL (i.node2, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node2
            || '</TD>'
            || CASE
                  WHEN i.node3_rank = 1 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node3_rank = 2 AND NVL (i.node3, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node3
            || '</TD>'
            || CASE
                  WHEN i.node4_rank = 1 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node4_rank = 2 AND NVL (i.node4, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node4
            || '</TD>'
            || CASE
                  WHEN i.node5_rank = 1 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node5_rank = 2 AND NVL (i.node5, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node5
            || '</TD>'
            || CASE
                  WHEN i.node6_rank = 1 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF0000;text-align:right;color:#FFFFFF;width=150;">'
                  WHEN i.node6_rank = 2 AND NVL (i.node6, 0) > 0
                  THEN
                     '<TD style="background-color:#FF8000;text-align:right;color:#FFFFFF;width=150;">'
                  ELSE
                     '<TD style="text-align:right;width=150;">'
               END
            || i.node6
            || '</TD>'
            || '</TR>'
            || CHR (10);
         DBMS_LOB.writeappend (l_mail_text_temp_space, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_mail_text_temp_space, LENGTH (l_txt), l_txt);

      BEGIN
         FOR i IN (  SELECT snap_id, inst_id, COUNT (1) cnt
                       FROM TABLE (CAST (l_data AS snapshot_tab))
                   GROUP BY snap_id, inst_id
                   ORDER BY inst_id, cnt DESC)
         LOOP
            DBMS_OUTPUT.put_line (
               'snap_id:' || i.snap_id || '-' || 'inst_id:' || i.inst_id);
            DBMS_APPLICATION_INFO.set_client_info (
               'snap_id:' || i.snap_id || '-' || 'inst_id:' || i.inst_id);

            FOR j IN (SELECT sql_id, active_sessions
                        FROM (  SELECT sql_id, COUNT ( * ) AS active_sessions
                                  FROM dba_hist_active_sess_history
                                 WHERE     snap_id = i.snap_id
                                       AND instance_number = i.inst_id
                                       AND dbid = l_dbid
                              GROUP BY sql_id
                                HAVING COUNT ( * ) > 120
                              ORDER BY active_sessions DESC))
            LOOP
               IF    (i.inst_id IN (1, 2) AND j.active_sessions > 1000)
                  OR (i.inst_id IN (6) AND j.active_sessions > 600)
                  OR (i.inst_id IN (3, 4, 5) AND j.active_sessions > 120)
               THEN
                  l_sql_id.EXTEND;
                  l_sql_id (l_sql_id.COUNT) :=
                     sql_id_typ (j.sql_id,
                                 i.snap_id,
                                 i.inst_id,
                                 j.active_sessions);
               END IF;
            END LOOP;
         END LOOP;
      END;

      l_cnt := 0;

      FOR i IN (SELECT *
                  FROM (  SELECT inst_id,
                                 sql_id,
                                 snap_id,
                                 active_sessions,
                                 ROW_NUMBER ()
                                    OVER (
                                       PARTITION BY inst_id
                                       ORDER BY active_sessions DESC NULLS LAST)
                                    rnk
                            FROM TABLE (CAST (l_sql_id AS sql_id_tab))
                        ORDER BY inst_id, active_sessions DESC NULLS LAST)
                 WHERE rnk <= 3)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Top Queries </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_top_query, LENGTH (l_txt), l_txt);
            l_txt :=
               '<TR>'
               || '<TD bgcolor="#C6DEFF" align=right width=100> Instance</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align="left" width=100> Sql Id</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right width=100> Snap Id</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right width=100> Active Time (Seconds)</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align="left" width="800"> SQL Text</TD>'
               || CHR (10)
               || '</TR>'
               || CHR (10);
            DBMS_LOB.writeappend (l_top_query, LENGTH (l_txt), l_txt);
         END IF;

         BEGIN
            SELECT REPLACE (sql_text, '  ', ' ')
              INTO l_sql_text
              FROM DBA_HIST_SQLTEXT
             WHERE sql_id = i.sql_id AND dbid = l_dbid AND ROWNUM = 1;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               l_sql_text := NULL;
         END;

         l_txt :=
               '<TR>'
            || '<TD align=right width=100>'
            || i.inst_id
            || '</TD>'
            || '<TD align="left" width=100>'
            || i.sql_id
            || '</TD>'
            || '<TD align=right width=100>'
            || i.snap_id
            || '</TD>'
            || '<TD align=right width=100>'
            || i.active_sessions
            || '</TD>'
            || '<TD align="left" width="800">'
            || l_sql_text
            || '</TD>'
            || '</TR>'
            || CHR (10);

         DBMS_LOB.writeappend (l_top_query, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_top_query, LENGTH (l_txt), l_txt);

      l_cnt := 0;
      l_txt := NULL;

      FOR i
         IN (  SELECT "Instance",
                      "Snap Id",
                      "Snap Time",
                      "Snap End Time",
                      "IST Time",
                      "PST Time",
                      "Event Name",
                      "Total Waits",
                      "Total Timeouts",
                      "Time (s)",
                      "Avg Wait (ms)",
                      "%Total Call Time",
                      "Wait Class"
                 FROM (SELECT instance_number AS "Instance",
                              snap_id AS "Snap Id",
                              CASE
                                 WHEN rn = 1
                                 THEN
                                    TO_CHAR (begin_interval_time,
                                             'DD/MM:HH24')
                                 ELSE
                                    NULL
                              END
                                 AS "Snap Time",
                              CASE
                                 WHEN rn = 1
                                 THEN
                                    TO_CHAR (end_interval_time,
                                             'DD/MM:HH24')
                                 ELSE
                                    NULL
                              END
                                 AS "Snap End Time",
                              CASE
                                 WHEN rn = 1
                                 THEN
                                    TO_CHAR (ist_time, 'DD/MM:HH24')
                                 ELSE
                                    NULL
                              END
                                 AS "IST Time",
                              CASE
                                 WHEN rn = 1
                                 THEN
                                    TO_CHAR (pst_time, 'DD/MM:HH24')
                                 ELSE
                                    NULL
                              END
                                 AS "PST Time",
                              CASE
                                 WHEN event_name = 'DB CPU' THEN 'CPU Time'
                                 ELSE event_name
                              END
                                 AS "Event Name",
                              total_waits AS "Total Waits",
                              total_timeouts AS "Total Timeouts",
                              ROUND (time_waited / 1000000) "Time (s)",
                              ROUND( (ROUND (time_waited / 100000)
                                      / CASE
                                           WHEN total_waits = 0 THEN NULL
                                           ELSE total_waits
                                        END
                                      * 100))
                                 "Avg Wait (ms)",
                              ROUND (
                                 (time_waited / total_time_waited) * 100,
                                 1)
                                 "%Total Call Time",
                              rn,
                              wait_class AS "Wait Class"
                         FROM (  SELECT a.instance_number,
                                        a.snap_id,
                                        a.begin_interval_time,
                                        a.end_interval_time,
                                        a.IST_TIME,
                                        a.pst_time,
                                        a.event_name,
                                        a.total_waits - a.lag_total_waits
                                           total_waits,
                                        a.total_timeouts
                                        - a.lag_total_timeouts
                                           total_timeouts,
                                        a.time_waited_micro
                                        - a.lag_time_waited_micro
                                           time_waited,
                                        ROW_NUMBER ()
                                           OVER (
                                              PARTITION BY a.instance_number,
                                                           a.snap_id
                                              ORDER BY
                                                 a.instance_number,
                                                 a.begin_interval_time,
                                                 a.time_waited_micro
                                                 - a.lag_time_waited_micro DESC NULLS LAST)
                                           rn,
                                        SUM(CASE
                                               WHEN a.event_name = 'DB CPU'
                                               THEN
                                                  a.time_waited_micro
                                                  - a.lag_time_waited_micro
                                               ELSE
                                                  a.time_waited_micro
                                                  - a.lag_time_waited_micro
                                            END)
                                           OVER (
                                              PARTITION BY a.instance_number,
                                                           a.snap_id)
                                           total_time_waited,
                                        a.wait_class
                                   FROM (WITH tab_snaps
                                                 AS (  SELECT /*+ MATERIALIZE */
                                                             DBID,
                                                              INSTANCE_NUMBER,
                                                              SNAP_ID,
                                                              BEGIN_INTERVAL_TIME,
                                                              END_INTERVAL_TIME,
                                                              end_interval_time
                                                              + (5 / 24)
                                                              + (30 / 1440)
                                                                 IST_TIME,
                                                              NEW_TIME (
                                                                 end_interval_time,
                                                                 'GMT',
                                                                 'PST')
                                                                 PST_TIME,
                                                              ROW_NUMBER ()
                                                                 OVER (
                                                                    PARTITION BY instance_number
                                                                    ORDER BY
                                                                       BEGIN_INTERVAL_TIME DESC)
                                                                 rn
                                                         FROM dba_hist_snapshot
                                                        WHERE END_INTERVAL_TIME >
                                                                 l_date
                                                              AND dbid =
                                                                    2533394332
                                                     ORDER BY instance_number,
                                                              BEGIN_INTERVAL_TIME DESC),
                                              tab_b
                                                 AS (SELECT /*+ MATERIALIZE */
                                                           a.INSTANCE_NUMBER,
                                                            a.SNAP_ID,
                                                            a.BEGIN_INTERVAL_TIME,
                                                            a.END_INTERVAL_TIME,
                                                            a.IST_TIME,
                                                            a.PST_TIME,
                                                            b.event_name,
                                                            b.total_waits,
                                                            b.total_timeouts,
                                                            b.wait_class,
                                                            b.TIME_WAITED_MICRO,
                                                            LAG(total_waits)
                                                               OVER (
                                                                  PARTITION BY a.instance_number,
                                                                               b.event_name
                                                                  ORDER BY
                                                                     b.SNAP_ID)
                                                               lag_total_waits,
                                                            LAG(total_timeouts)
                                                               OVER (
                                                                  PARTITION BY a.instance_number,
                                                                               b.event_name
                                                                  ORDER BY
                                                                     b.SNAP_ID)
                                                               lag_total_timeouts,
                                                            LAG(TIME_WAITED_MICRO)
                                                               OVER (
                                                                  PARTITION BY a.instance_number,
                                                                               b.event_name
                                                                  ORDER BY
                                                                     b.SNAP_ID)
                                                               lag_TIME_WAITED_MICRO
                                                       FROM    tab_snaps A
                                                            INNER JOIN
                                                               DBA_HIST_SYSTEM_EVENT b
                                                            ON (a.dbid =
                                                                   b.dbid
                                                                AND b.snap_id =
                                                                      a.snap_id
                                                                AND b.instance_number =
                                                                      a.instance_number)
                                                      WHERE wait_class <>
                                                               'Idle'),
                                              tab_c
                                                 AS (SELECT /*+ MATERIALIZE */
                                                           a.INSTANCE_NUMBER,
                                                            a.SNAP_ID,
                                                            a.BEGIN_INTERVAL_TIME,
                                                            a.END_INTERVAL_TIME,
                                                            a.IST_TIME,
                                                            a.PST_TIME,
                                                            b.stat_name
                                                               event_name,
                                                            NULL total_waits,
                                                            NULL
                                                               total_timeouts,
                                                            NULL wait_class,
                                                            b.VALUE
                                                               time_waited_micro,
                                                            NULL
                                                               lag_total_waits,
                                                            NULL
                                                               lag_total_timeouts,
                                                            LAG(VALUE)
                                                               OVER (
                                                                  PARTITION BY a.instance_number
                                                                  ORDER BY
                                                                     b.SNAP_ID)
                                                               lagTIME_WAITED_MICRO
                                                       FROM    tab_snaps A
                                                            INNER JOIN
                                                               dba_hist_sys_time_model b
                                                            ON (a.dbid =
                                                                   b.dbid
                                                                AND b.snap_id =
                                                                      a.snap_id
                                                                AND b.instance_number =
                                                                      a.instance_number)
                                                      WHERE a.end_interval_time >
                                                               l_date
                                                            AND b.stat_name =
                                                                  'DB CPU')
                                           SELECT * FROM tab_b
                                         UNION ALL
                                           SELECT * FROM tab_c) A
                               ORDER BY instance_number,
                                        begin_interval_time,
                                        a.time_waited_micro
                                        - a.lag_time_waited_micro DESC NULLS LAST))
                WHERE rn < 4
                      AND ("Instance", "Snap Id") IN
                               (SELECT DISTINCT inst_id, snap_id
                                  FROM TABLE (CAST (l_sql_id AS sql_id_tab)))
             ORDER BY "Snap Id", "Instance", rn)
      LOOP
         IF l_cnt = 0
         THEN
            l_cnt := 1;
            l_txt :=
               '<table border=1 style="font-family: Courier;"> <CAPTION style="background-color:#636f7f;text-align:left;color:#FFFFFF;"> <b> Wait Events </b>'
               || '</CAPTION>';
            DBMS_LOB.writeappend (l_wait_events, LENGTH (l_txt), l_txt);
         END IF;

         IF i."Snap Time" IS NOT NULL
         THEN
            l_txt :=
                  '<TR>'
               || '<TD bgcolor="#C6DEFF" align=right> Snap Id</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> Begin Time</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> End Time</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> IST Time</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> PST Time</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> Instance</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> Event Name</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> Total Waits</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> Total Timeouts</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> Time (s)</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> Avg Wait (ms)</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF" align=right> %Total Call Time</TD>'
               || CHR (10)
               || '<TD bgcolor="#C6DEFF"> Wait Class</TD>'
               || CHR (10)
               || '</TR>'
               || CHR (10);
            DBMS_LOB.writeappend (l_wait_events, LENGTH (l_txt), l_txt);
         END IF;

         l_txt :=
            '<TR>' || '<TD style="text-align:right;">'
            || CASE
                  WHEN i."Snap Time" IS NOT NULL THEN i."Snap Id"
                  ELSE NULL
               END
            || '</TD>'
            || '<TD>'
            || i."Snap Time"
            || '</TD>'
            || '<TD>'
            || i."Snap End Time"
            || '</TD>'
            || '<TD>'
            || i."IST Time"
            || '</TD>'
            || '<TD>'
            || i."PST Time"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || i."Instance"
            || '</TD>'
            || '<TD>'
            || i."Event Name"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || i."Total Waits"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || i."Total Timeouts"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || i."Time (s)"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || i."Avg Wait (ms)"
            || '</TD>'
            || '<TD style="text-align:right;">'
            || TO_CHAR (i."%Total Call Time", '999.99')
            || '</TD>'
            || '<TD>'
            || i."Wait Class"
            || '</TD>'
            || '</TR>'
            || CHR (10);

         DBMS_LOB.writeappend (l_wait_events, LENGTH (l_txt), l_txt);
      END LOOP;

      l_txt := '</table>';
      DBMS_LOB.writeappend (l_wait_events, LENGTH (l_txt), l_txt);

      l_txt := '<!DOCTYPE html> <html><head><style> </style> </head> <body>';
      DBMS_LOB.writeappend (l_mail_text, LENGTH (l_txt), l_txt);

      IF l_mail_logons IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_logons),
                                  l_mail_logons);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_logons := 'Failed to write l_mail_logons: -06502';
                  DBMS_LOB.writeappend (l_mail_text,
                                        DBMS_LOB.getlength (l_mail_logons),
                                        l_mail_logons);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_logons:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_active IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_text_active),
                                  l_mail_text_active);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_active :=
                     'Failed to write l_mail_text_active: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_active),
                     l_mail_text_active);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_active:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_wait_time IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_text_wait_time),
                                  l_mail_text_wait_time);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_wait_time :=
                     'Failed to write l_mail_text_wait_time: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_wait_time),
                     l_mail_text_wait_time);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_wait_time:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_cpu_util IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_text_cpu_util),
                                  l_mail_text_cpu_util);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_cpu_util :=
                     'Failed to write l_mail_text_cpu_util: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_cpu_util),
                     l_mail_text_cpu_util);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_cpu_util:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_resp_time IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_text_resp_time),
                                  l_mail_text_resp_time);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_resp_time :=
                     'Failed to write l_mail_text_resp_time: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_resp_time),
                     l_mail_text_resp_time);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_resp_time:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_temp_space IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (
               l_mail_text,
               DBMS_LOB.getlength (l_mail_text_temp_space),
               l_mail_text_temp_space);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_temp_space :=
                     'Failed to write l_mail_text_temp_space: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_temp_space),
                     l_mail_text_temp_space);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_temp_space:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_read_per_sec IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (
               l_mail_text,
               DBMS_LOB.getlength (l_mail_text_read_per_sec),
               l_mail_text_read_per_sec);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_read_per_sec :=
                     'Failed to write l_mail_text_read_per_sec: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_read_per_sec),
                     l_mail_text_read_per_sec);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_read_per_sec:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_mail_text_enq_waits IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_mail_text_enq_waits),
                                  l_mail_text_enq_waits);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_mail_text_enq_waits :=
                     'Failed to write l_mail_text_enq_waits: -06502';
                  DBMS_LOB.writeappend (
                     l_mail_text,
                     DBMS_LOB.getlength (l_mail_text_enq_waits),
                     l_mail_text_enq_waits);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_mail_text_enq_waits:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_top_query IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_top_query),
                                  l_top_query);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_top_query := 'Failed to write l_top_query: -06502';
                  DBMS_LOB.writeappend (l_mail_text,
                                        DBMS_LOB.getlength (l_top_query),
                                        l_top_query);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_top_query:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      IF l_wait_events IS NOT NULL
      THEN
         BEGIN
            DBMS_LOB.writeappend (l_mail_text,
                                  LENGTH ('<br><br>'),
                                  '<br><br>');
            DBMS_LOB.writeappend (l_mail_text,
                                  DBMS_LOB.getlength (l_wait_events),
                                  l_wait_events);
         EXCEPTION
            WHEN OTHERS
            THEN
               IF SQLCODE = -06502
               THEN
                  l_wait_events := 'Failed to write l_wait_events: -06502';
                  DBMS_LOB.writeappend (l_mail_text,
                                        DBMS_LOB.getlength (l_wait_events),
                                        l_wait_events);
               ELSE
                  RAISE_APPLICATION_ERROR (
                     -20500,
                        'Failed to write l_wait_events:'
                     || SQLCODE
                     || '-'
                     || SQLERRM);
               END IF;
         END;
      END IF;

      l_txt := '</body> </html>';
      DBMS_LOB.writeappend (l_mail_text, LENGTH (l_txt), l_txt);

      IF l_mail_text IS NOT NULL
      THEN
         DECLARE
            l_sender_mail_id      VARCHAR2 (100);
            l_recipient_mail_id   VARCHAR2 (100);
         BEGIN
            l_sender_mail_id :=
               pkg_alert_and_monitoring.get_parameter_value (
                  'SENDER_MAIL_ID');
            l_recipient_mail_id :=
               pkg_etl_common_routines.get_param_value (
                  'dna-mart-performance-report',
                  'dna-mart-performance-report@yahoo-inc.com');
            pkg_notification.sp_html_email (p_to        => l_recipient_mail_id,
                                            p_from      => l_sender_mail_id,
                                            p_subject   => l_subject,
                                            p_text      => l_mail_text,
                                            p_html      => l_mail_text);
         END;
      END IF;

      DBMS_LOB.freetemporary (l_mail_text);
      DBMS_LOB.freetemporary (l_mail_text_wait_time);
      DBMS_LOB.freetemporary (l_mail_text_active);
      DBMS_LOB.freetemporary (l_mail_text_enq_waits);
      DBMS_LOB.freetemporary (l_mail_text_cpu_util);
      DBMS_LOB.freetemporary (l_mail_text_resp_time);
      DBMS_LOB.freetemporary (l_mail_text_read_per_sec);
      DBMS_LOB.freetemporary (l_mail_text_temp_space);
      DBMS_LOB.freetemporary (l_top_query);
      DBMS_LOB.freetemporary (l_wait_events);
   END martPerformanceReport;

END pkg_operation;
/
