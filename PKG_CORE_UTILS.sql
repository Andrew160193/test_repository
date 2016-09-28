CREATE OR REPLACE PACKAGE pkg_core_utils AS

------------------------------------------------------
--Вспомогательный пакет для отчетности
------------------------------------------------------

  FUNCTION intervaltosec (i_interval INTERVAL DAY TO SECOND) RETURN NUMBER;

  CURSOR cur_periods_of_time
  (
    i_start_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_ym_interval INTERVAL YEAR TO MONTH,
    i_ds_interval INTERVAL DAY TO SECOND
  ) IS
    WITH sq_periods AS
    (
      SELECT
        --если интервал типа year-to-month не указан,
        --то использовать интервал типа day-to-second
        CASE
          WHEN i_ym_interval IS NOT NULL THEN i_start_time+(ROWNUM-1)*i_ym_interval
          ELSE i_start_time+(ROWNUM-1)*i_ds_interval
        END AS period_start_time
      FROM dual
      CONNECT BY
        --производить выборку до тех пор пока начало временного отрезка
        --меньше заданной верхней границы
        (
          CASE
            WHEN i_ym_interval IS NOT NULL THEN i_start_time+(ROWNUM-1)*i_ym_interval
            ELSE i_start_time+(ROWNUM-1)*i_ds_interval
          END
        )<i_finish_time
    )
    SELECT
      --нижняя граница временного интервала
      period_start_time,
      --верхняя граница временного интервала;
      --если конец временного отрезка больше заданной верхней границы,
      --то мен¤ем текущее значение на значение верхней границы
      least
      (
        CASE
          WHEN i_ym_interval IS NOT NULL THEN
            period_start_time+i_ym_interval
          ELSE period_start_time+i_ds_interval
        END,
        i_finish_time
      ) AS period_finish_time
    FROM sq_periods;

  TYPE pt_periods_of_time IS TABLE OF cur_periods_of_time%rowtype;

  FUNCTION fnc_get_periods_of_time
  (
    i_start_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_step_type VARCHAR:='day',
    i_step_num NUMBER:=1
  )
  RETURN pt_periods_of_time
  pipelined;


  --------------------------------------------------------------
  -- ФУНКЦИЯ ПРЕОБРАЗОВАНИЯ СЕКУНД В ФОРМАТ "ЧАСЫ:МИНУТЫ:СЕКУНДЫ"
  --------------------------------------------------------------
  FUNCTION fnc_to_time_format
  (
  I_COUNT_SECOND NUMBER

  )RETURN VARCHAR2;


  ---------------------------------------------------------------
  -- Выбока звонков для пакета PKG_ACSI_REPORTS
  --------------------------------------------------------------

  CURSOR cur_get_nau_calls_data
 (
    i_init_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_projectid VARCHAR2,
    i_phones VARCHAR2,
    i_is_need_inner_calls NUMBER DEFAULT 0,
    i_linefilter VARCHAR2,
    i_skill_group VARCHAR2
 ) is
      WITH
      ic AS
        (SELECT /*+ parallel (c 4) OPT_PARAM('_B_TREE_BITMAP_PLANS','FALSE') */
           C.id,
           leg_id,
           C.session_id,
           base_id,
           protocol,
           src_ip,
           src_port,
           dst_ip,
           dst_port,
           src_id,
           dst_id,
           src_abonent,
           dst_abonent,
           src_abonent_type,
           dst_abonent_type,
           incoming,
           intrusion,
           created,
           connected,
           ended,
           voip_reason,
           internal_reason,
           fid_project_id
         FROM naucrm.call_legs c
             JOIN common.d_project_phones ph ON ph.phone = dst_id
                                                AND c.created BETWEEN ph.begin_time AND ph.end_time
                                                AND ph.FID_PROJECTSADDINF_ID = 2905 --индивидуально для каждого проекта
             JOIN common.d_projectsaddinf pi ON pi.ID = ph.fid_projectsaddinf_id
             /*JOIN common.d_project_work_time pw ON pw.fid_project_phones_id = ph.ID
                                                AND c.created BETWEEN pw.init_time AND pw.final_time
             LEFT JOIN naucrm.call_params bcp ON bcp.session_id = c.session_id
                                                 AND bcp.param_name = 'black_list' */
         WHERE  c.created >= i_init_time
             AND c.created <= i_finish_time
             AND pi.status = 'ACTIVE'
             --AND ((substr(c.src_id, -10) NOT IN ('4957392201') OR c.src_id IS NULL OR LENGTH(c.src_id) < 10 /**/) OR i_is_need_inner_calls = 1)
             and c.src_id not in ('4957392201','957392201')
             AND (i_projectid IS NULL AND fid_project_id = 'project245')
            -- AND pi.dirrection  = 'IN'
             AND (i_phones IS NULL OR dst_id IN (SELECT * FROM TABLE(common.strutils.fnc_gettblfromstring(i_phones, ','))))
             AND c.src_abonent_type = 'UNKNOWN'
             AND c.incoming = '1'
             AND ((mod(to_number(to_char(c.created,'J')),7)+1 IN (1,2,3,4,5) /*AND (c.created BETWEEN trunc(c.created) + pw.begin_operating_time_weekdays AND trunc(c.created) + pw.end_operating_time_weekdays)*/)
               OR (mod(to_number(to_char(c.created,'J')),7)+1 IN (6,7)/* AND c.created BETWEEN trunc(c.created) + pw.begin_operating_time_holidays AND trunc(c.created) + pw.end_operating_time_holidays*/))
            -- AND COALESCE(bcp.param_value, '*') <> '1'
         ) ,

        qc AS (
          SELECT q.*,
            count(q.first_leg_id) OVER (PARTITION BY q.session_id) AS rows_count,
            row_number() OVER (PARTITION BY q.session_id ORDER BY q.enqueued_time) AS q_rownum
          FROM /*ic i
               JOIN*/ naucrm.queued_calls q --ON q.session_id = i.session_id
          WHERE q.enqueued_time BETWEEN i_init_time - INTERVAL '5' MINUTE
            AND i_finish_time + INTERVAL '5' MINUTE
            AND q.project_id = 'project245'
         ),

        icc AS (
           select session_id, fid_project_id from ic),

        ic2 AS (
           SELECT
             --ic.*,
             session_id,
             /*NVL(qc.project_id, ic.fid_project_id) AS*/ project_id,
             next_leg_id,
             qc.unblocked_time AS enqueued_time,
             qc.dequeued_time,
             row_number() OVER (PARTITION BY qc.session_id ORDER BY qc.enqueued_time) AS call_rownum
           FROM qc
           where NOT(unblocked_time IS NULL AND q_rownum < qc.rows_count AND qc.rows_count > 1)
            ),

        cl2 AS (
           SELECT
             cl.*
           FROM  /*ic
                JOIN*/ naucrm.call_legs cl --ON cl.session_id = i.session_id
           WHERE cl.intrusion = 0
             AND cl.created >= i_init_time
             AND cl.created <= i_finish_time + INTERVAL '5' MINUTE
             AND cl.src_abonent_type = 'SS' AND cl.dst_abonent_type = 'SP'
            ),

--        ht AS (
--          SELECT
--            cs.session_id, cs.initiator_id AS login,
--            sum(naucrm.intervaltosec(cs.ended - cs.entered)) AS hold_time
--          FROM naucrm.call_status cs
--               --JOIN ic i ON i.session_id = cs.session_id
--          WHERE cs.state = 'hold' and
--                cs.entered BETWEEN i_init_time AND i_finish_time
--          GROUP BY cs.session_id, cs.initiator_id),

--        wrp AS (
--          SELECT sc.reason AS session_id, sc.login, sum(duration) AS wrp_time
--          FROM naucrm.status_changes sc --ON sc.reason = c.session_id
--          WHERE sc.status = 'wrapup' and
--                sc.entered BETWEEN i_init_time AND i_finish_time
--          GROUP BY sc.reason, sc.login),

        calls AS
          (SELECT
             --A.project_id,
             nvl(a.project_id, ic.fid_project_id) as project_id,
             ic.session_id AS call_id,
             nvl(A.call_rownum,1) AS call_rownum,
             ic.created AS call_init_time, -- = ivrconnected
             ic.ended AS ended_time,
             cl2.created AS opr_created_time,
             cl2.connected AS opr_connected_time,
             cl2.ended AS opr_ended_time,
             cl2.dst_abonent AS opr_login,
             cl2.src_id,
             A.enqueued_time,
             A.dequeued_time,
             A.dequeued_time - A.enqueued_time AS queue_time,
             --cl2.ended - cl2.connected AS talk_time,
             CASE
               WHEN ic.created < to_timestamp ('03.08.2015 16:00:00', 'dd.mm.yyyy hh24:mi:ss')
                  THEN ic.ended - cl2.connected
               ELSE cl2.ended - cl2.connected
             END AS talk_time,
             cl2.connected - cl2.created AS ring_time,
            -- nvl(ht.hold_time, 0) hold_time,
            -- decode(cl2.connected, null, null, wrp.wrp_time) AS wrp_time,
             ic.src_id AS abonent_phone,
             common.pkg_strutils.fnc_trytonumber(ic.src_id) AS abonent_phone_num,
             ic.dst_id AS project_phone,
             ic.voip_reason as abonent_sip_code,
             cl2.voip_reason AS opr_sip_code
           FROM ic
             left join ic2 A on a.session_id = ic.session_id
             LEFT JOIN cl2 ON cl2.session_id = A.session_id AND
                              cl2.leg_id = A.next_leg_id
           --  LEFT JOIN ht ON ht.session_id = A.session_id AND ht.login = cl2.dst_abonent
           --  LEFT JOIN wrp ON wrp.session_id = A.session_id AND
           --                      cl2.dst_abonent = wrp.login
                       ),
        itog AS (
           SELECT
             call_id,
             call_init_time,
--             call_rownum,
--             dequeued_time,
--             ended_time,
             enqueued_time,
--             hold_time,
             opr_connected_time,
--             opr_created_time,
--             opr_ended_time,
             opr_login,
             project_id,
--             queue_time,
--             ring_time,
--             talk_time,
--             wrp_time,
             abonent_phone
--             abonent_phone_num,
--             project_phone,
--             opr_sip_code,
--             abonent_sip_code,
--             lag(opr_connected_time) OVER(PARTITION BY call_id ORDER BY opr_created_time,call_init_time) prev_opr_connected_time,
--             lead(opr_connected_time) OVER(PARTITION BY call_id ORDER BY opr_created_time,call_init_time) next_opr_connected_time,
--             lead(enqueued_time) OVER(PARTITION BY call_id ORDER BY enqueued_time,call_init_time) next_enqueued_time,
--             lead(queue_time) OVER(PARTITION BY call_id ORDER BY enqueued_time,call_init_time) next_queue_time,
--             lag(src_id) OVER(PARTITION BY call_id ORDER BY opr_created_time,call_init_time) prev_src_id,
--             lead(src_id) OVER(PARTITION BY call_id ORDER BY opr_created_time,call_init_time) next_src_id
           FROM calls)

         SELECT *
         FROM itog
         WHERE (i_skill_group = project_id OR i_skill_group IS NULL) AND
               (project_id IN (SELECT * FROM TABLE(common.strutils.fnc_gettblfromstring(i_projectid, ',')))
               OR  (i_projectid IS NULL AND project_id = 'project245')
               )             
               
              --фильтры по проектам и скилл-группам д.б.здесь, иначе неверно определяются переводы звонков!
                AND call_id NOT IN ( 
--                                      'nauss7_1467360183_374_2398086',
--                                      'nauss4_1467362978_821_2426660',
--                                      'nauss2_1467643301_550_690014',
--                                      'nauss3_1467713371_999_2697510',
--                                      'nauss5_1467905197_775_914598',
                                      'nauss6_1469436419_830_2293956'
                
                                  )
              ;
   TYPE t_get_nau_calls_data IS TABLE OF cur_get_nau_calls_data%rowtype;

  FUNCTION fnc_get_nau_calls_data
  (
    i_init_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_linefilter VARCHAR2,
    i_skill_group VARCHAR2,
    i_is_need_inner_calls NUMBER DEFAULT 0

  ) RETURN t_get_nau_calls_data pipelined;

END pkg_core_utils;
/


CREATE OR REPLACE PACKAGE BODY pkg_core_utils AS

  FUNCTION intervaltosec (i_interval INTERVAL DAY TO SECOND ) RETURN NUMBER AS
  BEGIN
    RETURN EXTRACT( SECOND FROM i_interval )+
    EXTRACT( MINUTE FROM i_interval )*60+
    EXTRACT( HOUR FROM i_interval )*60*60+
    EXTRACT( DAY FROM i_interval )*60*60*24;
  END intervaltosec;

  FUNCTION fnc_get_periods_of_time
  (
    i_start_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_step_type VARCHAR,
    i_step_num NUMBER
  ) RETURN pt_periods_of_time
  pipelined AS
    v_step_type VARCHAR(10):='day';
    v_step_num NUMBER := 1;
    v_ym_int INTERVAL YEAR TO MONTH:=NULL;
    v_ds_int INTERVAL DAY TO SECOND:=NULL;
  BEGIN
    IF i_step_type IN ('second', 'minute', 'hour', 'day', 'month', 'year') THEN
      v_step_type := i_step_type;
    END IF;
    IF i_step_num > 0 THEN
      v_step_num := i_step_num;
    END IF;
    v_ym_int:=
      CASE
        WHEN v_step_type = 'month' THEN numtoyminterval(v_step_num,'month')
        WHEN v_step_type = 'year'  THEN numtoyminterval(v_step_num,'year')
        ELSE NULL
      END;
    v_ds_int:=
      CASE
        WHEN v_step_type = 'day'    THEN numtodsinterval(v_step_num, 'day')
        WHEN v_step_type = 'hour'   THEN numtodsinterval(v_step_num, 'hour')
        WHEN v_step_type = 'minute' THEN numtodsinterval(v_step_num, 'minute')
        WHEN v_step_type = 'second' THEN numtodsinterval(v_step_num, 'second')
        ELSE NULL
      END;
    IF cur_periods_of_time%isopen THEN
      CLOSE cur_periods_of_time;
    END IF;
    FOR r IN cur_periods_of_time
    (
      i_start_time  => i_start_time,
      i_finish_time => i_finish_time,
      i_ym_interval => v_ym_int,
      i_ds_interval => v_ds_int
    )
    loop
      pipe ROW(r);
    END loop;
  END fnc_get_periods_of_time;


  --------------------------------------------------------------
  -- ФУНКЦИЯ ПРЕОБРАЗОВАНИЯ СЕКУНД В ФОРМАТ "ЧАСЫ:МИНУТЫ:СЕКУНДЫ"
  --------------------------------------------------------------
  FUNCTION fnc_to_time_format
  (
  I_COUNT_SECOND NUMBER

  )RETURN VARCHAR2
  AS
  TIME_FORMAT VARCHAR2(100 CHAR) := '';
  BEGIN
   SELECT
   (CASE
    WHEN FLOOR(I_COUNT_SECOND/3600) < 10
     THEN '0' || TO_CHAR(FLOOR(I_COUNT_SECOND/3600))
     ELSE TO_CHAR(FLOOR(I_COUNT_SECOND/3600))
    END) ||':'||
  (CASE
    WHEN FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60) < 10
     THEN '0' || TO_CHAR(FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60))
     ELSE TO_CHAR(FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60))
    END) ||':'||
  (CASE
  WHEN (I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600 - FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60)*60) < 10
   THEN '0' || TO_CHAR(FLOOR(I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600 - FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60)*60))
   ELSE TO_CHAR(FLOOR(I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600 - FLOOR((I_COUNT_SECOND - FLOOR(I_COUNT_SECOND/3600)*3600)/60)*60))
  END) INTO TIME_FORMAT
  from DUAL;
  RETURN TIME_FORMAT;
  END fnc_to_time_format;



    FUNCTION fnc_get_nau_calls_data
  (
    i_init_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_linefilter VARCHAR2,
    i_skill_group VARCHAR2,
    i_is_need_inner_calls NUMBER DEFAULT 0
  ) RETURN t_get_nau_calls_data pipelined AS
  PRAGMA AUTONOMOUS_TRANSACTION;
    v_project_id VARCHAR2(100);
    v_phones VARCHAR(300);

  BEGIN

    IF i_linefilter = 1 THEN  --Для тетирования
      v_project_id := 'project245';
     v_phones     := '606775111010498';
    ELSE                                 -- Все линии (для рабочей версии)
      v_project_id := NULL;
      v_phones     := '4957392507,5555319863,5555319862,4957392209,5555392209,5555392210';
    END IF;

    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
    EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

  IF(cur_get_nau_calls_data%isopen) THEN CLOSE cur_get_nau_calls_data;
  END IF;

  FOR l IN cur_get_nau_calls_data(i_init_time,i_finish_time,v_project_id,v_phones,i_is_need_inner_calls,i_linefilter,i_skill_group)
      loop
        pipe ROW (l);
      END loop;

  END fnc_get_nau_calls_data;


END pkg_core_utils;
/
