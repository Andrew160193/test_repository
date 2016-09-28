CREATE OR REPLACE PACKAGE PKG_CORE_REPORTS AS 

--------------------------------------------------
--Пакет в отчетности не используется
---------------------------------------------------

--------------------------------------------------------------------------------
-- Формирование списка экспертов с текущим статусом наумен
--------------------------------------------------------------------------------
CURSOR cur_get_experts_status(
        i_operator_name VARCHAR2
        , i_operator_number NUMBER
) IS
WITH
project_operators AS ( --ищем список активных экспертов
        SELECT 
        /*+ index(ius IDX_IP_USERS_LOGIN)*/
                ius.ID 
                , ius.login
                , exp.full_name
--                , exp.login
                , exp.phone_number
        FROM inc_call_d_experts exp 
        LEFT JOIN common.ip_users ius ON exp.login=ius.login AND exp.is_active=1
        where (lower(exp.full_name) like '%'||lower(i_operator_name)||'%' or i_operator_name is null)
                and (exp.phone_number = i_operator_number or i_operator_number is null)
)
--формируем список последних статусов операторов проекта
SELECT
        po.login
        , MAX(po.full_name) AS full_name
        , MAX(po.phone_number) AS phone_number
        , MAX(CAST(ua.dates AS TIMESTAMP)) KEEP (DENSE_RANK LAST ORDER BY ua.ID) AS dates
        , MAX(nvl(ab.sense, 'Отключён')) KEEP (DENSE_RANK LAST ORDER BY ua.ID) AS sense  
        , MAX(nvl(ab1.sense, 'Отключён')) KEEP (DENSE_RANK LAST ORDER BY ua.ID) AS subsense
        , MAX(nvl(ab1.isaviable, 'N')) KEEP (DENSE_RANK LAST ORDER BY ua.ID) AS isaviable
FROM project_operators po
LEFT JOIN common.ip_user_activity ua ON po.ID=ua.id_login AND ua.dates BETWEEN SYSDATE - INTERVAL '10' HOUR AND SYSDATE
LEFT JOIN common.d_abonentstatuses ab ON ab.code=ua.state
LEFT JOIN common.d_abonentstatuses ab1 ON ab1.code=ua.substate
GROUP BY po.login
ORDER BY decode(subsense, 'Ожидание', 1, 'Разговор', 2, 'Отключён', 4, 3), full_name
;


TYPE pt_get_experts_status IS TABLE OF cur_get_experts_status%rowtype;

FUNCTION fnc_get_experts_status (
        i_operator_name VARCHAR2
        , i_operator_number NUMBER
)
RETURN pt_get_experts_status pipelined;


--------------------------------------------------------------------------------
-- ЛОГ ЗВОНКОВ: Основная часть
--------------------------------------------------------------------------------

  CURSOR cur_calls_log_core 
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_id_call_result NUMBER
  , i_id_result_group NUMBER
  , i_operator_login VARCHAR2
  ) IS
  WITH
  sq_results AS (
    SELECT
      fid_result AS id_result
    FROM core_groups_results_groups
    WHERE (fid_result = i_id_call_result OR i_id_call_result IS NULL) AND
          (fid_group = i_id_result_group OR i_id_result_group IS NULL)
  )
  SELECT 
    mvcl.*
  FROM v_core_calls mvcl
    LEFT JOIN sq_results sr ON mvcl.id_result = sr.id_result
  WHERE mvcl.call_start_time BETWEEN i_init_time AND i_finish_time
    AND (mvcl.operator_login = i_operator_login OR i_operator_login IS NULL)
    AND (i_id_call_result IS NULL OR mvcl.id_result = sr.id_result);
  
  TYPE t_calls_log_core IS TABLE OF cur_calls_log_core%rowtype;

  FUNCTION fnc_get_calls_log_core
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_id_call_result NUMBER DEFAULT NULL
  , i_id_result_group NUMBER DEFAULT NULL
  , i_operator_login VARCHAR2 DEFAULT NULL
  ) RETURN t_calls_log_core pipelined; 
  
--------------------------------------------------------------------------------
-- СТАТИСТИКА ПО ЗВОНКАМ: разбивка кол-ва звонков по часам
--------------------------------------------------------------------------------

  CURSOR cur_calls_stat_core 
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_period_type VARCHAR
  , i_period_step NUMBER
  ) IS
  WITH
-- Интервалы
  sq_hours AS (
    SELECT DISTINCT
      to_char(period_start_time, 'hh24:mi') || '-' || to_char(period_finish_time, 'hh24:mi') AS period_char
    , EXTRACT(HOUR FROM period_start_time) AS start_hour
    , EXTRACT(HOUR FROM period_finish_time) AS finish_hour
    FROM TABLE(
      PKG_GENERAL_REPORTS.fnc_get_periods_of_time(
        trunc(i_init_time, 'dd'), 
        trunc(i_init_time, 'dd') + INTERVAL '1' DAY, 
        'hour', 
        1)
    )   
  ),
-- Интервалы
  sq_dates AS (
    SELECT DISTINCT
      period_start_time,
      period_finish_time
    FROM TABLE(
      PKG_GENERAL_REPORTS.fnc_get_periods_of_time(
        i_init_time
      , i_finish_time 
      , i_period_type
      , i_period_step)
    )   
  ),  
  sq AS (  
    SELECT 
      to_char(d.period_start_time, 'dd.mm.yyyy hh24:mi') AS period_start_time
    , EXTRACT(HOUR FROM mvcl.call_start_time) AS start_hour
    , mvcl.id_call
    FROM v_core_calls mvcl
      JOIN sq_hours h ON EXTRACT(HOUR FROM mvcl.call_start_time) >= h.start_hour AND
                         EXTRACT(HOUR FROM mvcl.call_start_time) <  h.finish_hour  
      RIGHT JOIN sq_dates d ON mvcl.call_start_time BETWEEN d.period_start_time AND d.period_finish_time    
    WHERE mvcl.call_start_time BETWEEN i_init_time AND i_finish_time
      
  ),
  sq_res AS (
    SELECT 
      *
    FROM sq
    pivot (count(id_call) 
           FOR start_hour IN (0 AS hour_0, 1 AS hour_1, 2 AS hour_2, 
                              3 AS hour_3, 4 AS hour_4, 5 AS hour_5, 
                              6 AS hour_6, 7 AS hour_7, 8 AS hour_8, 
                              9 AS hour_9, 10 AS hour_10, 11 AS hour_11, 
                              12 AS hour_12, 13 AS hour_13, 14 AS hour_14, 
                              15 AS hour_15, 16 AS hour_16, 17 AS hour_17, 
                              18 AS hour_18, 19 AS hour_19, 20 AS hour_20, 
                              21 AS hour_21, 22 AS hour_22, 23 AS hour_23)
           )
  )
  SELECT
    decode(
      GROUPING(period_start_time),
      1, 'Итого:',
      period_start_time
    ) AS period_start_time      
  , sum(hour_0) AS hour_0
  , sum(hour_1) AS hour_1
  , sum(hour_2) AS hour_2
  , sum(hour_3) AS hour_3
  , sum(hour_4) AS hour_4
  , sum(hour_5) AS hour_5
  , sum(hour_6) AS hour_6
  , sum(hour_7) AS hour_7
  , sum(hour_8) AS hour_8
  , sum(hour_9) AS hour_9
  , sum(hour_10) AS hour_10
  , sum(hour_11) AS hour_11
  , sum(hour_12) AS hour_12
  , sum(hour_13) AS hour_13
  , sum(hour_14) AS hour_14
  , sum(hour_15) AS hour_15
  , sum(hour_16) AS hour_16
  , sum(hour_17) AS hour_17
  , sum(hour_18) AS hour_18
  , sum(hour_19) AS hour_19
  , sum(hour_20) AS hour_20
  , sum(hour_21) AS hour_21
  , sum(hour_22) AS hour_22
  , sum(hour_23) AS hour_23
  FROM sq_res
  GROUP BY ROLLUP(period_start_time);
  
  TYPE t_calls_stat_core IS TABLE OF cur_calls_stat_core%rowtype;

  FUNCTION fnc_get_calls_stat_core
  (
    i_init_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_group_lvl NUMBER
  ) RETURN t_calls_stat_core pipelined; 
  
--------------------------------------------------------------------------------
-- ЛОГ ПОПЫТОК ПЕРЕВОДА: Основная часть
--------------------------------------------------------------------------------

CURSOR cur_transfer_log_core 
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_transfer_phone VARCHAR2
  , i_operator_login VARCHAR2
  ) IS
WITH
-- Попытки перевода
  sq_transfers AS (
      SELECT 
        id_transfer
      , fid_call
      , transfer_start_time
      , transfer_finish_time
      , transfer_type
      , transfer_result
      , transfer_phone
      , caller
      , id_call
      , call_start_time
      , call_finish_time
      , project_id
      , operator_login
    FROM v_core_calls_transfers
    WHERE call_start_time BETWEEN i_init_time AND i_finish_time  
      AND (transfer_phone = i_transfer_phone OR i_transfer_phone IS NULL)
  ),
--Тоновые наборы в рамках каждого перевода  
  sq_tones AS (
    SELECT 
      st.id_transfer,
      listagg(cctd.phone, '; ') WITHIN GROUP (ORDER BY cctd.created_at) AS tone_number
    FROM core_calls_tone_dials cctd
      JOIN sq_transfers st ON st.fid_call = cctd.fid_call AND
           cctd.created_at >= st.transfer_start_time AND
           cctd.created_at < nvl(st.transfer_finish_time, cctd.created_at + INTERVAL '5' SECOND)
    GROUP BY st.id_transfer      
  )
  SELECT 
    cct.id_call
  , cct.call_start_time
  , cct.call_finish_time
  , PKG_GENERAL_REPORTS.intervaltosec(cct.call_finish_time - cct.call_start_time) AS call_length_sec
  , ceil(PKG_GENERAL_REPORTS.intervaltosec(cct.call_finish_time - cct.call_start_time)/60) AS call_length_min
  , cct.caller
  , cct.operator_login
  , cct.project_id
  , cct.id_transfer
  , cct.transfer_type
  , cct.transfer_phone
  , st.tone_number
  , cct.transfer_result
  FROM sq_transfers cct 
    LEFT JOIN sq_tones st ON st.id_transfer = cct.id_transfer
  WHERE (cct.operator_login = i_operator_login OR i_operator_login IS NULL);
  
  TYPE t_transfer_log_core IS TABLE OF cur_transfer_log_core%rowtype;

  FUNCTION fnc_get_transfer_log_core
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_transfer_phone VARCHAR2 DEFAULT NULL
  , i_operator_login VARCHAR2 DEFAULT NULL
  ) RETURN t_transfer_log_core pipelined; 

END pkg_core_reports;
/


CREATE OR REPLACE PACKAGE BODY PKG_CORE_REPORTS AS

--------------------------------------------------------------------------------
-- Формирование списка экспертов с текущим статусом наумен
--------------------------------------------------------------------------------
FUNCTION fnc_get_experts_status (
        i_operator_name VARCHAR2
        , i_operator_number NUMBER
)
RETURN pt_get_experts_status pipelined AS
BEGIN
--Проверяем открыт ли курсор
IF cur_get_experts_status%isopen THEN
--Если открыт, то закрываем
        CLOSE cur_get_experts_status;
END IF;

FOR r IN cur_get_experts_status(i_operator_name, i_operator_number)
loop
        pipe ROW(r);
END loop;
END fnc_get_experts_status;
--------------------------------------------------------------------------------
-- ЛОГ ЗВОНКОВ: Основная часть
--------------------------------------------------------------------------------

  FUNCTION fnc_get_calls_log_core
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_id_call_result NUMBER DEFAULT NULL
  , i_id_result_group NUMBER DEFAULT NULL
  , i_operator_login VARCHAR2 DEFAULT NULL
  ) RETURN t_calls_log_core pipelined AS
  BEGIN
    FOR l IN cur_calls_log_core(i_init_time, i_finish_time, i_id_call_result,
                                i_id_result_group, i_operator_login)
    loop
      pipe ROW(l) ;
    END loop;
  END fnc_get_calls_log_core;
  
--------------------------------------------------------------------------------
-- СТАТИСТИКА ПО ЗВОНКАМ: разбивка кол-ва звонков по часам
--------------------------------------------------------------------------------

  FUNCTION fnc_get_calls_stat_core
  (
    i_init_time TIMESTAMP,
    i_finish_time TIMESTAMP,
    i_group_lvl NUMBER
  ) RETURN t_calls_stat_core pipelined AS
    v_period_type VARCHAR(100) := '';
    v_period_step NUMBER := 0;
  BEGIN
    if i_group_lvl = 0 then       -- суммарно за весь период
          v_period_type := 'year';
          v_period_step := 90;
    elsif i_group_lvl = 1 then       -- группировать по дням
          v_period_type := 'day'; 
          v_period_step := 1;
    elsif i_group_lvl = 2 then       -- группировать по часам
          v_period_type := 'hour';
          v_period_step := 1;
    elsif i_group_lvl = 3 then       -- группировать по пятнадцатиминутным интервалам
          v_period_type := 'minute'; 
          v_period_step := 15;
    end if;
    
    FOR l IN cur_calls_stat_core(i_init_time, i_finish_time, v_period_type, v_period_step)
    loop
      pipe ROW(l) ;
    END loop;
  END fnc_get_calls_stat_core;
  
--------------------------------------------------------------------------------
-- ЛОГ ПОПЫТОК ПЕРЕВОДА: Основная часть
--------------------------------------------------------------------------------

  FUNCTION fnc_get_transfer_log_core
  (
    i_init_time TIMESTAMP
  , i_finish_time TIMESTAMP
  , i_transfer_phone VARCHAR2 DEFAULT NULL
  , i_operator_login VARCHAR2 DEFAULT NULL
  ) RETURN t_transfer_log_core pipelined AS
  BEGIN
    FOR l IN cur_transfer_log_core(i_init_time, i_finish_time, 
                                   i_transfer_phone, i_operator_login)
    loop
      pipe ROW(l) ;
    END loop;
  END fnc_get_transfer_log_core;

END PKG_CORE_REPORTS;
/
