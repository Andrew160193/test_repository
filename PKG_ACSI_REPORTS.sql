CREATE OR REPLACE PACKAGE PKG_ACSI_REPORTS AS 

--------------------------------------------------
--Пакет для отчетов по ACSI (ГИС ЖКХ / ZHKKH-528)
--------------------------------------------------
----Для ознакомления можно посмореть задачу RUSPOST-33 (создан по аналогии)
--------------------------------------------------------------------------
--ОТЧЕТ №1 "Статистика по голосовому меню «Опрос на удовлетворенность»" 
--------------------------------------------------------------------------

  CURSOR cur_get_inquiry_ivr_stats
(
  I_init_time TIMESTAMP, 
  I_finish_time TIMESTAMP,
  I_group_type NUMBER,
  I_period VARCHAR2,
  I_LOCATION VARCHAR2 := NULL
) IS
WITH
  GIS_ZHKH AS (SELECT * FROM DUAL),
  DATA_INC_CALL AS --ОБЩАЯ ВЫГРУЗКА ЗВОНКОВ
     (SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
      WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
        AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
     ),
     DATA_INC_CALL_2 AS --УБИРАЕМ ДУБЛИ
      (
        SELECT *
        FROM DATA_INC_CALL
        WHERE RN = 1
        
        and 
                --По заявке ZHKKH-490:
        --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
        --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
          (
          (CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and CALLER NOT IN ('4957392201','957392201'))
       OR ((CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
            CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(CALLER, -10) NOT IN ('4957392201'))
       OR (CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
          )
      ),
    dates AS 
        (
--        SELECT DISTINCT
--           CASE WHEN I_group_type = 0 THEN I_init_time ELSE period_start_time END dd1,
--           CASE WHEN I_group_type = 0 THEN I_finish_time ELSE period_finish_time END dd2
--         FROM TABLE(common.pkg_datetime_utils.fnc_get_periods_of_time(I_init_time, I_finish_time, I_period, 1))   
         
         
                 SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_init_time) AS TIMESTAMP) AS dd1,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS dd2,
          TO_CHAR(GREATEST(PERIOD_START_TIME, I_init_time),'dd.mm.yyyy hh24:mi') || ' - ' ||
          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                I_init_time,
                I_finish_time,
                NVL(LOWER(I_period), 'year'),
                DECODE(I_period,'minute',15,1)
                ))
         ),      
         
      all_calls AS
        (SELECT 
           cl.call_id AS session_id,
           cl.call_init_time,
           MAX(cl.enqueued_time) KEEP (DENSE_RANK LAST ORDER BY cl.opr_connected_time NULLS FIRST) AS enqueued_time,         
           MAX(cl.opr_connected_time) KEEP (DENSE_RANK LAST ORDER BY cl.opr_connected_time NULLS FIRST) AS opr_connected_time,
           MAX(cl.opr_login) KEEP (DENSE_RANK LAST ORDER BY cl.opr_connected_time NULLS FIRST) AS opr_login
         FROM 
              TABLE(PKG_GENERAL_REPORTS.fnc_get_nau_calls_data
                      (
                       I_init_time,  --    i_init_time TIMESTAMP, 
                       I_finish_time,                                  --    i_finish_time TIMESTAMP
                       null, --линии с ivr                             --    i_linefilter VARCHAR2
                       NULL,                                           --    i_skill_group VARCHAR2
                       0                                               --    i_is_need_inner_calls NUMBER DEFAULT 0
                       )) cl           
         GROUP BY cl.call_id, cl.call_init_time         
        ),    
                      
    PREP_call_params AS (
    SELECT cp.* 
    FROM naucrm.call_params cp
    JOIN all_calls cl
     ON cp.session_id = cl.session_id 
     AND cp.param_name IN ('CS1', 'CS2', 'CS3', 'CS4', 'CS5', 'CS6', 'OUT_CS2', 'OUT_CS3', 'OUT_CS4', 'OUT_CS5', 'OUT_CS6')
     AND cl.opr_connected_time IS NOT NULL
    WHERE (cp.changed >= I_init_time and cp.changed < I_finish_time + interval '30' minute)

       
       /*AND
          cp.changed >= cl.opr_connected_time*/
    
    ),    
                      
    ivr AS (
      SELECT
          dd1,
          dd2,
          cl.opr_login,
          cl.session_id,
          tab.session_id as session_id_inc_call,
          (CASE
            WHEN tab.CONNECT_RESULT_NUM = 2-- OR tab.CONNECT_RESULT_NUM IS NULL
            THEN tab.SESSION_ID
            ELSE NULL
          END) AS ENQUEUED_TIME,
          (CASE
                    WHEN (tab.CALL_RESULT_NUM = 1
                      AND tab.CONNECT_RESULT_NUM = 2)
                       -- OR tab.CONNECT_RESULT_NUM IS NULL OR tab.CALL_RESULT_NUM IS NULL-- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                    THEN tab.SESSION_ID
                    ELSE NULL
          END) AS opr_connected_time,
          cp.param_name,
          cp.param_value
      FROM dates d
      JOIN DATA_INC_CALL_2 tab --ЧТОБЫ СХОДИЛОСЬ С ДРУГИМИ ОТЧЕТАМИ      
       ON tab.CALL_CREATED >= dd1 AND tab.CALL_CREATED < dd2
      LEFT JOIN all_calls cl
        ON CL.SESSION_ID = tab.session_id 
      LEFT JOIN PREP_call_params cp 
        ON cp.session_id = cl.session_id
      LEFT JOIN CIS.NC_USERS OPR --таблица операторов
          ON OPR.LOGIN = CL.OPR_LOGIN                         
       
      where (OPR.FID_LOCATION = I_LOCATION OR I_LOCATION IS NULL)   
      ),   
      

    itog AS (
      SELECT
        GROUPING(to_char(d.dd1, 'dd.mm.yyyy hh24:mi')||' - '||to_char(d.dd2, 'dd.mm.yyyy hh24:mi')) g1,
        trunc(d.dd1) dd,
        CASE 
          WHEN GROUPING(trunc(d.dd1)) = 1 AND GROUPING(to_char(d.dd1,'dd.mm.yyyy hh24:mi')||' - '||to_char(d.dd2,'dd.mm.yyyy hh24:mi')) = 1
             THEN 'Итого' --общая итоговая строка отчета
          WHEN GROUPING(trunc(d.dd1)) = 0 AND GROUPING(to_char(d.dd1,'dd.mm.yyyy hh24:mi')||' - '||to_char(d.dd2,'dd.mm.yyyy hh24:mi')) = 1
             THEN 'Итого за '||to_char(MIN(d.dd1),'dd.mm.yyyy hh24:mi')||' - '||to_char(MAX(d.dd2),'dd.mm.yyyy hh24:mi') --итоговая строка при группировке по дням и часам
          ELSE to_char(d.dd1,'dd.mm.yyyy hh24:mi')||' - '||to_char(d.dd2,'dd.mm.yyyy hh24:mi') 
        END period,
        count(DISTINCT enqueued_time /*nvl2(enqueued_time, session_id_inc_call, NULL)*/) incomingcalls,
        count(DISTINCT opr_connected_time /*nvl2(opr_connected_time, session_id_inc_call, NULL)*/) answeredcalls,
        ---
--        count(DISTINCT decode(param_name, 'CS1', session_id)) cs1,    
--        count(DISTINCT decode(param_name, 'CS2', session_id)) cs2, 
        count(DISTINCT CASE WHEN param_name = 'CS1' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs1,
        count(DISTINCT CASE WHEN param_name = 'CS2' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs2,
        
        count(DISTINCT CASE WHEN param_name = 'OUT_CS2' AND param_value = '0' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs2,
        COUNT(DISTINCT CASE WHEN param_name = 'OUT_CS2' AND param_value IN (5,4,3,2,1) AND opr_connected_time IS NOT NULL THEN session_id END) amount_cs2,
        
       -- count(DISTINCT decode(param_name, 'CS3', session_id)) cs3, 
        count(DISTINCT CASE WHEN param_name = 'CS3' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs3,
        count(DISTINCT CASE WHEN param_name = 'OUT_CS3' AND param_value = '0' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs3, 
        COUNT(DISTINCT CASE WHEN param_name = 'OUT_CS3' AND param_value IN (5,4,3,2,1) AND opr_connected_time IS NOT NULL THEN session_id END) amount_cs3,
        
        --count(DISTINCT decode(param_name, 'CS4', session_id)) cs4,
        count(DISTINCT CASE WHEN param_name = 'CS4' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs4,
        count(DISTINCT CASE WHEN param_name = 'OUT_CS4' AND param_value = '0' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs4, 
        COUNT(DISTINCT CASE WHEN param_name = 'OUT_CS4' AND param_value IN (5,4,3,2,1) AND opr_connected_time IS NOT NULL THEN session_id END) amount_cs4,
        
       -- count(DISTINCT decode(param_name, 'CS5', session_id)) cs5,
        count(DISTINCT CASE WHEN param_name = 'CS5' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs5,
        count(DISTINCT CASE WHEN param_name = 'OUT_CS5' AND param_value = '0' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs5,
        COUNT(DISTINCT CASE WHEN param_name = 'OUT_CS5' AND param_value IN (5,4,3,2,1) AND opr_connected_time IS NOT NULL THEN session_id END) amount_cs5,
        
        --count(DISTINCT decode(param_name, 'CS6', session_id)) cs6, 
        count(DISTINCT CASE WHEN param_name = 'CS6' AND opr_connected_time IS NOT NULL THEN session_id ELSE NULL END) cs6,
        count(DISTINCT CASE WHEN param_name = 'OUT_CS6' AND param_value = '0' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs6,        
        count(DISTINCT CASE WHEN param_name = 'OUT_CS6' AND param_value = '1' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs6_1,          
        count(DISTINCT CASE WHEN param_name = 'OUT_CS6' AND param_value = '2' AND opr_connected_time IS NOT NULL THEN session_id END) out_cs6_2,
        COUNT(DISTINCT CASE WHEN param_name = 'OUT_CS6' AND param_value IN (5,4,3,2,1) AND opr_connected_time IS NOT NULL THEN session_id END) amount_cs6

      FROM dates d
           LEFT JOIN ivr i
             ON d.dd1 = i.dd1 AND d.dd2 = i.dd2
      GROUP BY ROLLUP(trunc(d.dd1), to_char(d.dd1,'dd.mm.yyyy hh24:mi')||' - '||to_char(d.dd2,'dd.mm.yyyy hh24:mi'))
                                                           
      ),

    itog_all AS (
      SELECT
        g1,
        dd,
        substr(period,1,16) AS period_gr,
        MAX(period) AS period,
        sum(incomingcalls) incomingcalls,
        sum(answeredcalls) answeredcalls,     
        ---
        sum(cs1) cs1,    
        sum(cs2) cs2, 
        sum(out_cs2) out_cs2,
        sum(amount_cs2) amount_cs2,        
        sum(cs3) cs3, 
        sum(out_cs3) out_cs3,
        sum(amount_cs3) amount_cs3,  
        sum(cs4) cs4, 
        sum(out_cs4) out_cs4,
        sum(amount_cs4) amount_cs4,  
        sum(cs5) cs5, 
        sum(out_cs5) out_cs5,
        sum(amount_cs5) amount_cs5,  
        sum(cs6) cs6, 
        sum(out_cs6) out_cs6,        
        sum(out_cs6_1) out_cs6_1,          
        sum(out_cs6_2) out_cs6_2,
        sum(amount_cs6) amount_cs6

      FROM itog
      GROUP by g1, dd, substr(period,1,16)
      )      
      
      
  SELECT
    g1,
    dd,
    period,                                                                                 --1 Дата
    incomingcalls,                                                                          --2 Количество поступивших звонков на оператора
    answeredcalls,                                                                          --3 Количество обслуженных вызовов
    cs1 AS inquiry_ivr,                                                                     --4 Количество клиентов, переведенных на интерактивное меню
    to_char((round(cs1/decode(answeredcalls,0,1,answeredcalls),2)*100))||'%' AS pr_inquiry_ivr, --5 Процент переведенных на интерактивное меню
    to_char(100-(round(cs1/decode(answeredcalls,0,1,answeredcalls),2)*100))||'%' AS pr_n_inquiry_ivr, --5 Процент не переведенных на интерактивное меню
    out_cs6_1 + out_cs6_2 AS listened_all_ivr,                                              --6 Полностью прослушали IVR (Количество клиентов, полностью   прошедших опрос)
    to_char(round((out_cs6_1 + out_cs6_2)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_all_ivr_percent, --7 Процент прослушавших полностью IVR (Доля клиентов, полностью   прошедших опрос)
    cs1 - out_cs6_1 - out_cs6_2 AS n_listened_all_ivr,                                      --8 Количество не прошедших опрос
    to_char(round((cs1 - out_cs6_1 - out_cs6_2)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_all_ivr_percent, --9 Процент не прошедших опрос
    out_cs2 AS listened_q1,                                                                 --10 Количество клиентов, полностью прослушавших первый вопрос
    cs2 - out_cs2 AS n_listened_q1,                                                         --11 Количество клиентов, не прослушавших первый вопрос
    to_char(round((out_cs2)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_q1_percent,--12 Доля клиентов, полностью прослушавших первый вопрос
    amount_cs2 as passed_q1,  --Количество клиентов, поставивших оценку в первом вопросе
    to_char(round((amount_cs2)/decode(cs1,0,1,cs1),2)*100)||'%' AS passed_q1_percent,--Доля клиентов, поставивших оценку в первом вопросе
    to_char(round((cs2 - out_cs2)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_q1_percent,--12 Процент клиентов, не прослушавших первый вопрос
    out_cs3 AS listened_q2,                                                                 --13 Количество клиентов, полностью прослушавших второй вопрос
    cs3 - out_cs3 AS n_listened_q2,                                                         --14 Количество клиентов, не прослушавших второй вопрос
    to_char(round((cs3 - out_cs3)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_q2_percent,--15 Процент клиентов, не прослушавших второй вопрос  
    to_char(round((out_cs3)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_q2_percent,  --15 Доля клиентов, полностью прослушавших второй вопрос
    amount_cs3 as passed_q2,  --Количество клиентов, поставивших оценку во втором вопросе
    to_char(round((amount_cs3)/decode(cs1,0,1,cs1),2)*100)||'%' AS passed_q2_percent,--Доля клиентов, поставивших оценку во втором вопросе
    out_cs4 AS listened_q3,                                                                 --16 Количество клиентов, полностью прослушавших третий вопрос
    cs4 - out_cs4 AS n_listened_q3,                                                         --17 Количество клиентов, не прослушавших третий вопрос
    to_char(round((cs4 - out_cs4)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_q3_percent,--18 Процент клиентов, не прослушавших третий вопрос 
    to_char(round((out_cs4)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_q3_percent,     --18 Доля клиентов, полностью прослушавших третий вопрос 
    amount_cs4 as passed_q3,  --Количество клиентов, поставивших оценку третий вопросе
    to_char(round((amount_cs4)/decode(cs1,0,1,cs1),2)*100)||'%' AS passed_q3_percent,--Доля клиентов, поставивших оценку на третьем вопросе
    out_cs5 AS listened_q4,                                                                 --19 Количество клиентов, полностью прослушавших четвертый вопрос
    cs5 - out_cs5 AS n_listened_q4,                                                         --20 Количество клиентов, не прослушавших четвертый вопрос
    to_char(round((cs5 - out_cs5)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_q4_percent,--21 Процент клиентов, не прослушавших четвертый вопрос
    to_char(round((out_cs5)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_q4_percent,--21 Доля клиентов, полностью прослушавших четвертый вопрос
    amount_cs5 as passed_q4,  --Количество клиентов, поставивших оценку на четвертом вопросе
    to_char(round((amount_cs5)/decode(cs1,0,1,cs1),2)*100)||'%' AS passed_q4_percent,--Доля клиентов, поставивших оценку на четвертом вопросе
    out_cs6 AS listened_q5,                                                                 --22 Количество клиентов, полностью прослушавших пятый вопрос
    cs6 - out_cs6 AS n_listened_q5,                                                         --23 Количество клиентов, не прослушавших пятый вопрос
    to_char(round((cs6 - out_cs6)/decode(cs1,0,1,cs1),2)*100)||'%' AS n_listened_q5_percent, --24 Процент клиентов, не прослушавших пятый вопрос 
    to_char(round((out_cs6)/decode(cs1,0,1,cs1),2)*100)||'%' AS listened_q5_percent, --24 Доля клиентов, полностью прослушавших пятый вопрос
    amount_cs6 as passed_q5,  --Количество клиентов, поставивших оценку на четвертом вопросе
    to_char(round((amount_cs6)/decode(cs1,0,1,cs1),2)*100)||'%' AS passed_q5_percent--Доля клиентов, поставивших оценку на четвертом вопросе
  FROM itog_all i
  WHERE (I_group_type = 0 AND g1 = 0)  --при выборке за весь период не выводим итоговые строки
       OR (I_group_type = 1 AND (g1 = 0 OR (g1 = 1 AND dd IS NULL))) --при группировке по дням выводим только общую итоговую строку
       OR (I_group_type = 2 ) --при группировке по часам выводим все строки
     -- OR (I_group_type = 2 AND (g1 = 0 OR (g1 = 1 AND dd IS NULL)))
  ORDER BY 2 NULLS LAST, 1, 3
  ;  
         

TYPE t_inquiry_ivr_stats IS TABLE OF cur_get_inquiry_ivr_stats%rowtype;

FUNCTION fnc_get_inquiry_ivr_stats
(
  I_init_time TIMESTAMP, 
  I_finish_time TIMESTAMP,
  I_group_type NUMBER,
  I_LOCATION VARCHAR2 := NULL
) RETURN t_inquiry_ivr_stats pipelined;


--------------------------------------------------------------------
--ОТЧЕТ №2 "Статистика по результатам опроса на удовлетворенность"
--------------------------------------------------------------------

CURSOR cur_get_inq_ivr_result_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_mr_name VARCHAR2 --Просто регион
)
 IS  

WITH

  DATA_INC_CALL AS --ОБЩАЯ ВЫГРУЗКА ЗВОНКОВ
     (SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
      WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
        AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
     ),
     DATA_INC_CALL_2 AS --УБИРАЕМ ДУБЛИ
      (
        SELECT *
        FROM DATA_INC_CALL
        WHERE RN = 1
      ),
    cl AS (
      SELECT
        gt.call_id,
        gt.call_init_time,
        enqueued_time,
        opr_connected_time,
        opr_login
      FROM
            TABLE(PKG_GENERAL_REPORTS.fnc_get_nau_calls_data --Выгрузка та же, что и в первом отчете
                      (
                       I_init_time,  --    i_init_time TIMESTAMP, 
                       I_finish_time,                                  --    i_finish_time TIMESTAMP
                       null, --линии с ivr                             --    i_linefilter VARCHAR2
                       NULL,                                           --    i_skill_group VARCHAR2
                       0                                               --    i_is_need_inner_calls NUMBER DEFAULT 0
                       )) gt
             LEFT JOIN common.d_phonecodes_mr m ON (floor(REGEXP_REPLACE(gt.abonent_phone,'\D','') / 10000000)) = (floor(m.rangeend / 10000000)) AND
                                                REGEXP_REPLACE(gt.abonent_phone,'\D','') BETWEEN m.rangestart AND m.rangeend
             LEFT JOIN common.d_kladr_phonecodes_regions kpr ON kpr.phonecodess_area = m.area
             LEFT JOIN common.d_rf_macroregions mr ON mr.kladr_objectcode = kpr.kladr_objectcode                                   

      WHERE to_char(kpr.kladr_objectcode) = i_mr_name OR i_mr_name IS NULL --Фильтр по регионам
    --  mr.macroregion_short_name = i_mr_name OR i_mr_name IS NULL         --Фильтр по Макрорегионам
      
      ),
      
    all_calls_prep AS (
      SELECT 
        call_id AS session_id,
        call_init_time,
        MAX(enqueued_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS enqueued_time,         
        MAX(opr_connected_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_connected_time,
        MAX(opr_login) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_login
      FROM cl
      GROUP BY call_id, call_init_time         
      ),
     all_calls AS (
     SELECT
        cl.session_id,
        cl.call_init_time,
        (CASE
          WHEN tab.CONNECT_RESULT_NUM =2 OR tab.CONNECT_RESULT_NUM IS NULL
          THEN cl.enqueued_time
          ELSE NULL
        END) AS ENQUEUED_TIME,
        (CASE
                  WHEN (tab.CALL_RESULT_NUM = 1
                    AND tab.CONNECT_RESULT_NUM = 2)
                      OR tab.CONNECT_RESULT_NUM IS NULL OR tab.CALL_RESULT_NUM IS NULL -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN cl.opr_connected_time
                  ELSE NULL
        END) AS opr_connected_time,
        cl.opr_login
        FROM all_calls_prep CL
        LEFT JOIN DATA_INC_CALL_2 tab --ЧТОБЫ СХОДИЛОСЬ С ДРУГИМИ ОТЧЕТАМИ
          on tab.session_id = CL.SESSION_ID
     ) ,        
                      
    ivr AS (
      SELECT DISTINCT
        cl.session_id,
        cp.param_name,
        to_number(cp.param_value) AS param_value
      FROM all_calls cl         
           JOIN naucrm.call_params cp 
             ON cp.session_id = cl.session_id
             
      WHERE param_name LIKE 'OUT_CS%' /*IN ('OUT_CS2','OUT_CS3','OUT_CS4','OUT_CS5','OUT_CS6')*/ AND to_number(param_value)>0 AND
            cl.opr_connected_time IS NOT NULL AND
           -- cp.changed >= cl.opr_connected_time
            (cp.changed >= I_init_time and cp.changed < I_finish_time + interval '30' minute)
      ),

    listened_all_ivr AS (
      SELECT DISTINCT
        session_id AS listened_all_ivr,
        count(DISTINCT session_id) OVER() AS all_count
      FROM IVR
     WHERE param_name = 'OUT_CS6'
      ),
      
    itog AS (
      SELECT
        MAX(param_name) for_sort,
        NVL(
            CASE 
                 WHEN param_name = 'OUT_CS2' THEN 'Оцените общее впечатление от полученной консультации. По шкале от 1 до 5, где 5 это максимальная оценка'
                 WHEN param_name = 'OUT_CS3' THEN 'Оцените заинтересованность специалиста и желание помочь в решении Вашего вопроса по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS4' THEN 'Оцените профессионализм и компетентность сотрудника по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS5' THEN 'Оцените, пожалуйста, доброжелательность специалиста по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS6' THEN 'Уточните, была ли полезна полученная информация для решения Вашего вопроса? Если да – нажмите клавишу 2, нет – клавишу 1'
               END,'Итого') AS question,                                            --Суть вопроса
        COUNT(CASE WHEN param_name is not null THEN session_id END)  as  amount_listined_all,--Количество клиентов, прослушавших вопрос    
        COUNT(CASE WHEN param_value IN (5,4,3,2,1) THEN session_id END) amount_all, --Количество клиентов, поставивших оценку в этом вопросе  --прошедших этот вопрос	(Количество клиентов, которые проставили оценку по вопросу)
        COUNT(CASE WHEN param_value = 5 THEN session_id END) amount_v5,             --Количество оценок 5  
        COUNT(CASE WHEN param_value = 4 THEN session_id END) amount_v4,             --Количество оценок 4
        COUNT(CASE WHEN param_value = 3 THEN session_id END) amount_v3,             --Количество оценок 3
        COUNT(CASE WHEN param_value = 2 THEN session_id END) amount_v2,             --Количество оценок 2
        COUNT(CASE WHEN param_value = 1 THEN session_id END) amount_v1,             --Количество оценок 1
    
        ROUND(
              COUNT(CASE WHEN param_value IN (5,4) AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(max(all_count),0,1,max(all_count))
              ,2)*100 AS CSAT,                                                     --CSAT
        
        ROUND(
              COUNT(CASE WHEN param_value = 1 AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(max(all_count),0,1,max(all_count))
              ,2)*100 AS CDSAT                                                      --CDSAT                       
      FROM ivr
           JOIN listened_all_ivr
             ON session_id =  listened_all_ivr
      GROUP BY 
        ROLLUP(
               CASE 
                 WHEN param_name = 'OUT_CS2' THEN 'Оцените общее впечатление от полученной консультации. По шкале от 1 до 5, где 5 это максимальная оценка'
                 WHEN param_name = 'OUT_CS3' THEN 'Оцените заинтересованность специалиста и желание помочь в решении Вашего вопроса по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS4' THEN 'Оцените профессионализм и компетентность сотрудника по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS5' THEN 'Оцените, пожалуйста, доброжелательность специалиста по шкале от 1 до 5'
                 WHEN param_name = 'OUT_CS6' THEN 'Уточните, была ли полезна полученная информация для решения Вашего вопроса? Если да – нажмите клавишу 2, нет – клавишу 1'
               END)
        )
  SELECT 
    decode(question,'Итого', NULL, for_sort) AS for_sort,
    
    question,
    amount_all,
    amount_listined_all,
    
    CASE
      WHEN for_sort = 'OUT_CS6' AND question <> 'Итого' THEN '0'
      ELSE to_char(amount_v5)
    END AS amount_v5,
    
    CASE
      WHEN for_sort = 'OUT_CS6' AND question <> 'Итого' THEN '0'
      ELSE to_char(amount_v4)
    END as amount_v4,  
    
    CASE
      WHEN for_sort = 'OUT_CS6' AND question <> 'Итого' THEN '0'
      ELSE to_char(amount_v3)
    END AS amount_v3,
    
    amount_v2,
    amount_v1,
    CASE
      WHEN for_sort = 'OUT_CS2' THEN to_char(CSAT)||'%' 
      WHEN question = 'Итого' THEN to_char(CSAT)||'%'
      ELSE '-'
    END AS CSAT,
    CASE
      WHEN for_sort = 'OUT_CS2' THEN to_char(CDSAT)||'%' 
      WHEN question = 'Итого' THEN to_char(CDSAT)||'%'
      ELSE '-'
    END AS CDSAT
  FROM itog
  ORDER BY 1 NULLS LAST;


TYPE t_inq_ivr_result_stats IS TABLE OF cur_get_inq_ivr_result_stats%rowtype;

FUNCTION fnc_get_inq_ivr_result_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_mr_name VARCHAR2
) RETURN t_inq_ivr_result_stats pipelined;


---------------------------------------------------------------------------
--ОТЧЕТ №3 "Статистика по результатам опроса в разрезе по звонкам"
----------------------------------------------------------------------------

CURSOR cur_get_inquiry_ivr_calls
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_location VARCHAR2,
  i_login VARCHAR2,
  i_listened_all_ivr VARCHAR2
) IS

WITH
  DATA_INC_CALL AS --ОБЩАЯ ВЫГРУЗКА ЗВОНКОВ
     (SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
      WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
        AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
     ),
     DATA_INC_CALL_2 AS --УБИРАЕМ ДУБЛИ
      (
        SELECT *
        FROM DATA_INC_CALL
        WHERE RN = 1
      ),
    UNIC_CORE_CALLS as --Уникальные звонки из таблицы CORE_CALLS (в таблицу пишутся дубли)
    (
    SELECT 
      SESSION_ID,
      MAX(ID_CALL) AS ID_CALL
    FROM CORE_CALLS
     WHERE CREATED_AT >= i_init_time
       AND CREATED_AT    < i_finish_time
    GROUP BY SESSION_ID
    ),
   TYPES_TICKETS as --Уникальные звонки из таблицы CORE_CALLS (в таблицу пишутся дубли)
    (
      SELECT
      CL.SESSION_ID,
      COALESCE(TTP.NAME,
               (CASE
                 WHEN CL.FID_RESULT IN (5,6,7,8)
                   THEN 'Посторонний звонок'
                 WHEN CL.FID_RESULT = 4
                   THEN 'Тестовое обращение' 
                END)
      ) AS TYPE_NAME--,
      --TTP.NAME AS TYPE_NAME
      FROM
      UNIC_CORE_CALLS UCL
      JOIN CORE_CALLS CL
       ON CL.ID_CALL = UCL.ID_CALL
      LEFT JOIN INC_CALL_CONTACT_DATA INC
       ON INC.FID_CALL = CL.ID_CALL AND INC.IS_PRIMARY = 1
      LEFT JOIN TICKETS_D_TYPES TTP
       ON INC.FID_TYPE = TTP.ID_TYPE
      LEFT JOIN CORE_CALLS_RESULTS RES
       ON RES.ID_RESULT = CL.FID_RESULT    

    ),   
    
    ic AS (
      SELECT 
        call_id AS session_id,
        call_init_time,
        abonent_phone,
        MAX(enqueued_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS enqueued_time,         
        MAX(opr_connected_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_connected_time,
        MAX(opr_login) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_login
      FROM 
           TABLE(PKG_GENERAL_REPORTS.fnc_get_nau_calls_data
                      (
                       i_init_time, 
                       i_finish_time,   
                       null, --линии с ivr
                       NULL,    
                       0
                       )) cl          
      GROUP BY call_id, call_init_time, abonent_phone         
      ),
      all_calls AS
        (SELECT 
           A.session_id,
           A.call_init_time, -- = ivrconnected
           A.abonent_phone as phone,
           (CASE
              WHEN (tab.CALL_RESULT_NUM = 1
                AND tab.CONNECT_RESULT_NUM = 2)
                  OR tab.CONNECT_RESULT_NUM IS NULL OR tab.CALL_RESULT_NUM IS NULL-- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
              THEN A.opr_connected_time
              ELSE NULL
           END) AS opr_connected_time,
           A.opr_login,
           opr.surname || ' ' || opr.NAME || ' ' || SUBSTR(opr.patronymic, 1, INSTR(opr.patronymic,'_',1,1)-1) AS opr_fio,
           opr.fid_location AS fid_location,
           TTC.TYPE_NAME AS request_type
         FROM ic A               
         LEFT JOIN DATA_INC_CALL_2 TAB ON tab.session_id = A.session_id
         LEFT JOIN TYPES_TICKETS TTC
          ON TTC.SESSION_ID = A.SESSION_ID
         LEFT JOIN cis.nc_users opr --таблица операторов
          ON opr.login = A.opr_login                                
         WHERE (opr.fid_location = i_location OR i_location IS NULL) AND --Фильтр по площадке
                 (opr.login = i_login OR i_login IS NULL)                  --Фильтр по Логину
      ),    
                      
    ivr AS (
      SELECT
        NVL(opr_fio, opr_login) AS opr_fio , --для ситуации, когда логин удалили из naucrm.abonents (напр., d.a.burlakov_ruspost_vol)
        fid_location,
        cl.session_id,
        phone,
        cl.opr_connected_time,
        request_type,
        cp.param_name,
        to_number(cp.param_value) AS param_value,
        count(cp.param_name) OVER (partition by cl.session_id) cnt_param   
      FROM all_calls cl        
           JOIN naucrm.call_params cp 
             ON cp.session_id = cl.session_id
      WHERE param_name LIKE 'OUT_CS%' AND to_number(param_value)>0 AND
            cl.opr_connected_time IS NOT NULL AND
           -- cp.changed >= cl.opr_connected_time
            (cp.changed >= I_init_time and cp.changed < I_finish_time + interval '30' minute)
      ),
    listened_all_ivr AS (
      SELECT DISTINCT 
        session_id,
        DECODE(param_name,'OUT_CS6','OUT_CS6','OUT_CS2') AS listened_all_ivr
--        (CASE
--          WHEN param_name = 'OUT_CS6' --Оценка по всем вопросам
--           THEN 'OUT_CS6'
--          WHEN param_name = 'OUT_CS2' AND cnt_param < 2 --Оценка только по первому вопросу
--           THEN 'OUT_CS2'
--          WHEN param_name = 'OUT_CS3' AND cnt_param < 5 --Оценка по 2 - по 4 вопрос
--           THEN 'OUT_CS3_5'
--        END) AS listened_all_ivr
       FROM IVR
      WHERE param_name = 'OUT_CS6' OR 
           (param_name = 'OUT_CS2' AND cnt_param < 5 /*session_id NOT IN (SELECT session_id FROM ivr WHERE param_name = 'OUT_CS6')*/)               
      )
      
            
  SELECT
    opr_connected_time for_sort,
    ivr.session_id,
    opr_fio,                                                     --ФИО оператора (ФИО оператора, принявшего звонок)                 
    location_name,                                               --Площадка	(Площадка, на которой находится оператор, принявший звонок) 
    to_char(opr_connected_time, 'dd.mm.yyyy hh24:mi:ss') AS opr_connected_time, --Дата и время звонка	(Дата и время принятия звонка оператором)
    phone,                                                       --Телефон (Номер телефона, с которого поступил звонок)
    max(request_type) as request_type,                           --Тип обращения(Тема звонка, зафиксированная оператором в скрипте)
    MAX(decode(param_name, 'OUT_CS2', param_value)) AS value_q1, --Оценка, которую клиент поставил по вопросу
    MAX(decode(param_name, 'OUT_CS3', param_value)) AS value_q2,
    MAX(decode(param_name, 'OUT_CS4', param_value)) AS value_q3,
    MAX(decode(param_name, 'OUT_CS5', param_value)) AS value_q4,
    MAX(decode(param_name, 'OUT_CS6', param_value)) AS value_q5,
    round(
    (
    MAX(decode(param_name, 'OUT_CS2', param_value, 0)) +
    MAX(decode(param_name, 'OUT_CS3', param_value, 0)) +
    MAX(decode(param_name, 'OUT_CS4', param_value, 0)) +
    MAX(decode(param_name, 'OUT_CS5', param_value, 0)) +
    MAX(CASE
          WHEN param_name = 'OUT_CS6' AND param_value = 1 THEN param_value
          WHEN param_name = 'OUT_CS6' AND param_value = 2 THEN 5
          ELSE 0
        END)
    )//*5*/count(DISTINCT param_name),1) AS average_value        --Средний балл (делим на кол-во отвеченных вопросов)
  FROM ivr
       JOIN listened_all_ivr i      
         ON i.session_id = ivr.session_id
       LEFT JOIN cis.d_locations loc
         ON loc.id_location = fid_location
  WHERE listened_all_ivr = i_listened_all_ivr OR i_listened_all_ivr IS NULL 
  GROUP BY ivr.session_id, opr_fio, location_name, opr_connected_time, phone--, last_request_type
  ORDER BY 1;

TYPE t_inquiry_ivr_calls IS TABLE OF cur_get_inquiry_ivr_calls%rowtype;

FUNCTION fnc_get_inquiry_ivr_calls
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_location VARCHAR2,
  i_login VARCHAR2,
  i_listened_all_ivr VARCHAR2
) RETURN t_inquiry_ivr_calls pipelined;


-------------------------------------------------------------------------
--ОТЧЕТ №4 "Статистика по результатам опроса в разрезе по тематикам"  --
-------------------------------------------------------------------------
CURSOR cur_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER, --Просто регион
  I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
) IS
WITH
   CALLS_TYPE AS ( --Первый выбранный тип при ответе на вопросы --ZHKKH-917
          SELECT 
                  CL.SESSION_ID         
                  , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK FIRST ORDER BY QST.ID_QUESTION) AS TYPE_NAME_LEVEL_1 
                  , MAX(TDT.NAME) KEEP (DENSE_RANK FIRST ORDER BY QST.ID_QUESTION) AS TYPE_NAME_LEVEL_2 --Классификатор 2 
                  , MAX(ADT.NAME) KEEP (DENSE_RANK FIRST ORDER BY QST.ID_QUESTION) AS ADMIN_TYPE --Административный тип 
          FROM INC_CALL_QUESTIONS QST
                  JOIN CORE_CALLS CL
                      ON CL.ID_CALL = QST.FID_CALL
                  JOIN TICKETS_D_TYPES TDT
                      ON TDT.ID_TYPE = QST.FID_TICKET_TYPE
                  JOIN TICKETS_D_TYPES TDT_LEV_1
                      ON TDT_LEV_1.ID_TYPE = TDT.ID_PARENT 
                  JOIN TICKETS_D_ADM_TYPES ADT
                      ON ADT.ID_TYPE = QST.FID_TICKET_ADM_TYPE
          WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME 
          GROUP BY CL.SESSION_ID 
  )
  , ALL_TYPES AS ( --ВСЕ ТИПЫ В СПРАВОЧНИКЕ
          SELECT 
                  TDT_LEV_2.ID_TYPE AS ID_TYPE_LEVEL_2 --ID ТИПА ВТОРОГО УРОВНЯ  
                  , TDT_LEV_1.NAME AS TYPE_NAME_LEVEL_1 --ТИП ПЕРВОГО УРОВНЯ
                  , TDT_LEV_2.NAME AS TYPE_NAME_LEVEL_2 --ТИП ВТОРОГО УРОВНЯ
          FROM TICKETS_D_TYPES TDT_LEV_2
                  JOIN TICKETS_D_TYPES TDT_LEV_1
                      ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_2.IS_ACTIVE = 1 
  )     
  , ALL_TYPES_FOR_FORMAT AS ( --ВСЕ ТИПЫ В СПРАВОЧНИКЕ
          SELECT 
                  TDT_LEV_1.ID_TYPE AS ID_TYPE_LEVEL_1 --ID ТИПА ПЕРВОГО УРОВНЯ
                  , TDT_LEV_2.ID_TYPE AS ID_TYPE_LEVEL_2 --ID ТИПА ВТОРОГО УРОВНЯ  
                  , TDT_LEV_1.NAME AS TYPE_NAME_LEVEL_1 --ТИП ПЕРВОГО УРОВНЯ
                  , TDT_LEV_2.NAME AS TYPE_NAME_LEVEL_2 --ТИП ВТОРОГО УРОВНЯ
                  , ACT.NAME AS CLASS_TYPE --(ГРАЖДАНИН ИЛИ НЕ ГРАЖДАНИН)
                  , (CASE WHEN TDT_LEV_2.NAME = 'Тестовое обращение' THEN 2 ELSE 1 END) AS ORD -- ДЛЯ СОРТИРОВКИ
          FROM TICKETS_D_TYPES TDT_LEV_2
                  JOIN TICKETS_D_TYPES TDT_LEV_1
                      ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT
                  JOIN TICKETS_TPS_HAS_ACS_TPS HAT
                      ON HAT.FID_TICKET_TYPE = TDT_LEV_2.ID_TYPE
                  JOIN TICKETS_D_TPS_ACS_TPS ACT
                      ON ACT.ID_TYPE = HAT.FID_ACCESS_TYPE
                      AND TDT_LEV_1.IS_ACTIVE = 1 
          UNION ALL
          SELECT 
                  1001 AS ID_TYPE_LEVEL_1 --ID ТИПА ПЕРВОГО УРОВНЯ
                  , 1001 AS ID_TYPE_LEVEL_2 --ID ТИПА ВТОРОГО УРОВНЯ  
                  , 'Посторонний звонок' AS TYPE_NAME_LEVEL_1 --ТИП ПЕРВОГО УРОВНЯ
                  , 'Посторонний звонок' AS TYPE_NAME_LEVEL_2 --ТИП ВТОРОГО УРОВНЯ
                  , '-' AS CLASS_TYPE --(ГРАЖДАНИН ИЛИ НЕ ГРАЖДАНИН)
                  , 3 AS ORD
          FROM DUAL  
          --ORDER BY (case when act.code = 'not_citizen' then 1 else 2 end),TDT_LEV_1.ID_TYPE, TDT_LEV_2.ID_TYPE
  ) 
  , FORMAT AS (
          SELECT * 
          FROM /*PERIODS
                  ,*/ ALL_TYPES_FOR_FORMAT TTP
  --  ORDER BY START_PERIOD,ORD,(case when CLASS_TYPE = 'Гражданин' then 1 else 2 end), ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  ),
  DATA_INC_CALL AS --ОБЩАЯ ВЫГРУЗКА ЗВОНКОВ
     (SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
      WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
        AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
     ),
     DATA_INC_CALL_2 AS --УБИРАЕМ ДУБЛИ
      (
        SELECT *
        FROM DATA_INC_CALL
        WHERE RN = 1
      ),

 UNIC_CORE_CALLS as --Уникальные звонки из таблицы CORE_CALLS (в таблицу пишутся дубли)
    (
    SELECT 
      SESSION_ID,
      MAX(ID_CALL) AS ID_CALL
    FROM CORE_CALLS
     WHERE CREATED_AT >= I_init_time
       AND CREATED_AT    < I_finish_time
    GROUP BY SESSION_ID
    ),
   TYPES_TICKETS as --Уникальные звонки из таблицы CORE_CALLS (в таблицу пишутся дубли)
    (
      SELECT
      CL.SESSION_ID,
      COALESCE(TTP.NAME,
               (CASE
                 WHEN CL.FID_RESULT IN (5,6,7,8)
                   THEN 'Посторонний звонок'
                 WHEN CL.FID_RESULT = 4
                   THEN 'Тестовое обращение' 
                END)
      ) AS TYPE_NAME,
      (CASE
        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
        ELSE 'НЕ гражданин'
       END) AS CLASS_TYPE,
      INC.FID_COMPANY_REGION
      --TTP.NAME AS TYPE_NAME
      FROM
      UNIC_CORE_CALLS UCL
      JOIN CORE_CALLS CL
       ON CL.ID_CALL = UCL.ID_CALL
      LEFT JOIN INC_CALL_CONTACT_DATA INC
       ON INC.FID_CALL = CL.ID_CALL AND INC.IS_PRIMARY = 1
      LEFT JOIN TICKETS_D_TYPES TTP
       ON INC.FID_TYPE = TTP.ID_TYPE
      LEFT JOIN TICKETS_D_COMPANY_TYPES CPT
       ON CPT.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE 
      LEFT JOIN CORE_CALLS_RESULTS RES
       ON RES.ID_RESULT = CL.FID_RESULT
       

    ), 
    ic AS (
      SELECT 
        call_id AS session_id,
        call_init_time,
        abonent_phone,
        MAX(enqueued_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS enqueued_time,         
        MAX(opr_connected_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_connected_time,
        MAX(opr_login) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_login
      FROM
           TABLE(PKG_GENERAL_REPORTS.fnc_get_nau_calls_data
                      (
                       I_init_time, 
                       I_finish_time,   
                       NULL, --линии с ivr
                       NULL,    
                       0
                       )) cl 
            -- LEFT JOIN common.d_phonecodes_mr m ON (floor(REGEXP_REPLACE(cl.abonent_phone,'\D','') / 10000000)) = (floor(m.rangeend / 10000000)) AND
            --                                    REGEXP_REPLACE(cl.abonent_phone,'\D','') BETWEEN m.rangestart AND m.rangeend
            -- LEFT JOIN common.d_kladr_phonecodes_regions kpr ON kpr.phonecodess_area = m.area
            -- LEFT JOIN common.d_rf_macroregions mr ON mr.kladr_objectcode = kpr.kladr_objectcode                                   

     -- WHERE to_char(kpr.kladr_objectcode) = I_mr_name OR I_mr_name IS NULL --Фильтр по регионам
      GROUP BY call_id, call_init_time, abonent_phone         
      ),
      
  cl AS (
    SELECT
        A.session_id,
        to_number(opr.fid_location) AS fid_location,
        (CASE
            WHEN (tab.CALL_RESULT_NUM = 1
              AND tab.CONNECT_RESULT_NUM = 2)
               OR tab.CONNECT_RESULT_NUM IS NULL OR tab.CALL_RESULT_NUM IS NULL-- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
            THEN A.opr_connected_time
            ELSE NULL
         END) AS opr_connected_time,
        -- A.opr_connected_time,--
       --  TTC.TYPE_NAME AS request_type,
         
         CT.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1,
         CT.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2,
         TTC.CLASS_TYPE AS CLASS_TYPE,
         CT.ADMIN_TYPE AS ADMIN_TYPE 
     FROM ic A
     LEFT JOIN DATA_INC_CALL_2 TAB 
      ON tab.session_id = A.session_id
     LEFT JOIN CALLS_TYPE CT
      ON CT.session_id = A.session_id
     LEFT JOIN TYPES_TICKETS TTC
      ON TTC.SESSION_ID = A.SESSION_ID
     LEFT JOIN cis.nc_users opr --таблица операторов
      ON opr.login = A.opr_login 
      
     WHERE (TTC.FID_COMPANY_REGION = I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
       AND (CT.ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND CT.ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
 --  where TAB.rn =1   

     ),   
                      
    ivr_all AS (
      SELECT
        cl.session_id,
        cp.param_name,
        to_number(cp.param_value) AS param_value,
        fid_location,
        --request_type,
        
        TYPE_NAME_LEVEL_1,
        TYPE_NAME_LEVEL_2,
        CLASS_TYPE
        
      FROM cl   
           JOIN naucrm.call_params cp 
             ON cp.session_id = cl.session_id
      WHERE 
            cp.param_name IN ('OUT_CS2', 'OUT_CS3', 'OUT_CS4', 'OUT_CS5', 'OUT_CS6')-- LIKE 'OUT_CS%'
        --AND to_number(cp.param_value)>0 --ПОЧЕМУ ОН С "IN" НЕ ХОЧЕТ РАБОТАТЬ???? 
        AND cl.opr_connected_time IS NOT NULL
        AND (cp.changed >= I_init_time and cp.changed < I_finish_time + interval '30' minute)
            --cp.changed >= cl.opr_connected_time
      )
      ,       
   listened_all_ivr AS (
      SELECT  
        MAX(session_id) AS listened_all_ivr
      FROM ivr_all
      WHERE param_name = 'OUT_CS6'
        AND param_value>0
      GROUP BY session_id  
      ),      
      
    ivr AS (
      SELECT
        iv.*
      FROM ivr_all iv      
           JOIN listened_all_ivr 
             ON listened_all_ivr = session_id
      WHERE iv.param_value>0       
      ),   
    itog AS(
      SELECT 

        DECODE(GROUPING(FT.TYPE_NAME_LEVEL_1)
             ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
      , FT.TYPE_NAME_LEVEL_2
      , FT.CLASS_TYPE
      , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
      , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
      , MAX(FT.ORD) AS ORD,

        count(DISTINCT session_id) AS amount_calls,
        round(
              sum(decode(param_name, 'OUT_CS2', param_value,0))/
              decode(count(decode(param_name, 'OUT_CS2', param_value)),0,1,count(decode(param_name, 'OUT_CS2', param_value)))
              ,1) AS average_value_q1,
          
        round(
              sum(decode(param_name, 'OUT_CS3', param_value,0))/
              decode(count(decode(param_name, 'OUT_CS3', param_value)),0,1,count(decode(param_name, 'OUT_CS3', param_value)))
              ,1) AS average_value_q2, 
          
        round(
              sum(decode(param_name, 'OUT_CS4', param_value,0))/
              decode(count(decode(param_name, 'OUT_CS4', param_value)),0,1,count(decode(param_name, 'OUT_CS4', param_value))) 
              ,1) AS average_value_q3,
          
        round(
              sum(decode(param_name, 'OUT_CS5', param_value,0))/
              decode(count(decode(param_name, 'OUT_CS5', param_value)),0,1,count(decode(param_name, 'OUT_CS5', param_value)))
              ,1) AS average_value_q4,
          
        round(      
              sum(decode(param_name, 'OUT_CS6', param_value,0))/
              decode(count(decode(param_name, 'OUT_CS6', param_value)),0,1,count(decode(param_name, 'OUT_CS6', param_value))) 
              ,1) AS average_value_q5,
    
    
        round(
              count(DISTINCT CASE WHEN param_value IN (5,4) AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(count(DISTINCT session_id),0,1,
                     count(DISTINCT session_id))
              ,2)*100  AS csat,                                                            --CSAT
        round(
              count(DISTINCT CASE WHEN fid_location = 1 AND (param_value IN (5,4) AND param_name = 'OUT_CS2')THEN session_id END)/
              decode(count(DISTINCT CASE WHEN fid_location = 1 THEN session_id END),0,1,
                     count(DISTINCT CASE WHEN fid_location = 1 THEN session_id END))
              ,2)*100  AS csat_smol,                                                       --CSAT Смоленск
        round(
              count(DISTINCT CASE WHEN fid_location = 4 AND (param_value IN (5,4) AND param_name = 'OUT_CS2')THEN session_id END)/
              decode(count(DISTINCT CASE WHEN fid_location = 4 THEN session_id END),0,1,
                     count(DISTINCT CASE WHEN fid_location = 4 THEN session_id END))
              ,2)*100  AS csat_vol,                                                        --CSAT Волжский                
              
         
        round(
              count(DISTINCT CASE WHEN param_value = 1 AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(count(DISTINCT session_id),0,1,
                     count(DISTINCT session_id))
              ,2)*100 AS cdsat,                                                            --CDSAT  
        round(
              count(DISTINCT CASE WHEN fid_location = 1 AND param_value = 1 AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(count(DISTINCT CASE WHEN fid_location = 1 THEN session_id END),0,1,
                     count(DISTINCT CASE WHEN fid_location = 1 THEN session_id END))
              ,2)*100 AS cdsat_smol,                                                       --CDSAT Смоленск                 
        round(
              count(DISTINCT CASE WHEN fid_location = 4 AND param_value = 1 AND param_name = 'OUT_CS2' THEN session_id END)/
              decode(count(DISTINCT CASE WHEN fid_location = 4 THEN session_id END),0,1,
                     count(DISTINCT CASE WHEN fid_location = 4 THEN session_id END))
              ,2)*100 AS cdsat_vol                                                         --CDSAT Волжский                                          
      FROM ivr
      RIGHT JOIN FORMAT FT 
        ON FT.TYPE_NAME_LEVEL_1 = ivr.TYPE_NAME_LEVEL_1
       AND FT.TYPE_NAME_LEVEL_2 = ivr.TYPE_NAME_LEVEL_2
       AND FT.CLASS_TYPE = ivr.CLASS_TYPE
    
      GROUP BY ROLLUP(FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)
      ORDER BY GROUPING(FT.TYPE_NAME_LEVEL_1),ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1, ID_TYPE_LEVEL_2

)          
  SELECT 
    TYPE_NAME_LEVEL_1,--Тип обращения
    TYPE_NAME_LEVEL_2,
    CLASS_TYPE,
    TO_CHAR(amount_calls) as amount_calls, --Кол-во оцененных обращений по этой тематике
    decode(average_value_q1,0,'нет оценок',to_char(average_value_q1)) AS average_value_q1, --Вопрос 1
    decode(average_value_q2,0,'нет оценок',to_char(average_value_q2)) AS average_value_q2,
    decode(average_value_q3,0,'нет оценок',to_char(average_value_q3)) AS average_value_q3,
    decode(average_value_q4,0,'нет оценок',to_char(average_value_q4)) AS average_value_q4,
    decode(average_value_q5,0,'нет оценок',to_char(average_value_q5)) AS average_value_q5, --Вопрос 5
    decode(csat,0,'нет оценок',to_char(csat)||'%') AS csat, --CSAT суммарно по площадкам
    decode(csat_smol,0,'нет оценок',to_char(csat_smol)||'%') AS csat_smol, --CSAT Смоленск
    decode(csat_vol,0,'нет оценок',to_char(csat_vol)||'%') AS csat_vol, --CSAT Волжский
    decode(cdsat,0,'нет оценок',to_char(cdsat)||'%') AS cdsat, --CDSAT суммарно по площадкам
    decode(cdsat_smol,0,'нет оценок',to_char(cdsat_smol)||'%') AS cdsat_smol, --CDSAT Смоленск
    decode(cdsat_vol,0,'нет оценок',to_char(cdsat_vol)||'%') AS cdsat_vol --CDSAT Волжский
  FROM itog i
  WHERE
        (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) 
        OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
;

TYPE t_inq_ivr_cl_types IS TABLE OF cur_get_inq_ivr_cl_types%rowtype;

FUNCTION fnc_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER,
  I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
) RETURN t_inq_ivr_cl_types pipelined;



--------------------------------------------------
--ОТЧЕТ №5 "Статистика по результатам опроса в разрезе по операторам"
--------------------------------------------------
CURSOR cur_get_inq_ivr_opr_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
--  i_location VARCHAR2,
  i_login VARCHAR2
  --i_param_value NUMBER
) IS
WITH 
  DATA_INC_CALL AS --ОБЩАЯ ВЫГРУЗКА ЗВОНКОВ
     (SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
      WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
        AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
     ),
     DATA_INC_CALL_2 AS --УБИРАЕМ ДУБЛИ
      (
        SELECT *
        FROM DATA_INC_CALL
        WHERE RN = 1
      ),
    ic AS (
      SELECT 
        call_id AS session_id,
        call_init_time,
        abonent_phone,
        MAX(enqueued_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS enqueued_time,         
        MAX(opr_connected_time) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_connected_time,
        MAX(opr_login) KEEP (DENSE_RANK LAST ORDER BY opr_connected_time NULLS FIRST) AS opr_login
      FROM 
          TABLE(PKG_GENERAL_REPORTS.fnc_get_nau_calls_data
                      (
                       I_init_time,  --    i_init_time TIMESTAMP, 
                       I_finish_time,                                  --    i_finish_time TIMESTAMP
                       null, --линии с ivr                             --    i_linefilter VARCHAR2
                       NULL,                                           --    i_skill_group VARCHAR2
                       0                                               --    i_is_need_inner_calls NUMBER DEFAULT 0
                       )) cl         
      GROUP BY call_id, call_init_time, abonent_phone         
      ),
      
      all_calls AS
        (SELECT 
           A.session_id,
           A.call_init_time, -- = ivrconnected
           (CASE
              WHEN (tab.CALL_RESULT_NUM = 1
                AND tab.CONNECT_RESULT_NUM = 2)
                  OR tab.CONNECT_RESULT_NUM IS NULL OR tab.CALL_RESULT_NUM IS NULL-- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
              THEN A.opr_connected_time
              ELSE NULL
           END) AS opr_connected_time,
           A.opr_login,
           opr.surname || ' ' || opr.NAME || ' ' || opr.patronymic AS opr_fio,
           to_number(opr.fid_location) AS fid_location
         FROM ic A 
              LEFT JOIN DATA_INC_CALL_2 TAB ON tab.session_id = A.session_id
              LEFT JOIN cis.nc_users opr --таблица операторов
                ON opr.login = A.opr_login             
         WHERE --(opr.fid_location = i_location OR i_location IS NULL) AND --Фильтр по площадке
               (opr.login = i_login OR i_login IS NULL)                    --Фильтр по Логину
      ),           
           
                      
    ivr AS (
      SELECT
        NVL(opr_fio, opr_login) AS opr_fio, --для ситуации, когда логин удалили из naucrm.abonents (напр., d.a.burlakov_ruspost_vol)
        location_name,
        cl.session_id,
        cp.param_name,
        to_number(cp.param_value) AS param_value
      FROM all_calls cl        
           JOIN naucrm.call_params cp 
             ON cp.session_id = cl.session_id
           LEFT JOIN cis.d_locations loc
             ON loc.id_location = fid_location             
      WHERE param_name LIKE 'OUT_CS%' AND to_number(param_value)>0 AND
            cl.opr_connected_time IS NOT NULL AND
           -- cp.changed >= cl.opr_connected_time
            (cp.changed >= I_init_time and cp.changed < I_finish_time + interval '30' minute)
      ),            

    listened_all_ivr AS (
      SELECT DISTINCT 
        session_id AS listened_all_ivr
      FROM IVR
     WHERE param_name = 'OUT_CS6'
      )
      
  SELECT
    opr_fio for_sort,
    nvl(opr_fio, 'Итого') opr_fio,                          --ФИО оператора (ФИО оператора, принявшего звонок)                 
    nvl2(opr_fio,MAX(location_name),'') AS location_name,   --Площадка	(Площадка, на которой находится оператор, принявший звонок)
    count(DISTINCT listened_all_ivr) AS amount_listened_all_ivr,
    
    round(
          sum(decode(param_name, 'OUT_CS2', param_value,0))/
          decode(count(decode(param_name, 'OUT_CS2', param_value)),0,1,
                 count(decode(param_name, 'OUT_CS2', param_value)))
          ,1) AS average_value_q1,
          
    round(
          sum(decode(param_name, 'OUT_CS3', param_value,0))/
          decode(count(decode(param_name, 'OUT_CS3', param_value)),0,1,
                 count(decode(param_name, 'OUT_CS3', param_value)))
          ,1) AS average_value_q2, 
          
    round(
          sum(decode(param_name, 'OUT_CS4', param_value,0))/
          decode(count(decode(param_name, 'OUT_CS4', param_value)),0,1,
                 count(decode(param_name, 'OUT_CS4', param_value))) 
          ,1) AS average_value_q3,
          
    round(
          sum(decode(param_name, 'OUT_CS5', param_value,0))/
          decode(count(decode(param_name, 'OUT_CS5', param_value)),0,1,
                 count(decode(param_name, 'OUT_CS5', param_value)))
          ,1) AS average_value_q4,
          
    round(      
          sum(decode(param_name, 'OUT_CS6', param_value,0))/
          decode(count(decode(param_name, 'OUT_CS6', param_value)),0,1,
                 count(decode(param_name, 'OUT_CS6', param_value))) 
          ,1) AS average_value_q5,
    
    to_char(
    ROUND(
          COUNT(CASE WHEN param_value IN (5,4) AND param_name = 'OUT_CS2' THEN session_id END)/
          decode(count(DISTINCT listened_all_ivr),0,1,count(DISTINCT listened_all_ivr))
          ,2)*100)||'%'  AS CSAT,                                                          --CSAT
    
    to_char(     
    ROUND(
          COUNT(CASE WHEN param_value = 1 AND param_name = 'OUT_CS2' THEN session_id END)/
          decode(count(DISTINCT listened_all_ivr),0,1,count(DISTINCT listened_all_ivr))
          ,2)*100)||'%' AS CDSAT                                                           --CDSAT            
    
  
  FROM ivr
       JOIN listened_all_ivr
         ON session_id = listened_all_ivr
  --WHERE (param_value = i_param_value or i_param_value is null)  --Фильтр по оценке (не нужен) 
  GROUP BY ROLLUP(opr_fio)
  ORDER BY 1 NULLS LAST, 3 NULLS LAST;

TYPE t_inq_ivr_opr_stats IS TABLE OF cur_get_inq_ivr_opr_stats%rowtype;

FUNCTION fnc_get_inq_ivr_opr_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
 -- i_location VARCHAR2,
  i_login VARCHAR2
 -- i_param_value NUMBER
) RETURN t_inq_ivr_opr_stats pipelined;

END PKG_ACSI_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_ACSI_REPORTS AS

--------------------------------------------------
--Пакет для отчетов по ACSI (ГИС ЖКХ / ZHKKH-528)
-----------------------------------------------------------------------------
----Для ознакомления можно посмореть задачу RUSPOST-33 (создан по аналогии)
--------------------------------------------------------------------------
--
--------------------------------------------------------------------------
--ОТЧЕТ №1 "Статистика по голосовому меню «Опрос на удовлетворенность»" 
--------------------------------------------------------------------------

FUNCTION fnc_get_inquiry_ivr_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_group_type NUMBER,
  I_LOCATION VARCHAR2 := NULL
) RETURN t_inquiry_ivr_stats pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;  
v_period VARCHAR2(10);
BEGIN 

  IF i_group_type = 1 THEN v_period := 'day';
  elsif i_group_type = 2 THEN v_period := 'hour';
  END IF;
  
    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
    EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

  IF(cur_get_inquiry_ivr_stats%isopen) THEN CLOSE cur_get_inquiry_ivr_stats;
  END IF;
  
  FOR l IN cur_get_inquiry_ivr_stats(i_init_time, i_finish_time, i_group_type, v_period, I_LOCATION)
    loop
      pipe ROW (l);
    END loop;

END fnc_get_inquiry_ivr_stats;

-------------------------------------------------------------------
--ОТЧЕТ №2 "Статистика по результатам опроса на удовлетворенность" 
--(создан по заявке JIRA ZHKKH-528 / ZHKKH-551)--
-------------------------------------------------------------------

FUNCTION fnc_get_inq_ivr_result_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_mr_name VARCHAR2
)RETURN t_inq_ivr_result_stats pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;  
BEGIN

    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
    EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
  IF(cur_get_inq_ivr_result_stats%isopen) THEN CLOSE cur_get_inq_ivr_result_stats;
  END IF;
  
  FOR l IN cur_get_inq_ivr_result_stats(i_init_time, i_finish_time, i_mr_name)
    loop
      pipe ROW (l);
    END loop;
END fnc_get_inq_ivr_result_stats;


-------------------------------------------------------------------------
--ОТЧЕТ №3 "Статистика по результатам опроса в разрезе по звонкам" --
-------------------------------------------------------------------------

FUNCTION fnc_get_inquiry_ivr_calls
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  i_location VARCHAR2,
  i_login VARCHAR2,
  i_listened_all_ivr VARCHAR2
)RETURN t_inquiry_ivr_calls pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;   
BEGIN

  IF(cur_get_inquiry_ivr_calls%isopen) THEN CLOSE cur_get_inquiry_ivr_calls;
  END IF;
  
  FOR l IN cur_get_inquiry_ivr_calls(i_init_time, i_finish_time, i_location, i_login, i_listened_all_ivr)
    loop
      pipe ROW (l);
    END loop;
END fnc_get_inquiry_ivr_calls;



-------------------------------------------------------------------------
--ОТЧЕТ №4 "Статистика по результатам опроса в разрезе по тематикам"  --
-------------------------------------------------------------------------
FUNCTION fnc_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER,
  I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
)RETURN t_inq_ivr_cl_types pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;  
BEGIN

  IF(cur_get_inq_ivr_cl_types%isopen) THEN CLOSE cur_get_inq_ivr_cl_types;
  END IF;
  
  FOR l IN cur_get_inq_ivr_cl_types(i_init_time, i_finish_time, I_COMPANY_REGION,I_ADMIN_TYPE)
    loop
      pipe ROW (l);
    END loop;
END fnc_get_inq_ivr_cl_types;  


--------------------------------------------------
--ОТЧЕТ №5 "Статистика по результатам опроса в разрезе по операторам"
--------------------------------------------------
FUNCTION fnc_get_inq_ivr_opr_stats
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  --i_location VARCHAR2,
  i_login VARCHAR2
  --i_param_value NUMBER
)RETURN t_inq_ivr_opr_stats pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;   
BEGIN

    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
    EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
  IF(cur_get_inq_ivr_opr_stats%isopen) THEN CLOSE cur_get_inq_ivr_opr_stats;
  END IF;
  
  FOR l IN cur_get_inq_ivr_opr_stats(i_init_time, i_finish_time,/* i_location,*/ i_login /*, i_param_value*/)
    loop
      pipe ROW (l);
    END loop;
END fnc_get_inq_ivr_opr_stats;

END PKG_ACSI_REPORTS;
/
