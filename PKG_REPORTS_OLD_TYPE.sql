CREATE OR REPLACE PACKAGE PKG_REPORTS_OLD_TYPE AS 

  -------------------------------------------------------------------
  --   Пакет с отчетами по старой классификации до изменений по заявке ZHKKH-916, ZHKKH-917
  -------------------------------------------------------------------
  --
  -------------------------------------------------------------------------
  --ОТЧЕТ №4 "Статистика по результатам опроса в разрезе по тематикам"  --
  -------------------------------------------------------------------------
CURSOR cur_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER --Просто регион
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
      INC.FID_COMPANY_REGION
      --TTP.NAME AS TYPE_NAME
      FROM
      UNIC_CORE_CALLS UCL
      JOIN CORE_CALLS CL
       ON CL.ID_CALL = UCL.ID_CALL
      LEFT JOIN INC_CALL_CONTACT_DATA INC
       ON INC.FID_CALL = CL.ID_CALL AND INC.IS_PRIMARY = 1
      LEFT JOIN TICKETS_D_TYPES TTP
       ON INC.FID_TYPE = TTP.ID_TYPE AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
      LEFT JOIN CORE_CALLS_RESULTS RES
       ON RES.ID_RESULT = CL.FID_RESULT
       

    ), 
     FORMAT AS --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
        (
               SELECT DECODE(ID_TYPE,
                                    11,9.5,
                                    12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES where ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
               UNION
               SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
               UNION
               SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
               ORDER BY ID_TYPE
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

     -- WHERE to_char(kpr.kladr_objectcode) = :I_mr_name OR :I_mr_name IS NULL --Фильтр по регионам
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
         TTC.TYPE_NAME AS request_type
     FROM ic A
     LEFT JOIN DATA_INC_CALL_2 TAB 
      ON tab.session_id = A.session_id
     LEFT JOIN TYPES_TICKETS TTC
      ON TTC.SESSION_ID = A.SESSION_ID
     LEFT JOIN cis.nc_users opr --таблица операторов
      ON opr.login = A.opr_login 
      
     WHERE (TTC.FID_COMPANY_REGION = I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
 --  where TAB.rn =1   

     ),   
                      
    ivr_all AS (
      SELECT
        cl.session_id,
        cp.param_name,
        to_number(cp.param_value) AS param_value,
        fid_location,
        request_type

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

        DECODE(GROUPING(FR.NAME)
                ,0,FR.NAME,'Всего') AS request_type,

        MAX(FR.ID_TYPE) AS ID_TYPE,

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
      RIGHT JOIN FORMAT FR
       ON FR.NAME =  ivr.request_type
      GROUP BY ROLLUP(FR.NAME)
      ORDER BY ID_TYPE

)          
  SELECT 
    request_type AS text,--Тип обращения
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
  ;

TYPE t_inq_ivr_cl_types IS TABLE OF cur_get_inq_ivr_cl_types%rowtype;

FUNCTION fnc_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER
) RETURN t_inq_ivr_cl_types pipelined;


  -------------------------------------------------------------------------------
  --                         ОТЧЕТ ПО CRM
  -------------------------------------------------------------------------------
  
   CURSOR cur_report_on_crm (
              I_INIT_TIME TIMESTAMP,
              I_FINISH_TIME TIMESTAMP,
              I_CHECK_REPORT NUMBER := 0
    )
  IS
       WITH
  GIS_ZHKH AS (SELECT * FROM DUAL)
,  ALL_CALLS_PREV AS (
    SELECT 
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
    GROUP BY CL.SESSION_ID
  )  
,  ALL_CALLS AS (
      SELECT
        CL.SESSION_ID
      , CL.ID_CALL AS ID_CALL
      , TAB.START_PERIOD as PERIOD
      , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL --Отвеченные операторами
       , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CALL_RESULT_NUM_SECOND = 1
            AND TAB.CONNECT_RESULT_NUM = 2 AND TAB.CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL_SECOND --Отвеченные операторами   
    --  , TAB.SECOND_LINE   
      FROM ALL_CALLS_PREV CL
      JOIN (
            SELECT tab.*,
                   ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
            FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
            WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
            UNION ALL
            SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
            WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
              AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
            
            ) TAB--ZHKKH-490
       ON TAB.SESSION_ID = CL.SESSION_ID and RN = 1
      WHERE
  --    CL.PROJECT_ID = 'project245' AND
  --    AND LENGTH(CL.CALLER) = 10
  --    AND SUBSTR(CL.CALLER, -10) NOT IN ('4957392201')
                    --По заявке ZHKKH-490:
          --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
          --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
            (
            (TAB.CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and TAB.CALLER NOT IN ('4957392201','957392201'))
         OR ((TAB.CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
              TAB.CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(TAB.CALLER, -10) NOT IN ('4957392201'))
         OR (TAB.CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
            )
    )
 , FORMAT AS --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
        (
               SELECT DECODE(ID_TYPE,
                                    13,9.5,
                                    --12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES 
                                            WHERE CODE !=  'test'
                                              and ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
              -- UNION
              -- SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
              -- UNION
              -- SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
               ORDER BY ID_TYPE
         )
  , GROUP_VALUES AS (
  --Группировка по ОГРН
  SELECT
  TTP.NAME AS NAME_TYPE,
  INC.COMPANY_OGRN AS GROUP_NUMBER,
  (CASE
    WHEN COUNT(INC.COMPANY_OGRN) >=4
    THEN 4
    ELSE COUNT(INC.COMPANY_OGRN)
  END) AS REPLAY_COUNT
  FROM
  ALL_CALLS ACL
  JOIN CORE_CALLS CL
   ON CL.ID_CALL = ACL.ID_CALL
  JOIN INC_CALL_CONTACT_DATA INC
   ON INC.FID_CALL = CL.ID_CALL AND INC.IS_PRIMARY = 1
  JOIN TICKETS_D_TYPES TTP 
   ON TTP.ID_TYPE = INC.FID_TYPE and  TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  WHERE
       INC.REFUSED_TO_COMPANY_OGRN = 0
   AND LENGTH(REGEXP_REPLACE(INC.COMPANY_OGRN,'\D','')) > 0
   AND ACL.ANS_CALL = 1 --Отвеченные операторами
   AND INC.IS_LEGAL_ENTITY = 1 --Является ли юридическим лицом (0 - физическое, 1 - юридическое)
   AND I_CHECK_REPORT = 0 --OGRN
  GROUP BY TTP.NAME, INC.COMPANY_OGRN

  UNION ALL
  --Группировка по АОН
  SELECT
  TTP.NAME AS NAME_TYPE,
  CL.CALLER AS GROUP_NUMBER,
  (CASE
    WHEN COUNT(CL.CALLER) >=4
    THEN 4
    ELSE COUNT(CL.CALLER)
  END) AS REPLAY_COUNT
  FROM
  ALL_CALLS ACL
  JOIN CORE_CALLS CL
   ON CL.ID_CALL = ACL.ID_CALL
  JOIN INC_CALL_CONTACT_DATA INC
   ON INC.FID_CALL = CL.ID_CALL AND INC.IS_PRIMARY = 1
  JOIN TICKETS_D_TYPES TTP
   ON TTP.ID_TYPE = INC.FID_TYPE and TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  WHERE
    (INC.REFUSED_TO_COMPANY_OGRN = 1 OR LENGTH(REGEXP_REPLACE(INC.COMPANY_OGRN,'\D','')) = 0)
   AND ACL.ANS_CALL = 1 --Отвеченные операторами 
   AND INC.IS_LEGAL_ENTITY = 0 --Является ли юридическим лицом (0 - физическое, 1 - юридическое)
   AND I_CHECK_REPORT = 1 --AOH
  GROUP BY TTP.NAME, CL.CALLER

 )
-- , SUM_VALUES AS --РАСПРЕДЕЛЕНИЕ ПО ТЕМАМ И ПОВТОРАМ
--  (SELECT * FROM
--  (SELECT NAME_TYPE, REPLAY_COUNT FROM GROUP_VALUES)
--  PIVOT
--  (COUNT(*) FOR REPLAY_COUNT IN (1 AS REPLAY_ONE, 2 AS REPLAY_TWO, 3 AS REPLAY_THREE, 4 AS REPLAY_FOUR))
--  ORDER BY NAME_TYPE
--  )
, SUM_VALUES AS (
   SELECT 
     NAME_TYPE
   , SUM(CASE
         WHEN REPLAY_COUNT = 1
         THEN 1
         ELSE 0
       END) AS REPLAY_ONE
   , SUM(CASE
         WHEN REPLAY_COUNT = 2
         THEN 1
         ELSE 0
       END) AS REPLAY_TWO
   , SUM(CASE
         WHEN REPLAY_COUNT = 3
         THEN 1
         ELSE 0
       END) AS REPLAY_THREE
   , SUM(CASE
         WHEN REPLAY_COUNT = 4
         THEN 1
         ELSE 0
       END) AS REPLAY_FOUR       
   FROM GROUP_VALUES
   GROUP BY NAME_TYPE
)
  
--СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
SELECT
    DECODE(GROUPING(FT.NAME)
                ,0,FT.NAME,'Всего') AS LAST_TYPE --Классификация по теме
  , MAX(FT.ID_TYPE) AS ID_TYPE
  , SUM(NVL(SV.REPLAY_ONE,0)) AS REPLAY_ONE --Количество однократных звонков
  , SUM(NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ) AS REPLAY_MULTI --Количество повторных звонков
  , SUM(NVL(SV.REPLAY_TWO,0)) AS REPLAY_TWO  --Количество двукратных звонков
  , SUM(NVL(SV.REPLAY_THREE,0)) AS REPLAY_THREE  --Кол-во трехкратных звонков
  , SUM(NVL(SV.REPLAY_FOUR,0)) AS REPLAY_FOUR  --Количество четырехкратных и более звонков
  , SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ) AS ALL_CALLS
  , REPLACE(TRIM(TO_CHAR(ROUND(
    SUM(NVL(SV.REPLAY_ONE,0)) /
          DECODE( SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ),
           0, 1,
                SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) )
          )*100,2))),'.',',')||'%' AS PROCENT_UNIC

  , REPLACE(TRIM(TO_CHAR(ROUND(
      SUM(NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ) /
            DECODE( SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ),
             0, 1,
                  SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) )
            ) *100,2))),'.',',')||'%' AS PROCENT_MULTI

--  , REPLACE(TRIM(TO_CHAR(
--      SUM(NVL(SV.REPLAY_ONE,0)) /
--            DECODE( SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ),
--             0, 1,
--                  SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) )
--            )*100,'990D99')),'.',',')||'%' AS PROCENT_UNIC
--
--  , REPLACE(TRIM(TO_CHAR(
--      SUM(NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ) /
--            DECODE( SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) ),
--             0, 1,
--                  SUM(NVL(SV.REPLAY_ONE,0) + NVL(SV.REPLAY_TWO,0) + NVL(SV.REPLAY_THREE,0) + NVL(SV.REPLAY_FOUR,0) )
--            ) *100,'990D99')),'.',',')||'%' AS PROCENT_MULTI
  FROM SUM_VALUES SV
  RIGHT JOIN FORMAT FT ON FT.NAME = SV.NAME_TYPE

  GROUP BY ROLLUP(FT.NAME)

  ORDER BY GROUPING(FT.NAME),ID_TYPE 
;

   TYPE t_report_on_crm  IS TABLE OF cur_report_on_crm%rowtype;

  FUNCTION fnc_report_on_crm 
  (
          I_INIT_TIME TIMESTAMP,
          I_FINISH_TIME TIMESTAMP,
          I_CHECK_REPORT NUMBER := 0


  ) RETURN t_report_on_crm pipelined; 


---------------------------------------------------------
--         ОТЧЕТ Сроки обработки обращений
---------------------------------------------------------

CURSOR cur_TICKETS_proc_time (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL
  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL)
, PERIODS AS
      (
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                I_INIT_TIME,
                I_FINISH_TIME,
                NVL(LOWER(I_GROUP), 'year'),
                DECODE(I_GROUP,'minute',15,1)
                ))

  )

, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS FID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  --, MAX(TDT.NAME) KEEP (DENSE_RANK FIRST ORDER BY TTP.ID_HAS) AS LAST_TYPE

  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП


  WHERE -- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
, TYPE_FIRST_MESSAGE AS (  --ОПРЕДЕЛЯЕТ ТИП ПЕРВОГО ПИСЬМА
  SELECT
    TCK.ID_TICKET AS FID_TICKET
  , MAX(MTP.CODE) KEEP (DENSE_RANK FIRST ORDER BY MSG.ID_MESSAGE) AS CODE_MESSAGE

  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN MAIL_D_MSG_TYPES MTP
   ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП


  WHERE-- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )    
, ALL_TICKET_TASKS AS (
    SELECT DISTINCT TTS.FID_TICKET
    FROM TICKETS_TASKS TTS
    JOIN TICKETS TCK
     ON TCK.ID_TICKET = TTS.FID_TICKET
    WHERE-- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME) 
    )
,  TICKET_PROCESSING_TIME AS  (--определяет время РЕГИСТРАЦИИ
      SELECT
         TCK.ID_TICKET AS FID_TICKET
       , MIN(ACL.CREATED_AT) AS REGISTER_TIME

            
      FROM TICKETS TCK 
      JOIN TICKETS_D_SOURCE TSR
       ON TSR.ID_SOURCE = TCK.FID_SOURCE
      JOIN TICKETS_D_STATUSES TST
       ON TST.ID_STATUS = TCK.FID_STATUS
      JOIN USER_ACTIONS_LOG ACL
       ON ACL.LOGGABLE_ID = TCK.ID_TICKET AND LOGGABLE_TYPE = 'TICKETS'
      JOIN USER_ACTION_TYPES ACT
       ON ACT.ID_TYPE = ACL.FID_TYPE


      WHERE -- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME)
       AND TCK.IS_ACTIVE = 1
       AND TST.CODE = 'resolved'
       AND ACT.CODE = 'ticket-register'
      GROUP BY TCK.ID_TICKET

)    
,  TICKET_RESOLVED_TIME AS  (--определяет время РЕГИСТРАЦИИ
      SELECT
         TCK.ID_TICKET AS FID_TICKET
       , MAX(TSC.CREATED_AT) AS RESOLVED_TIME

            
      FROM TICKETS TCK 
      JOIN TICKETS_STATUS_CHANGES TSC
       ON TSC.FID_TICKET = TCK.ID_TICKET
      JOIN TICKETS_D_STATUSES TST
       ON TST.ID_STATUS = TSC.FID_STATUS  
      
      WHERE -- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME)
       AND TCK.IS_ACTIVE = 1
       AND TST.CODE = 'resolved'
      GROUP BY TCK.ID_TICKET

)    
, ALL_TICKETS_PREP AS
  (
  --E-mail
  SELECT
    TCK.ID_TICKET
  , TTP.LAST_TYPE AS LAST_TYPE
  , PR.START_PERIOD AS PERIOD
  , (CASE
      WHEN TFM.CODE_MESSAGE = 'web_form_new'
      THEN TPT.REGISTER_TIME
      ELSE TCK.CREATED_AT
     END) AS CREATED_TIME
  ,  NVL(TRT.RESOLVED_TIME,
         (CASE
          WHEN TST.CODE = 'resolved' THEN TCK.UPDATED_AT
          ELSE TCK.UPDATED_AT - 5
          END)
         ) AS RESOLVED_TIME
  , (CASE WHEN TTS.FID_TICKET IS NULL THEN 0 ELSE 1 END) AS SECOND_LINE
    FROM
     PERIODS PR -- тут от даты резервирования номера
    JOIN TICKETS TCK ON nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= PR.START_PERIOD AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < PR.STOP_PERIOD
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE
    JOIN TICKETS_D_STATUSES TST
     ON TST.ID_STATUS = TCK.FID_STATUS
    JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.FID_TICKET = TCK.ID_TICKET
    LEFT JOIN ALL_TICKET_TASKS TTS
     ON TTS.FID_TICKET = TCK.ID_TICKET
    LEFT JOIN TICKET_PROCESSING_TIME TPT 
     ON TPT.FID_TICKET = TCK.ID_TICKET     
    LEFT JOIN TYPE_FIRST_MESSAGE TFM 
     ON TFM.FID_TICKET = TCK.ID_TICKET 
    LEFT JOIN TICKET_RESOLVED_TIME TRT
     ON TRT.FID_TICKET = TCK.ID_TICKET
     
    WHERE TCK.IS_ACTIVE = 1
      AND TST.CODE IN ('resolved','closed')

  )
  
, ALL_TICKETS AS (
   
   SELECT 
     ID_TICKET
   , LAST_TYPE
   , PERIOD
   , CREATED_TIME
   , RESOLVED_TIME
   , SECOND_LINE
   FROM ALL_TICKETS_PREP
   WHERE (RESOLVED_TIME >= I_INIT_TIME AND RESOLVED_TIME < I_FINISH_TIME)
      AND (RESOLVED_TIME >= CREATED_TIME) 
      --нужно чтобы время создания и время резервирования входили в выбранный промежуток времени  
 
 )  
  
, FORMAT AS (
  SELECT * FROM PERIODS
  ,(--ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
               SELECT DECODE(ID_TYPE,
                                    13,9.5,
                                    12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES where ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
               UNION
               SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
               UNION
               SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
               ORDER BY ID_TYPE
             ) TTP

               ORDER BY START_PERIOD,ID_TYPE
  )  
, PREPARE_STATISTIC AS (
    SELECT 
      DECODE(GROUPING(FT.START_PERIOD)
                  ,0,FT.NAME,'Всего') AS LAST_TYPE --Классификация по теме
    , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
    , MAX(FT.ID_TYPE) AS ID_TYPE
    , SUM(CASE WHEN ATK.SECOND_LINE = 0 THEN 1 ELSE 0 END) AS RESOLVED_CC_COUNT -- Решено КЦ
    , SUM(CASE WHEN ATK.SECOND_LINE = 0 THEN NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME) ELSE 0 END) AS RESOLVED_CC_TIME
    , SUM(CASE WHEN ATK.SECOND_LINE = 1 THEN 1 ELSE 0 END) AS RESOLVED_SECOND_LINE_COUNT -- Решено с участием 2-й линии
    , SUM(CASE WHEN ATK.SECOND_LINE = 1 THEN NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME) ELSE 0 END) AS RESOLVED_SECOND_LINE_TIME
    , COUNT(ATK.ID_TICKET) AS RESOLVED_ALL_COUNT -- Решено ВСЕГО
    , NVL(SUM(NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME)),0) AS RESOLVED_ALL_TIME
    FROM ALL_TICKETS ATK
    RIGHT JOIN FORMAT FT
     ON FT.NAME = ATK.LAST_TYPE AND FT.START_PERIOD = ATK.PERIOD
        
    GROUP BY ROLLUP(FT.START_PERIOD, FT.NAME)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)
    ORDER BY FT.START_PERIOD, ID_TYPE --СОРТИРОВКА КАК В ТЗ
  
  )
  SELECT 
    LAST_TYPE -- Классификатор
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_CC_TIME/DECODE(RESOLVED_CC_COUNT,0,1,RESOLVED_CC_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_CC_AVG --Решено КЦ, часов
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_SECOND_LINE_TIME/DECODE(RESOLVED_SECOND_LINE_COUNT,0,1,RESOLVED_SECOND_LINE_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_SECOND_LINE_AVG -- Решено с участием 2-й линии, часов
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_ALL_TIME/DECODE(RESOLVED_ALL_COUNT,0,1,RESOLVED_ALL_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_ALL_AVG --Общий итог

  , ceil(NVL(RESOLVED_CC_TIME/DECODE(RESOLVED_CC_COUNT,0,1,RESOLVED_CC_COUNT),0)/3600) AS RESOLVED_CC_AVG --Решено КЦ, часов
  , ceil(NVL(RESOLVED_SECOND_LINE_TIME/DECODE(RESOLVED_SECOND_LINE_COUNT,0,1,RESOLVED_SECOND_LINE_COUNT),0)/3600) AS RESOLVED_SECOND_LINE_AVG -- Решено с участием 2-й линии, часов
  , ceil(NVL(RESOLVED_ALL_TIME/DECODE(RESOLVED_ALL_COUNT,0,1,RESOLVED_ALL_COUNT),0)/3600) AS RESOLVED_ALL_AVG --Общий итог
  FROM PREPARE_STATISTIC
  WHERE LAST_TYPE IS NOT NULL
  ;

    TYPE t_TICKETS_proc_time IS TABLE OF cur_TICKETS_proc_time%rowtype;

  FUNCTION fnc_TICKETS_proc_time
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_TICKETS_proc_time pipelined;


--------------------------------------------------------------
--     СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ MAILREADER    --
--------------------------------------------------------------

CURSOR cur_tickets_statistic (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL),
 PERIODS AS
      (
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
            TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy') AS VIEW_PERIOD
--          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
--          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                NVL2(
                      LOWER(I_GROUP),
                      CAST(TRUNC(I_INIT_TIME) AS TIMESTAMP),
                      I_INIT_TIME
                    ),
                I_FINISH_TIME, NVL(LOWER(I_GROUP), 'year')))
      )
,  COMPANY_TYPE_FOR_FILTER AS --ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
  (SELECT
     ID_COMPANY_TYPE
   , NAME AS FULL_NAME
   , COALESCE(SHORT_NAME, NAME) AS NAME
   , (CASE
       WHEN NAME = 'Гражданин' THEN 'По гражданам'
       WHEN NAME = 'Не определено' THEN 'Не определено'
       ELSE 'По организациям'
      END) AS CLIENT_TYPE
   FROM TICKETS_D_COMPANY_TYPES

  )

, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
  , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
,  ALL_CALLS_PREV AS (
    SELECT 
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
    GROUP BY CL.SESSION_ID
  )  
,  ALL_CALLS AS (
      SELECT
        CL.SESSION_ID
      , CL.ID_CALL AS ID_CALL
      , TAB.START_PERIOD as PERIOD
      , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL --Отвеченные операторами
       , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CALL_RESULT_NUM_SECOND = 1
            AND TAB.CONNECT_RESULT_NUM = 2 AND TAB.CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL_SECOND --Отвеченные операторами   
    --  , TAB.SECOND_LINE   
      FROM ALL_CALLS_PREV CL
      JOIN (
            SELECT tab.*,
                   ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
            FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
            WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
            UNION ALL
            SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
            WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
              AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
            
            ) TAB--ZHKKH-490
       ON TAB.SESSION_ID = CL.SESSION_ID and TAB.RN = 1
      WHERE
  --    CL.PROJECT_ID = 'project245' AND
  --    AND LENGTH(CL.CALLER) = 10
  --    AND SUBSTR(CL.CALLER, -10) NOT IN ('4957392201')
                    --По заявке ZHKKH-490:
          --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
          --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
            (
            (TAB.CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and TAB.CALLER NOT IN ('4957392201','957392201'))
         OR ((TAB.CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
              TAB.CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(TAB.CALLER, -10) NOT IN ('4957392201'))
         OR (TAB.CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
            )
    )
, ALL_TICKETS AS
  (
  --E-mail
  SELECT
    TTP.LAST_TYPE AS LAST_TYPE
  , 'MAILREADER' AS LINE
  , PR.START_PERIOD AS PERIOD
  , NULL AS ANS_CALL_SECOND
    FROM
     PERIODS PR
    JOIN TICKETS TCK ON nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= PR.START_PERIOD AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < PR.STOP_PERIOD
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE
    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.ID_TICKET = TCK.ID_TICKET
    WHERE
           (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
       AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
       AND TCK.IS_ACTIVE = 1

  UNION ALL
  -- E-mail (Из них заявок на 2-ю линию, шт. )
  SELECT
    TTP.LAST_TYPE AS LAST_TYPE
  , 'MAILREADER_LINE_3' AS LINE
  , PR.START_PERIOD AS PERIOD
  , NULL AS ANS_CALL_SECOND
    FROM
     PERIODS PR
    JOIN TICKETS TCK ON nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= PR.START_PERIOD AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < PR.STOP_PERIOD
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE
    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.ID_TICKET = TCK.ID_TICKET
    JOIN (SELECT DISTINCT FID_TICKET FROM TICKETS_TASKS) TTS
     ON TTS.FID_TICKET = TCK.ID_TICKET
    WHERE
           (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
       AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
       AND TCK.IS_ACTIVE = 1

  UNION ALL
  -------------------------------------
  -- Голос
  -------------------------------------
   SELECT
    (CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
     END) AS LAST_TYPE -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
  , 'INCOMING' AS LINE
  , PR.START_PERIOD AS PERIOD --ACL.PERIOD AS PERIOD
  , ACL.ANS_CALL_SECOND
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD 
   LEFT JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
      AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
      AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND CL.FID_RESULT IN (4,5,6,7,8,10,11)
      AND ACL.ANS_CALL = 1

  UNION ALL
  --Голос
   SELECT
   (CASE
     WHEN CCD.IS_PRIMARY = 1
     THEN TTP.NAME
     ELSE ''
    END) AS LAST_TYPE -- Подстатус звонка
  , 'INCOMING' AS LINE
  , PR.START_PERIOD AS PERIOD --ACL.PERIOD AS PERIOD
  , ACL.ANS_CALL_SECOND
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD
   JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL --AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_TYPES TTP
    ON TTP.ID_TYPE = CCD.FID_TYPE  AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
      AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
      AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND CL.FID_RESULT in (1,2,3,9)
      AND ACL.ANS_CALL = 1

  UNION ALL
  --Голос (Оформлено заявок на 2-ю линию, шт.)
  SELECT
    TTP.NAME AS LAST_TYPE -- Подстатус звонка
  , 'INCOMING_LINE_3' AS LINE
  , PR.START_PERIOD AS PERIOD
  , ACL.ANS_CALL_SECOND
   FROM PERIODS PR
   JOIN CORE_CALLS CL 
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD
   JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL --AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_TYPES TTP 
    ON TTP.ID_TYPE = CCD.FID_TYPE  AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   LEFT JOIN ALL_CALLS ACL
    ON ACL.ID_CALL = CL.ID_CALL
   WHERE  CL.CREATED_AT BETWEEN I_INIT_TIME AND I_FINISH_TIME
      AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
      AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND LOWER(CL.DIRECTION) = 'in'
      AND CCD.FID_MESSAGE_MAIL IS NOT NULL

  )
--, SUM_TICKETS AS --РАСПРЕДЕЛЕНИЕ ПО ТЕМАМ И ЛИНИЯМ
--  ( SELECT * FROM
--     (SELECT * FROM ALL_TICKETS)
--  PIVOT
--    (COUNT(*) FOR LINE IN ('INCOMING_FIRST' AS INCOMING_FIRST,
--                           'INCOMING_SECOND' AS INCOMING_SECOND,
--                           'INCOMING_LINE_3_FIRST' AS INCOMING_LINE_3_FIRST,
--                           'INCOMING_LINE_3_SECOND' AS INCOMING_LINE_3_SECOND,
--                           'MAILREADER' AS MAILREADER,
--                           'MAILREADER_LINE_3' AS MAILREADER_LINE_3
--                          )
--    )
--  ORDER BY LAST_TYPE
--  )
, SUM_TICKETS AS 
  ( 
   SELECT
     LAST_TYPE
   , PERIOD
   , SUM(CASE
       WHEN LINE = 'INCOMING'
       THEN 1
       ELSE 0
     END) AS INCOMING_FIRST
   , SUM(CASE
       WHEN LINE = 'INCOMING' AND ANS_CALL_SECOND = 1
       THEN 1
       ELSE 0
     END) AS INCOMING_SECOND
   , SUM(CASE
       WHEN LINE = 'INCOMING_LINE_3'
       THEN 1
       ELSE 0
     END) AS INCOMING_LINE_3_FIRST
   , SUM(CASE
       WHEN LINE = 'INCOMING_LINE_3' AND ANS_CALL_SECOND = 1
       THEN 1
       ELSE 0
     END) AS INCOMING_LINE_3_SECOND
   , SUM(CASE
       WHEN LINE = 'MAILREADER'
       THEN 1
       ELSE 0
     END) AS MAILREADER
   , SUM(CASE
       WHEN LINE = 'MAILREADER_LINE_3'
       THEN 1
       ELSE 0
     END) AS MAILREADER_LINE_3
    FROM  ALL_TICKETS
     GROUP BY LAST_TYPE, PERIOD

  
  )
  
  
, FORMAT AS (
  SELECT * FROM PERIODS
  ,(--ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
               SELECT DECODE(ID_TYPE,
                                    13,9.5,
                                    12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES where ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
               UNION
               SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
               UNION
               SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
               ORDER BY ID_TYPE
             ) TTP

               ORDER BY START_PERIOD,ID_TYPE
  )
, SUM_TICKETS_2 AS --ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
 (SELECT
    DECODE(GROUPING(FT.START_PERIOD)
                ,0,FT.NAME,'Всего') AS LAST_TYPE --Классификация по теме
  , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
  , MAX(FT.ID_TYPE) AS ID_TYPE
  , SUM(NVL(ST.INCOMING_FIRST,0)) AS INCOMING_FIRST --Входящая линия --1-Я ЛИНИЯ
  , SUM(NVL(ST.INCOMING_SECOND,0)) AS INCOMING_SECOND --Входящая линия --2-Я ЛИНИЯ
  , SUM(NVL(ST.INCOMING_FIRST,0)+NVL(ST.INCOMING_SECOND,0)) AS INCOMING --Входящая линия
 -- , SUM(NVL(ST.INCOMING_LINE_3,0)) AS INCOMING_LINE_3 --Входящая линия
  
  , SUM(DECODE(LAST_TYPE,
                    'Тестовое обращение',0,
                    NVL(ST.INCOMING_LINE_3_FIRST,0))) AS INCOMING_LINE_3_FIRST --Входящая линия --1-Я ЛИНИЯ
                    
  , SUM(DECODE(LAST_TYPE,
                    'Тестовое обращение',0,
                    NVL(ST.INCOMING_LINE_3_SECOND,0))) AS INCOMING_LINE_3_SECOND --Входящая линия   
                    
  , SUM(DECODE(LAST_TYPE,
                    'Тестовое обращение',0,
                    NVL(ST.INCOMING_LINE_3_FIRST,0) + NVL(ST.INCOMING_LINE_3_SECOND,0))) AS INCOMING_LINE_3                     
  
  , SUM(NVL(ST.MAILREADER,0)) AS MAILREADER  --MailReader
  , SUM(NVL(ST.MAILREADER_LINE_3,0)) AS MAILREADER_LINE_3  --MailReader
  , SUM(NVL(ST.MAILREADER,0)+NVL(ST.INCOMING_FIRST,0)+ NVL(ST.INCOMING_SECOND,0)) AS ITOGO --Итого
  FROM SUM_TICKETS ST
  RIGHT JOIN FORMAT FT ON FT.NAME = ST.LAST_TYPE AND FT.START_PERIOD = ST.PERIOD

  GROUP BY ROLLUP(FT.START_PERIOD, FT.NAME)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)

  ORDER BY FT.START_PERIOD, ID_TYPE --СОРТИРОВКА КАК В ТЗ
  )
  SELECT
    LAST_TYPE --Классификация по теме
  , NVL2(I_GROUP,PERIOD,'') AS PERIOD
  , INCOMING_FIRST --Голос 1-Я
  , INCOMING_SECOND --Голос 2-Я
  , INCOMING --Голос, ВСЕГО
  
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0',
                    INCOMING_LINE_3_FIRST) AS INCOMING_LINE_3_FIRST
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0,00%',
                    REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3_FIRST/DECODE(INCOMING_FIRST,0,1,INCOMING_FIRST),0)*100,'990D99')),'.',',')||'%') AS INCOMING_FIRST_PROCENT
                    
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0',
                    INCOMING_LINE_3_SECOND) AS INCOMING_LINE_3_SECOND
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0,00%',
                    REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3_SECOND/DECODE(INCOMING_SECOND,0,1,INCOMING_SECOND),0)*100,'990D99')),'.',',')||'%') AS INCOMING_SECOND_PROCENT
  
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0',
                    INCOMING_LINE_3) AS INCOMING_LINE_3
  , DECODE(LAST_TYPE,
                    'Тестовое обращение','0,00%',
                    REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3/DECODE(INCOMING,0,1,INCOMING),0)*100,'990D99')),'.',',')||'%') AS INCOMING_PROCENT
                    
                    
  , DECODE(LAST_TYPE,
                     'Посторонний звонок','-',
                     MAILREADER) AS MAILREADER --E-mail
  , DECODE(LAST_TYPE,
                     'Посторонний звонок','-',
                     MAILREADER_LINE_3) AS MAILREADER_LINE_3 --E-mail
  , REPLACE(TRIM(TO_CHAR(NVL(MAILREADER_LINE_3/DECODE(MAILREADER,0,1,MAILREADER),0)*100,'990D99')),'.',',')||'%' AS MAILREADER_PROCENT
  , ITOGO --Итого

  FROM
  SUM_TICKETS_2
  WHERE LAST_TYPE is not null --Убираем промежуточные суммы
  
  ;

  TYPE t_tickets_statistic IS TABLE OF cur_tickets_statistic%rowtype;

  FUNCTION fnc_tickets_statistic
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_tickets_statistic pipelined;



---------------------------------------------------------------
-- СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ В РАЗРЕЗЕ РЕГИОНОВ --
---------------------------------------------------------------
CURSOR cur_tickets_statistic_regions (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_DST_ID VARCHAR DEFAULT NULL
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL 

  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL),
 PERIODS AS
      (
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
--            TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy') AS VIEW_PERIOD
          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                I_INIT_TIME,
                I_FINISH_TIME,
                NVL(LOWER(I_GROUP), 'year')))
  )
, FORMAT AS (
   SELECT *
   FROM PERIODS, TICKETS_D_REGIONS
  )  
--  SELECT * FROM PERIODS;
, COMPANY_TYPE_FOR_FILTER AS --ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
  (SELECT
     ID_COMPANY_TYPE
   , NAME AS FULL_NAME
   , COALESCE(SHORT_NAME, NAME) AS NAME
   , (CASE
       WHEN NAME = 'Гражданин' THEN 'По гражданам'
       WHEN NAME = 'Не определено' THEN 'Не определено'
       ELSE 'По организациям'
      END) AS CLIENT_TYPE
   FROM TICKETS_D_COMPANY_TYPES

  )
,  ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.ID_TYPE) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
  , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM
  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
,  ALL_CALLS_PREV AS (
    SELECT 
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
    GROUP BY CL.SESSION_ID
  )  
,  ALL_CALLS AS (
      SELECT
        CL.SESSION_ID
      , CL.ID_CALL AS ID_CALL
      , TAB.START_PERIOD as PERIOD
      , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL --Отвеченные операторами
       , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CALL_RESULT_NUM_SECOND = 1
            AND TAB.CONNECT_RESULT_NUM = 2 AND TAB.CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL_SECOND --Отвеченные операторами   
    --  , TAB.SECOND_LINE   
      FROM ALL_CALLS_PREV CL
      JOIN (
            SELECT tab.*,
                   ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
            FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_INIT_TIME, I_FINISH_TIME)) tab
            WHERE I_FINISH_TIME > TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME
            UNION ALL
            SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
            WHERE (I_FINISH_TIME <= TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
              AND (tab.CALL_CREATED >= I_INIT_TIME AND tab.CALL_CREATED < I_FINISH_TIME)
            ) TAB--ZHKKH-490
       ON TAB.SESSION_ID = CL.SESSION_ID and RN = 1
      WHERE
  --    CL.PROJECT_ID = 'project245' AND
  --    AND LENGTH(CL.CALLER) = 10
  --    AND SUBSTR(CL.CALLER, -10) NOT IN ('4957392201')
                    --По заявке ZHKKH-490:
          --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
          --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
            (
            (TAB.CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and TAB.CALLER NOT IN ('4957392201','957392201'))
         OR ((TAB.CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
              TAB.CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(TAB.CALLER, -10) NOT IN ('4957392201'))
         OR (TAB.CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
            )
            
        AND (  (I_DST_ID = '88003020305' and DST_ID IN ('4957392507','5555319863','5555319862'))
            or (I_DST_ID = '84957718181' and DST_ID IN ('4957392209','5555392209','5555392210'))
            or I_DST_ID is null                  
             )    
    )
, ALL_TICKETS_CALLS AS
  (SELECT
    NVL(TCK.FID_COMPANY_REGION,85) AS FID_COMPANY_REGION
  , TTP.LAST_TYPE AS LAST_TYPE-- Все обращения
  , PR.START_PERIOD AS PERIOD
  , PR.VIEW_PERIOD
  , 2 AS FID_SOURCE
   FROM
     PERIODS PR
    JOIN TICKETS TCK ON nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= PR.START_PERIOD AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < PR.STOP_PERIOD
   JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE 
   LEFT JOIN ALL_TICKETS_TYPES TTP
    ON TTP.ID_TICKET = TCK.ID_TICKET
  WHERE
           (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
       AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
       AND TCK.IS_ACTIVE = 1
  UNION ALL
   SELECT --не всегда пишется регион, как быть??????????
    NVL(CCD.FID_COMPANY_REGION,85) AS FID_COMPANY_REGION
  , (CASE
     WHEN CL.FID_RESULT IN (5,6,7,8)
      THEN -1  -- Посторонний звонок
     WHEN CL.FID_RESULT = 4
      THEN -2  -- Посторонний звонок (Тестовый звонок)
     END) AS LAST_TYPE -- Посторонний звонок
  , PR.START_PERIOD AS PERIOD
  , PR.VIEW_PERIOD     
  , 1 AS FID_SOURCE
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD  
   LEFT JOIN INC_CALL_CONTACT_DATA CCD -- НЕ ВСЕГДА СОХРАНЯЕТСЯ АНКЕТА
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
          AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
          AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
          AND CL.FID_RESULT IN (4,5,6,7,8,10,11)
          AND ACL.ANS_CALL = 1
         -- AND CL.PROJECT_ID = 'project245'
  UNION ALL
    SELECT
    NVL(CCD.FID_COMPANY_REGION,85) AS FID_COMPANY_REGION
  , TTP.ID_TYPE AS LAST_TYPE -- ПОДСТАТУС ЗВОНКА
  , PR.START_PERIOD AS PERIOD
  , PR.VIEW_PERIOD  
  , 1 AS FID_SOURCE
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD  
   JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_TYPES TTP
    ON TTP.ID_TYPE = CCD.FID_TYPE  AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
          AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
          AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
          AND CL.FID_RESULT in (1,2,3,9)
          AND ACL.ANS_CALL = 1

  )
, ALL_TICKETS AS
  (
  SELECT * FROM ALL_TICKETS_CALLS
  WHERE (FID_SOURCE = I_CHANNEL OR I_CHANNEL IS NULL)
  )
, SUM_TICKETS AS --ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
 (SELECT
    DECODE(GROUPING(FT.VIEW_PERIOD)
                ,0,FT.VIEW_PERIOD,'Всего') AS PERIOD --Период   
  , FT.START_PERIOD
  , FT.NAME AS REGION
  , MAX(FT.CODE) AS CODE           
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 1
     THEN 1
     ELSE 0
    END) AS SYS_ERRORS --Ошибки системы
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 2
     THEN 1
     ELSE 0
    END) AS PROP_WORK_SYS --Предложения по работе системы
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 3
     THEN 1
     ELSE 0
    END) AS QUES_WORK_SYS --Вопрос по работе системы
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 4
     THEN 1
     ELSE 0
    END) AS INTEGRATION --Интеграция
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 5
     THEN 1
     ELSE 0
    END) AS QUES_LEGISLATION --Вопрос по законодательству
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 6
     THEN 1
     ELSE 0
    END) AS HARDWARE_SOFTWARE --Техническое и программное обеспечение
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 7
     THEN 1
     ELSE 0
    END) AS LICENSING --Лицензирование
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 8
     THEN 1
     ELSE 0
    END) AS REGISTRATION --Регистрация
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 9
     THEN 1
     ELSE 0
    END) AS ADD_ADDRESS --Добавление адреса
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 10
     THEN 1
     ELSE 0
    END) AS OTHER --Другое
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 11
     THEN 1
     ELSE 0
    END) AS DOCUMENTATION --Другое
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = 13
     THEN 1
     ELSE 0
    END) AS PAYMENT --Другое    
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE = -1
     THEN 1
     ELSE 0
    END) AS UNUSUAL_CALL --ПОСТОРОННИЙ ЗВОНОК
  , SUM(
    CASE
    WHEN ATC.LAST_TYPE IN (-2,12)
     THEN 1
     ELSE 0
    END) AS UNUSUAL_CALL_TEST --Посторонний звонок (Тестовый звонок)
  , COUNT(ATC.LAST_TYPE) AS ITOGO
  FROM ALL_TICKETS ATC
  RIGHT JOIN FORMAT FT ON FT.ID_REGION = ATC.FID_COMPANY_REGION AND FT.START_PERIOD = ATC.PERIOD --лучше сделать по коду
  GROUP BY ROLLUP(FT.VIEW_PERIOD, FT.START_PERIOD, FT.NAME)
  ORDER BY GROUPING(FT.VIEW_PERIOD), FT.START_PERIOD, CODE 
  )
--  SELECT * FROM SUM_TICKETS;

  SELECT
    (CASE WHEN I_GROUP IS NULL AND PERIOD != 'Всего' THEN '' ELSE PERIOD END) AS PERIOD --Период
  , REGION --Регион
  , SYS_ERRORS --Ошибки системы
  , PROP_WORK_SYS --Предложения по работе системы
  , QUES_WORK_SYS --Вопрос по работе системы
  , INTEGRATION --Интеграция
  , QUES_LEGISLATION --Вопрос по законодательству
  , HARDWARE_SOFTWARE --Техническое и программное обеспечение
  , LICENSING --Лицензирование
  , REGISTRATION --Регистрация
  , ADD_ADDRESS --Добавление адреса
  , PAYMENT
  , OTHER --Другое
  , DOCUMENTATION --Документация и инструкции
  , UNUSUAL_CALL --ПОСТОРОННИЙ ЗВОНОК
  , UNUSUAL_CALL_TEST --Посторонний звонок (Тестовый звонок)
  , ITOGO --Итого

  FROM
  SUM_TICKETS
  WHERE REGION IS NOT NULL OR PERIOD = 'Всего'
  ;


  TYPE t_tickets_statistic_regions IS TABLE OF cur_tickets_statistic_regions%rowtype;

  FUNCTION fnc_tickets_statistic_regions
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_DST_ID VARCHAR DEFAULT NULL --НАШ НОМЕР
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_tickets_statistic_regions pipelined;
  


---------------------------------------------------------------
--   Статистика по полномочиям -- тоже что и СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ, но ПО ПОЛНОМОЧИЯМ (TICKETS_D_COMPANY_TYPES) --
---------------------------------------------------------------
CURSOR cur_tickets_statistic_COMPANY (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_REGION NUMBER --ФИЛЬТР ПО РЕГИОНАМ
      , I_GROUP VARCHAR2 DEFAULT NULL
  )
IS
   WITH
   GIS_ZHKH AS (SELECT * FROM DUAL),
  PERIODS AS
      (
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
            TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy') AS VIEW_PERIOD
--          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
--          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                NVL2(
                      LOWER(I_GROUP),
                      CAST(TRUNC(I_INIT_TIME) AS TIMESTAMP),
                      I_INIT_TIME
                    ),
                I_FINISH_TIME, NVL(LOWER(I_GROUP), 'year')))
      ),

  COMPANY_TYPE_FOR_FILTER AS --ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
  (SELECT
     ID_COMPANY_TYPE
   , NAME AS FULL_NAME
   , COALESCE(SHORT_NAME, NAME) AS NAME
   , (CASE
       WHEN NAME = 'Гражданин' THEN 'По гражданам'
       WHEN NAME = 'Не определено' THEN 'Не определено'
       ELSE 'По организациям'
      END) AS CLIENT_TYPE
   FROM TICKETS_D_COMPANY_TYPES

  ),

 ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
  , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >=I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) <I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )    
,  ALL_CALLS_PREV AS (
    SELECT 
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
    GROUP BY CL.SESSION_ID
  )  
,  ALL_CALLS AS (
      SELECT
        CL.SESSION_ID
      , CL.ID_CALL AS ID_CALL
      , TAB.START_PERIOD as PERIOD
      , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL --Отвеченные операторами
       , (CASE
           WHEN TAB.CALL_RESULT_NUM = 1 AND TAB.CALL_RESULT_NUM_SECOND = 1
            AND TAB.CONNECT_RESULT_NUM = 2 AND TAB.CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
           THEN 1
           ELSE 0
         END) AS ANS_CALL_SECOND --Отвеченные операторами   
    --  , TAB.SECOND_LINE   
      FROM ALL_CALLS_PREV CL
      JOIN (
              SELECT tab.*,
                     ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
              FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_init_time, I_finish_time)) tab
              WHERE I_finish_time > TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time
              UNION ALL
              SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
              WHERE (I_finish_time <= TRUNC(SYSTIMESTAMP) AND I_init_time <= I_finish_time)
                AND (tab.CALL_CREATED >= I_init_time AND tab.CALL_CREATED < I_finish_time)
            
            ) TAB--ZHKKH-490
       ON TAB.SESSION_ID = CL.SESSION_ID and RN = 1
      WHERE
  --    CL.PROJECT_ID = 'project245' AND
  --    AND LENGTH(CL.CALLER) = 10
  --    AND SUBSTR(CL.CALLER, -10) NOT IN ('4957392201')
                    --По заявке ZHKKH-490:
          --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
          --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
            (
            (TAB.CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and TAB.CALLER NOT IN ('4957392201','957392201'))
         OR ((TAB.CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
              TAB.CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(TAB.CALLER, -10) NOT IN ('4957392201'))
         OR (TAB.CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
            )
    ) 
, ALL_TICKETS AS
  (
  --E-mail
  SELECT
    TTP.LAST_TYPE AS LAST_TYPE
  , 'MAILREADER' AS LINE
  , nvl(TTP.ID_COMPANY_TYPE,0) as FID_COMPANY_TYPE
  , PR.START_PERIOD AS PERIOD
  , NULL AS ANS_CALL_SECOND
    FROM
     PERIODS PR
    JOIN TICKETS TCK ON nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= PR.START_PERIOD AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < PR.STOP_PERIOD
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE
    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.ID_TICKET = TCK.ID_TICKET
    WHERE (TCK.FID_COMPANY_REGION = I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
      AND TCK.IS_ACTIVE = 1
  --  WHERE
  --         (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
  --     AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)

--  UNION ALL
  -- E-mail (Из них заявок на 2-ю линию, шт. )
--  SELECT
--    TTP.LAST_TYPE AS LAST_TYPE
--  , 'MAILREADER_LINE_3' AS LINE
--  , nvl(TCK.FID_COMPANY_TYPE,0) as FID_COMPANY_TYPE
--    FROM TICKETS TCK
--    JOIN TICKETS_D_SOURCE TSR
--     ON TSR.ID_SOURCE = TCK.FID_SOURCE
--    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
--     ON TTP.ID_TICKET = TCK.ID_TICKET
--    JOIN (SELECT DISTINCT FID_TICKET FROM TICKETS_TASKS) TTS
--     ON TTS.FID_TICKET = TCK.ID_TICKET
--   -- WHERE
--   --        (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
--   --    AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)

  UNION ALL
  -------------------------------------
  -- Голос
  -------------------------------------
   SELECT
    (CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
     END) AS LAST_TYPE -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
   , 'INCOMING' AS LINE
   , nvl(CCD.FID_COMPANY_TYPE,0) as FID_COMPANY_TYPE
   , PR.START_PERIOD AS PERIOD --ACL.PERIOD AS PERIOD
   , ACL.ANS_CALL_SECOND
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD 
   LEFT JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
--   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
--    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >=I_INIT_TIME AND CL.CREATED_AT <I_FINISH_TIME
     AND (CCD.FID_COMPANY_REGION =I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
   --   AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
   --   AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND CL.FID_RESULT IN (4,5,6,7,8,10,11)
      AND ACL.ANS_CALL = 1

  UNION ALL
  --Голос
   SELECT
   (CASE
     WHEN CCD.IS_PRIMARY = 1
     THEN TTP.NAME
     ELSE ''
    END) AS LAST_TYPE -- Подстатус звонка
   , 'INCOMING' AS LINE
   , nvl(CCD.FID_COMPANY_TYPE,0) as FID_COMPANY_TYPE 
   , PR.START_PERIOD AS PERIOD --ACL.PERIOD AS PERIOD
   , ACL.ANS_CALL_SECOND
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD  
   JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL --AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_TYPES TTP
    ON TTP.ID_TYPE = CCD.FID_TYPE AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
--   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
--    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >=I_INIT_TIME AND CL.CREATED_AT <I_FINISH_TIME
     AND (CCD.FID_COMPANY_REGION =I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
   --   AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
   --   AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND CL.FID_RESULT in (1,2,3,9)
      AND ACL.ANS_CALL = 1

--  UNION ALL
--  --Голос (Оформлено заявок на 2-ю линию, шт.)
--  SELECT
--    TTP.NAME AS LAST_TYPE -- Подстатус звонка
--  , (CASE
--       WHEN ACL.SECOND_LINE = 1
--       THEN 'INCOMING_LINE_3_SECOND' --СО ВТОРОЙ ЛИНИИ
--       ELSE 'INCOMING_LINE_3_FIRST'
--      END) AS LINE
--  , nvl(CCD.FID_COMPANY_TYPE,0) as FID_COMPANY_TYPE  
--   FROM CORE_CALLS CL 
--   JOIN INC_CALL_CONTACT_DATA CCD
--    ON CCD.FID_CALL = CL.ID_CALL --AND CCD.IS_PRIMARY = 1
--   JOIN TICKETS_D_TYPES TTP
--    ON TTP.ID_TYPE = CCD.FID_TYPE
----   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
----    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
--   LEFT JOIN ALL_CALLS ACL
--    ON ACL.ID_CALL = CL.ID_CALL
--   WHERE  CL.CREATED_AT BETWEEN I_INIT_TIME AND I_FINISH_TIME
--   --   AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
--   --   AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
--      AND LOWER(CL.DIRECTION) = 'in'
--      AND CCD.FID_MESSAGE_MAIL IS NOT NULL

  )    
--, SUM_TICKETS AS --РАСПРЕДЕЛЕНИЕ ПО ТЕМАМ И ЛИНИЯМ
--  ( SELECT * FROM
--     (SELECT * FROM ALL_TICKETS)
--  PIVOT
--    (COUNT(*) FOR LINE IN ('INCOMING_FIRST' AS INCOMING_FIRST,
--                           'INCOMING_SECOND' AS INCOMING_SECOND,
--                        --   'INCOMING_LINE_3_FIRST' AS INCOMING_LINE_3_FIRST,
--                        --   'INCOMING_LINE_3_SECOND' AS INCOMING_LINE_3_SECOND,
--                           'MAILREADER' AS MAILREADER
--                        --   'MAILREADER_LINE_3' AS MAILREADER_LINE_3
--                          )
--    )
--  ORDER BY LAST_TYPE
--  )
  , SUM_TICKETS AS 
  ( 
   SELECT
     FID_COMPANY_TYPE
   , LAST_TYPE
   , PERIOD
   , SUM(CASE
       WHEN LINE = 'INCOMING'
       THEN 1
       ELSE 0
     END) AS INCOMING_FIRST
--   , SUM(CASE
--       WHEN LINE = 'INCOMING' AND ANS_CALL_SECOND = 1
--       THEN 1
--       ELSE 0
--     END) AS INCOMING_SECOND
--   , SUM(CASE
--       WHEN LINE = 'INCOMING_LINE_3'
--       THEN 1
--       ELSE 0
--     END) AS INCOMING_LINE_3_FIRST
--   , SUM(CASE
--       WHEN LINE = 'INCOMING_LINE_3' AND ANS_CALL_SECOND = 1
--       THEN 1
--       ELSE 0
--     END) AS INCOMING_LINE_3_SECOND
   , SUM(CASE
       WHEN LINE = 'MAILREADER'
       THEN 1
       ELSE 0
     END) AS MAILREADER
--   , SUM(CASE
--       WHEN LINE = 'MAILREADER_LINE_3'
--       THEN 1
--       ELSE 0
--     END) AS MAILREADER_LINE_3
    FROM  ALL_TICKETS
     GROUP BY LAST_TYPE, PERIOD, FID_COMPANY_TYPE

  
  )
, FORMAT AS (

  SELECT distinct   --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
    PR.*,
    CTP.ID_COMPANY_TYPE,
    CTP.COMPANY_NAME,
    CTP.ord,
    TTP.*
  FROM PERIODS PR, 
   (SELECT 
      ID_COMPANY_TYPE,
      COALESCE(SHORT_NAME, NAME) AS COMPANY_NAME,
      ID_COMPANY_TYPE as ord
    FROM TICKETS_D_COMPANY_TYPES CTP
      UNION
     SELECT 
       0 AS ID_COMPANY_TYPE,
       'Не указан' AS NAME,
       1000 as ord
     FROM DUAL   
    ) CTP
  ,(
               SELECT DECODE(ID_TYPE,
                                    13,9.5,
                                    12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES where ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
               UNION
               SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
               UNION
               SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
               ORDER BY ID_TYPE
             ) TTP

               ORDER BY PR.START_PERIOD,CTP.ord,TTP.ID_TYPE
  )  
  
, SUM_TICKETS_2 AS --ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
 (SELECT
    DECODE(GROUPING(FT.START_PERIOD)
                ,0,FT.COMPANY_NAME,'Всего') AS COMPANY_NAME --Полномочие
 
  , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
  , FT.NAME AS LAST_TYPE --Классификация по теме
  , MAX(FT.ID_TYPE) AS ID_TYPE
  , MAX(FT.ORD) AS ORD
  , SUM(NVL(ST.INCOMING_FIRST,0)) AS INCOMING_FIRST --Входящая линия                
  , SUM(NVL(ST.MAILREADER,0)) AS MAILREADER  --MailReader
  , SUM(NVL(ST.MAILREADER,0)+NVL(ST.INCOMING_FIRST,0)) AS ITOGO --Итого
  FROM SUM_TICKETS ST
  RIGHT JOIN FORMAT FT ON FT.NAME = ST.LAST_TYPE
         AND FT.ID_COMPANY_TYPE = ST.FID_COMPANY_TYPE
         AND FT.START_PERIOD = ST.PERIOD

  GROUP BY ROLLUP(FT.START_PERIOD, FT.COMPANY_NAME, FT.NAME)

  ORDER BY  FT.START_PERIOD,ORD, ID_TYPE --СОРТИРОВКА КАК В ТЗ
  )
  
  SELECT
    COMPANY_NAME
  , LAST_TYPE --Классификация по теме
  , NVL2(I_GROUP,PERIOD,'') AS PERIOD
  , INCOMING_FIRST --Голос 1-Я (МЫ ТУТ СКЛЕИВАЕМ 2 ЛИНИИ В ОДНУ)
  , DECODE(LAST_TYPE,
                     'Посторонний звонок','-',
                     MAILREADER) AS MAILREADER --E-mail
  , ITOGO --Итого

  FROM
  SUM_TICKETS_2
  WHERE (LAST_TYPE is not null OR COMPANY_NAME = 'Всего') --Убираем промежуточные суммы
  ;


  TYPE t_tickets_statistic_COMPANY IS TABLE OF cur_tickets_statistic_COMPANY%rowtype;


  FUNCTION fnc_tickets_statistic_COMPANY
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_REGION NUMBER  --ФИЛЬТР ПО РЕГИОНАМ
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_tickets_statistic_COMPANY pipelined;
  
  
  
  -----------------------------------------------------------
--         ОТЧЕТ ПО СТАТУСАМ ОБРАЩЕНИЙ                   --
-----------------------------------------------------------

CURSOR cur_tickets_statuses (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  )
IS
WITH 
GIS_ZHKH AS (SELECT * FROM DUAL),
ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  FROM
  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
, ALL_TICKETS AS
  (SELECT
    TTP.LAST_TYPE AS LAST_TYPE
  , TST.CODE AS CODE_STATUS
    FROM
    TICKETS TCK
    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.ID_TICKET = TCK.ID_TICKET
    JOIN TICKETS_D_STATUSES TST
     ON TST.ID_STATUS = TCK.FID_STATUS
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE 
     WHERE (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
        AND TTP.LAST_TYPE IS NOT NULL
        AND TCK.IS_ACTIVE = 1
  )
, SUM_TICKETS AS --РАСПРЕДЕЛЕНИЕ ПО ТЕМАМ И ЛИНИЯМ
  (SELECT * FROM
  (SELECT * FROM ALL_TICKETS)
  PIVOT
  (COUNT(*) FOR CODE_STATUS IN (  'new' AS ST_NEW --Новое
                                   --В работе (в таблице пропущен статус)
                                , 'transferred-3rd-line' AS ST_TRANSFERRED_3RD_LINE --Переведен на специалиста 3й линии
                                , 'waiting-requester-answer' AS ST_WAITING_REQUESTER_ANSWER --Ожидается ответ заявителя
                                , 'need-call-to-requester' AS ST_NEED_CALL_TO_REQUESTER --Требуется исходящий звонок заявителю
                                , 'pre-resolved' AS ST_PRE_RESOLVED --Предварительное решение
                                , 'resolved' AS ST_RESOLVED --Решено
                                , 'closed-on-request' AS ST_CLOSED_ON_REQUEST --Закрыто по запросу
                                , 'closed' AS ST_CLOSED --Закрыто
                                , 'solved-by-fias' AS ST_SOLVED_BY_FIAS --Решено ФИАС
                               )
  )
  ORDER BY LAST_TYPE
  )
, SUM_TICKETS_2 AS --ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
 (SELECT
    DECODE(GROUPING(TTP.NAME)
                ,0,TTP.NAME,'Всего') AS LAST_TYPE --Классификация по теме
  , MAX(ID_TYPE) AS ID_TYPE
  , SUM(NVL(ST.ST_NEW,0)) AS ST_NEW --Новое
  , SUM(0) AS IN_WORK --В работе (Такие записи не добавляются)
  , SUM(NVL(ST.ST_TRANSFERRED_3RD_LINE,0)) AS ST_TRANSFERRED_3RD_LINE  --Переведен на специалиста 3й линии
  , SUM(NVL(ST.ST_WAITING_REQUESTER_ANSWER,0)) AS ST_WAITING_REQUESTER_ANSWER --Ожидается ответ заявителя
  , SUM(NVL(ST.ST_NEED_CALL_TO_REQUESTER,0)) AS ST_NEED_CALL_TO_REQUESTER --Требуется исходящий звонок заявителю
  , SUM(NVL(ST.ST_PRE_RESOLVED,0)) AS ST_PRE_RESOLVED --Предварительное решение
  , SUM(NVL(ST.ST_RESOLVED,0)) AS ST_RESOLVED --Решено
  , SUM(NVL(ST.ST_CLOSED_ON_REQUEST,0)) AS ST_CLOSED_ON_REQUEST --Закрыто по запросу
  , SUM(NVL(ST.ST_CLOSED,0)) AS ST_CLOSED --Закрыто
  , SUM(NVL(ST.ST_SOLVED_BY_FIAS,0)) AS ST_SOLVED_BY_FIAS --Решено ФИАС

  , SUM( NVL(ST.ST_NEW,0) +
         --В РАБОТЕ +
         NVL(ST.ST_TRANSFERRED_3RD_LINE,0) +
         NVL(ST.ST_WAITING_REQUESTER_ANSWER,0) +
         NVL(ST.ST_NEED_CALL_TO_REQUESTER,0) +
         NVL(ST.ST_PRE_RESOLVED,0) +
         NVL(ST.ST_RESOLVED,0) +
         NVL(ST.ST_CLOSED_ON_REQUEST,0) +
         NVL(ST.ST_CLOSED,0) +
         NVL(ST.ST_SOLVED_BY_FIAS,0)

       ) AS ITOGO --Итого

  FROM SUM_TICKETS ST
  RIGHT JOIN (--ВСЕ ЭТИ МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
               SELECT DECODE(ID_TYPE,
                                    13,9.5,
                                    12,1000,
                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES where ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП

             ) TTP ON TTP.NAME = ST.LAST_TYPE
  GROUP BY ROLLUP(TTP.NAME)
  ORDER BY GROUPING(TTP.NAME),ID_TYPE --СОРТИРОВКА КАК В ТЗ
  )
  SELECT
    LAST_TYPE --Классификация по теме
  , ST_NEW --Новое
  , IN_WORK --в РАБОТЕ
  , ST_TRANSFERRED_3RD_LINE  --Переведен на специалиста 3й линии
  , ST_WAITING_REQUESTER_ANSWER --Ожидается ответ заявителя
  , ST_NEED_CALL_TO_REQUESTER --Требуется исходящий звонок заявителю
  , ST_PRE_RESOLVED --Предварительное решение
  , ST_RESOLVED --Решено
 -- , ST_CLOSED_ON_REQUEST --Закрыто по запросу
  , (ST_CLOSED_ON_REQUEST + ST_CLOSED) AS ST_CLOSED  --Закрыто --НАДО СУММИРОВАТЬ
  , ST_SOLVED_BY_FIAS --Решено ФИАС
  , ITOGO --Итого
  FROM
  SUM_TICKETS_2
  ;

  TYPE t_tickets_statuses IS TABLE OF cur_tickets_statuses%rowtype;

  FUNCTION fnc_tickets_statuses
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  ) RETURN t_tickets_statuses pipelined;

END PKG_REPORTS_OLD_TYPE;
/


CREATE OR REPLACE PACKAGE BODY PKG_REPORTS_OLD_TYPE AS

   -------------------------------------------------------------------
  --   Пакет с отчетами по старой классификации до изменений по заявке ZHKKH-916, ZHKKH-917
  -------------------------------------------------------------------
  --
  -------------------------------------------------------------------------
  --   ОТЧЕТ №4 "Статистика по результатам опроса в разрезе по тематикам"  --
  -------------------------------------------------------------------------
FUNCTION fnc_get_inq_ivr_cl_types
(
  i_init_time TIMESTAMP, 
  i_finish_time TIMESTAMP,
  I_COMPANY_REGION NUMBER
)RETURN t_inq_ivr_cl_types pipelined AS
PRAGMA AUTONOMOUS_TRANSACTION;  
BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

  IF(cur_get_inq_ivr_cl_types%isopen) THEN CLOSE cur_get_inq_ivr_cl_types;
  END IF;
  
  FOR l IN cur_get_inq_ivr_cl_types(i_init_time, i_finish_time, I_COMPANY_REGION)
    loop
      pipe ROW (l);
    END loop;
END fnc_get_inq_ivr_cl_types;




  -------------------------------------------------------------------------------
  --                       ОТЧЕТ ПО НАГРУЗКЕ                                   --
  -------------------------------------------------------------------------------

  FUNCTION fnc_report_on_crm 
  (
          I_INIT_TIME TIMESTAMP,
          I_FINISH_TIME TIMESTAMP,
          I_CHECK_REPORT NUMBER := 0
    
    ) RETURN t_report_on_crm pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_report_on_crm(I_INIT_TIME, I_FINISH_TIME, I_CHECK_REPORT)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_report_on_crm;  


  --------------------------------------------------------------
  --               ОТЧЕТ Сроки обработки обращений            --
  --------------------------------------------------------------

    FUNCTION fnc_TICKETS_proc_time
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_TICKETS_proc_time pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_TICKETS_proc_time(I_INIT_TIME, I_FINISH_TIME, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_TICKETS_proc_time;  


--------------------------------------------------------------
--     СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ MAILREADER    --
--------------------------------------------------------------

    FUNCTION fnc_tickets_statistic
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_tickets_statistic(I_INIT_TIME, I_FINISH_TIME,I_CLIENT_TYPE,I_COMPANY_TYPE, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statistic;


---------------------------------------------------------------
-- СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ В РАЗРЕЗЕ РЕГИОНОВ --
---------------------------------------------------------------

    FUNCTION fnc_tickets_statistic_regions
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_DST_ID VARCHAR DEFAULT NULL
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic_regions pipelined AS
  BEGIN

  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
   FOR L IN cur_tickets_statistic_regions(I_INIT_TIME, I_FINISH_TIME,I_CHANNEL,I_DST_ID,I_CLIENT_TYPE,I_COMPANY_TYPE,I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statistic_regions;
  
  
  
---------------------------------------------------------------
-- СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ ПО ПОЛНОМОЧИЯМ (TICKETS_D_COMPANY_TYPES) --
---------------------------------------------------------------

    FUNCTION fnc_tickets_statistic_COMPANY
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_REGION NUMBER
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic_COMPANY pipelined AS
  BEGIN

  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
   FOR L IN cur_tickets_statistic_COMPANY(I_INIT_TIME, I_FINISH_TIME,I_COMPANY_REGION, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statistic_COMPANY;  
 
 
 --------------------------------------------------------------
 --           ОТЧЕТ ПО СТАТУСАМ ОБРАЩЕНИЙ                    --
 --------------------------------------------------------------

    FUNCTION fnc_tickets_statuses
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

) RETURN t_tickets_statuses pipelined AS
  BEGIN
    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_tickets_statuses(I_INIT_TIME, I_FINISH_TIME)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statuses; 


END PKG_REPORTS_OLD_TYPE;
/
