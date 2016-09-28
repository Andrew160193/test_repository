CREATE OR REPLACE PACKAGE PKG_REPORTS AS 


  --------------------------------------------------------------------------------
  --          ДЕТАЛИЗИРОВАННЫЙ ОТЧЕТ ПО ВХОДЯЩИМ ЗВОНКАМ                        --
  --------------------------------------------------------------------------------
  
   CURSOR cur_rep_inc_call (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
  )
IS
WITH
    GIS_ZHKH AS (SELECT * FROM DUAL),
    SCRIPT_DATA AS
      (
        SELECT CCL.SESSION_ID, --ID сессии
          CCL.CALLER, --Номер звонящего
          CCL.OPERATOR_LOGIN, --Логин принявшего вызов
          CCL.ID_CALL, --ID звонка
          DECODE(ICCD.REFUSED_TO_COMPANY_REGION,1,'Отказался называть',TDRG.NAME) AS REGION_NAME, --Регион
          TDRG.REGION_TIMEZONE AS REGION_TIMEZONE,
				  DECODE(ICCD.REFUSED_TO_CONTACT_NAME,1,'Отказался называть',ICCD.CONTACT_NAME) AS CONTACT_NAME, --ФИО абонента
          TDCTP.NAME AS ELIGIBLE_ORGANIZATION, --Правомочие организации
          CCLR.NAME AS CALL_STATUS, --Статус звонка
          TDTP.NAME AS TYPE_NAME, --Классификатор
          --'' AS TYPE_NAME_LEVEL_2, --Классификатор 
          --'' AS ADMIN_TYPE, --Административный тип
          NVL2(CCLTR.FID_RESULT,CCLTRR.NAME,'Не переводился') AS TRANSFER_CALL, --Совершение перевода звонка
          NVL2(ICCD.FID_MESSAGE_MAIL,'Оформлен','Не оформлялся') AS CREATE_MESSAGE, --Оформление заказа
          NVL2(MM.FID_TICKET,'Оформлено','Не оформлялось') AS CREATE_TICKETS, --Оформление обращения
          CCL.COMMENTS AS OPERATOR_COMMENTS, --Комментарий оператора
          (CASE
            WHEN ICCD.REFUSED_TO_COMPANY_OGRN = 0
            THEN ICCD.COMPANY_OGRN
           END) AS COMPANY_OGRN, --ОГРН
          (CASE
            WHEN ICCD.REFUSED_TO_COMPANY_OGRN = 1
            THEN ICCD.OGRN_REFUSE_REASON
           END) AS OGRN_REFUSE_REASON, --Отказ ОГРН
          CCL.COMMENTS AS OPR_COMMENT, --Комменатрий оператора
--          (CASE WHEN IVR.SP = 2 THEN 'Физ. лицо'
--           WHEN IVR.SP = 3 THEN 'Ничего не выбрано'
--           WHEN IVR.SP = 4 THEN 'Юр. лицо, ОГРН не распознан'
--           WHEN IVR.SP = 5 THEN 'Юр. лицо, ОГРН распознан'
--           ELSE NULL
--          END) AS IVR_CHOICE, --IVR_CHOICE
          IVR.OGRN AS IVR_OGRN, --IVR_OGRN
          ROW_NUMBER()OVER(PARTITION BY SESSION_ID ORDER BY ID_CALL DESC)                  AS RN
        FROM CORE_CALLS CCL
        LEFT JOIN INC_CALL_CONTACT_DATA ICCD ON ICCD.FID_CALL = CCL.ID_CALL AND ICCD.IS_PRIMARY = 1
        LEFT JOIN INC_CALL_IVR_DATA IVR ON IVR.FID_CALL = CCL.ID_CALL
        LEFT JOIN TICKETS_D_REGIONS TDRG ON TDRG.ID_REGION=ICCD.FID_COMPANY_REGION
        LEFT JOIN TICKETS_D_COMPANY_TYPES TDCTP ON TDCTP.ID_COMPANY_TYPE=ICCD.FID_COMPANY_TYPE
        LEFT JOIN TICKETS_D_TYPES TDTP ON TDTP.ID_TYPE=ICCD.FID_TYPE/* AND TDTP.ID_TYPE BETWEEN 1 AND 13*/
        LEFT JOIN CORE_CALLS_RESULTS CCLR ON CCLR.ID_RESULT=CCL.FID_RESULT
        LEFT JOIN CORE_CALLS_TRANSFERS CCLTR ON CCLTR.FID_CALL=CCL.ID_CALL
        LEFT JOIN CORE_CALLS_TRANSFERS_RESULTS CCLTRR ON CCLTRR.ID_RESULT=CCLTR.FID_RESULT
        LEFT JOIN MAIL_MESSAGES MM ON MM.ID_MESSAGE=ICCD.FID_MESSAGE_MAIL
        WHERE CCL.CREATED_AT BETWEEN I_INIT_TIME AND I_FINISH_TIME
          AND LOWER(TRIM(CCL.DIRECTION)) = 'in'
      ),
      SCRIPT_DATA_2 AS
      (
        SELECT *
        FROM SCRIPT_DATA
        WHERE RN = 1
      ),
     DATA_INC_CALL AS --
     (
      SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_INIT_TIME, I_FINISH_TIME)) tab
      WHERE I_FINISH_TIME > TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_FINISH_TIME <= TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
        AND (tab.CALL_CREATED >= I_INIT_TIME AND tab.CALL_CREATED < I_FINISH_TIME)
     ),
  IVR AS (
      SELECT

        tab.session_id,
        cp.param_name,
        cp.param_value,
        cp.changed--,
--        (CASE
--          WHEN cp.param_name = 'CS1' THEN 1
--
--          WHEN cp.param_name = 'CS2' AND cp.param_value = 0 THEN 100
--          WHEN cp.param_name = 'OUT_CS2' AND cp.param_value = 0 THEN 110
--          WHEN cp.param_name = 'OUT_CS2' AND cp.param_value != 0 THEN 111
--
--          WHEN cp.param_name = 'CS3' AND cp.param_value = 0 THEN 200
--          WHEN cp.param_name = 'OUT_CS3' AND cp.param_value = 0 THEN 220
--          WHEN cp.param_name = 'OUT_CS3' AND cp.param_value != 0 THEN 222
--
--          WHEN cp.param_name = 'CS4' AND cp.param_value = 0 THEN 300
--          WHEN cp.param_name = 'OUT_CS4' AND cp.param_value = 0 THEN 330
--          WHEN cp.param_name = 'OUT_CS4' AND cp.param_value != 0 THEN 333
--
--          WHEN cp.param_name = 'CS4' AND cp.param_value = 0 THEN 300
--          WHEN cp.param_name = 'OUT_CS4' AND cp.param_value = 0 THEN 330
--          WHEN cp.param_name = 'OUT_CS4' AND cp.param_value != 0 THEN 333
--
--          WHEN cp.param_name = 'CS5' AND cp.param_value = 0 THEN 400
--          WHEN cp.param_name = 'OUT_CS5' AND cp.param_value = 0 THEN 440
--          WHEN cp.param_name = 'OUT_CS5' AND cp.param_value != 0 THEN 444
--
--
--          WHEN cp.param_name = 'CS6' AND cp.param_value = 0 THEN 500
--          WHEN cp.param_name = 'OUT_CS6' AND cp.param_value = 0 THEN 550
--          WHEN cp.param_name = 'OUT_CS6' AND cp.param_value != 0 THEN 555
--
--          ELSE 1
--         END) AS NUM_ACTION
         --, tab.COMPLET_CALL_TIME
      FROM DATA_INC_CALL tab --ЧТОБЫ СХОДИЛОСЬ С ДРУГИМИ ОТЧЕТАМИ
      JOIN naucrm.call_params cp --
       ON cp.session_id = tab.session_id AND
          cp.param_name IN ('CS1', 'CS2', 'CS3', 'CS4', 'CS5', 'CS6', 'OUT_CS2', 'OUT_CS3', 'OUT_CS4', 'OUT_CS5', 'OUT_CS6') AND
          tab.COMPLET_CALL_TIME IS NOT NULL  /*AND
                cp.changed >= tab.COMPLET_CALL_TIME */
          WHERE tab.RN = 1
      ),
  IVR_ITOG AS (
      select
       SESSION_ID,
      -- MIN(CHANGED) AS MIN_CHANGED,
      -- MAX(CHANGED) AS MAX_CHANGED,
       NAUCRM.intervaltosec ( MAX(CHANGED) - MIN(CHANGED) ) AS DIR_CHANGED--,
      -- MAX(NUM_ACTION) AS END_NUM_ACTION

      from ivr
      GROUP BY SESSION_ID
      )
 
 ,  ALL_CHOOSED_ANSWERS AS(
      SELECT
         SESSION_ID
       , rtrim ( xmlcast ( xmlagg ( xmlelement ( "a", SUBSTANCE_NAME ||'; ' ) ORDER BY ID_CALLS_WIKI_ANSWERS ASC ) AS VARCHAR2(4000 CHAR) ), '; ' ) AS CHOOSED_ANSWERS
       --, LISTAGG(MSG.ID_MESSAGE, ', ') WITHIN GROUP ( ORDER BY MSG.ID_MESSAGE) AS FID_MESSAGES
       FROM (
            SELECT 
              SESSION_ID
            , SUBSTANCE_NAME
            , ID_CALLS_WIKI_ANSWERS
            , ROW_NUMBER()OVER(PARTITION BY SESSION_ID ORDER BY ID_CALLS_WIKI_ANSWERS DESC)   AS RN 
            FROM (
             
                  SELECT DISTINCT            
                    CL.SESSION_ID
                  , CWA.ID_CALLS_WIKI_ANSWERS 
                  , WS.SUBSTANCE_NAME  
                  FROM CALLS_WIKI_ANSWERS CWA
                  JOIN CORE_CALLS CL
                   ON CL.ID_CALL = CWA.FID_CALL
                  JOIN WIKI_ANSWER WA
                   ON WA.ID_ANSWER = CWA.FID_ANSWER
                  JOIN WIKI_D_SUBSTANCE WS
                   ON WS.ID_SUBSTANCE = WA.FID_SUBSTANCE
                  WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME 
            
                )   
        )
        WHERE RN<17
        GROUP BY SESSION_ID
   
   )
   , ALL_CALLS_TYPES AS ( --Выбранные типы при ответе на вопросы--ZHKKH-917
         SELECT 
           CL.SESSION_ID
         , LISTAGG(TDT_LEV_1.NAME, '; ') WITHIN GROUP (ORDER BY QST.ID_QUESTION ASC) AS TYPE_NAME 
         , LISTAGG(TDT.NAME, '; ') WITHIN GROUP (ORDER BY QST.ID_QUESTION ASC) AS TYPE_NAME_LEVEL_2 --Классификатор 2 
         , LISTAGG(ADT.NAME, '; ') WITHIN GROUP (ORDER BY QST.ID_QUESTION ASC) AS ADMIN_TYPE --Административный тип
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
   
  , ivr_sp AS (
        SELECT
                tab.session_id,
--                cp.param_name,
--                cp.param_value,
--                cp.changed,
                MAX(CASE 
                        WHEN cp.param_name = 'SP' AND cp.param_value='1' THEN 'Юридическое лицо'
                        WHEN cp.param_name = 'SP' AND cp.param_value='2' THEN 'Физическое лицо'
                END) AS ivr_choice --IVR_CHOICE
        FROM data_inc_call tab --ЧТОБЫ СХОДИЛОСЬ С ДРУГИМИ ОТЧЕТАМИ
                LEFT JOIN naucrm.call_params cp
                    ON cp.session_id = tab.session_id 
                    AND cp.param_name = 'SP'
--                    AND tab.complet_call_time IS NOT NULL  
--                    AND cp.changed >= tab.COMPLET_CALL_TIME 
        WHERE tab.rn = 1
        GROUP BY tab.session_id
)
   

    SELECT CI.SESSION_ID,
    (CASE
      WHEN pw.id IS NOT NULL
      THEN 'Да'
      ELSE 'Нет'
     END) AS WORK_TIME,
      (CASE
        WHEN CI.DST_ID IN ('4957392507','5555319863','5555319862')
        THEN '88003020305'
        WHEN CI.DST_ID IN ('4957392209','5555392209','5555392210')
        THEN '84957718181'
        ELSE 'Не определено'
      END) AS DST_ID,
      --CI.DST_ID, --источник (номер, на который позвонил абонент)
      CI.CALLER,
      TO_CHAR(CI.CALL_CREATED,'dd.mm.yyyy hh24:mi:ss') AS CALL_CREATED,
      IVR_SP.IVR_CHOICE,
      SD.IVR_OGRN,
      CI.CONNECT_RESULT,
      CI.WELCOME_DUR,
      
     CASE
        WHEN CONNECT_RESULT_NUM != 3
        THEN CI.BUSY_DUR
      END AS BUSY_DUR,
      CASE
        WHEN CONNECT_RESULT_NUM != 3
        THEN CI.CALL_RESULT
      END AS CALL_RESULT,
      TO_CHAR(CI.OPR_CREATED,'dd.mm.yyyy hh24:mi:ss') AS OPR_CREATED,
      CI.RINGING_DUR - NVL(CI.RINGING_DUR_SECOND,0) AS RINGING_DUR,
      CI.TALK_DUR - NVL(CI.TALK_DUR_SECOND,0) AS TALK_DUR,
      CI.HOLD_DUR - NVL(CI.HOLD_DUR_SECOND,0) AS HOLD_DUR,
      CI.WRAPUP_DUR - NVL(CI.WRAPUP_DUR_SECOND,0) AS WRAPUP_DUR,
      CI.SERVISE_CALL_DUR - NVL(CI.SERVISE_CALL_DUR_SECOND,0) AS SERVISE_CALL_DUR,
      CEIL((CI.SERVISE_CALL_DUR - NVL(CI.SERVISE_CALL_DUR_SECOND,0))/60) AS SERVISE_CALL_DUR2,
      CI.OPR_LOGIN,
      
      CI.BUSY_DUR_SECOND,
      CASE
        WHEN CONNECT_RESULT_NUM_SECOND != 3
        THEN CI.CALL_RESULT_SECOND
      END AS CALL_RESULT_SECOND,
      TO_CHAR(CI.OPR_CREATED_SECOND,'dd.mm.yyyy hh24:mi:ss') AS OPR_CREATED_SECOND,
      CI.RINGING_DUR_SECOND,
      CI.TALK_DUR_SECOND,
      CI.HOLD_DUR_SECOND,
      CI.WRAPUP_DUR_SECOND,
      CI.SERVISE_CALL_DUR_SECOND,
      CEIL(CI.SERVISE_CALL_DUR_SECOND/60) AS SERVISE_CALL_DUR2_SECOND,
      CI.OPR_LOGIN_SECOND,
      
      TO_CHAR(CI.COMPLET_CALL_TIME,'dd.mm.yyyy hh24:mi:ss') AS COMPLET_CALL_TIME,
      CI.SEP_INIT,
      SD.ID_CALL,
      CASE WHEN SD.REGION_NAME IS not NULL THEN SD.REGION_NAME
           WHEN instr(m.area, '|') = 0 THEN m.area
           WHEN instr(m.area, '|', -1) > 0 THEN trim(substr(m.area, -1 * (LENGTH(m.area) - instr(m.area, '|', -1))))
      END AS REGION_NAME,
      CASE
      WHEN trim(m.area) like '%Москва%' OR trim(substr(m.area, -1 * (LENGTH(m.area) - instr(m.area, '|', -1)))) like '%Москва%'
      THEN 0
      ELSE COALESCE(SD.REGION_TIMEZONE,TDR.REGION_TIMEZONE)
      END AS REGION_TIMEZONE,
      SD.CONTACT_NAME,
      SD.ELIGIBLE_ORGANIZATION, --Правомочие организации
      SD.CALL_STATUS,
--      SD.TYPE_NAME,--
      NVL(ACT.TYPE_NAME,SD.TYPE_NAME) AS TYPE_NAME,--ZHKKH-917
--      '' AS TYPE_NAME_LEVEL_2,--
      ACT.TYPE_NAME_LEVEL_2, --Классификатор 2--ZHKKH-917
--      '' AS ADMIN_TYPE, --
      ACT.ADMIN_TYPE, --Административный тип--ZHKKH-917
      SD.OPR_COMMENT,
      SD.COMPANY_OGRN,
      SD.OGRN_REFUSE_REASON, --Отказ ОГРН
      (CASE
        WHEN II.DIR_CHANGED = 0 OR II.DIR_CHANGED IS NULL
        THEN 'Не переведен'
        ELSE 'Переведен'
       END) AS ACSI_STATUS,
       (CASE
        WHEN II.DIR_CHANGED = 0 OR II.DIR_CHANGED IS NULL
        THEN 0
        ELSE II.DIR_CHANGED
       END) AS ACSI_TIME,
       ACA.CHOOSED_ANSWERS,
        ROW_NUMBER()OVER(PARTITION BY CI.SESSION_ID ORDER BY (CASE
                                                                WHEN trim(m.area) like '%Москва%' OR trim(substr(m.area, -1 * (LENGTH(m.area) - instr(m.area, '|', -1)))) like '%Москва%'
                                                                THEN 0
                                                                ELSE COALESCE(SD.REGION_TIMEZONE,TDR.REGION_TIMEZONE)
                                                              END)
                                                                 ASC)   AS RN
    FROM DATA_INC_CALL CI
    LEFT JOIN SCRIPT_DATA_2 SD ON SD.SESSION_ID=CI.SESSION_ID
    LEFT JOIN IVR_ITOG II ON II.SESSION_ID=CI.SESSION_ID
    LEFT JOIN ALL_CHOOSED_ANSWERS ACA ON ACA.SESSION_ID=CI.SESSION_ID
    LEFT JOIN ALL_CALLS_TYPES ACT ON ACT.SESSION_ID=CI.SESSION_ID --Выбранные типы при ответе на вопросы--ZHKKH-917

    JOIN common.d_project_phones ph ON ph.phone = CI.DST_ID
                                              AND CI.CALL_CREATED BETWEEN ph.begin_time AND ph.end_time
    LEFT JOIN common.d_project_work_time pw ON pw.fid_project_phones_id = ph.ID
           AND CI.CALL_CREATED BETWEEN pw.init_time AND pw.final_time
           AND ((mod(to_number(to_char(CI.CALL_CREATED,'J')),7)+1 IN (1,2,3,4,5) AND (CI.CALL_CREATED BETWEEN trunc(CI.CALL_CREATED) + pw.begin_operating_time_weekdays AND trunc(CI.CALL_CREATED) + pw.end_operating_time_weekdays))
               OR (mod(to_number(to_char(CI.CALL_CREATED,'J')),7)+1 IN (6,7) AND CI.CALL_CREATED BETWEEN trunc(CI.CALL_CREATED) + pw.begin_operating_time_holidays AND trunc(CI.CALL_CREATED) + pw.end_operating_time_holidays))
    LEFT JOIN common.d_phonecodes_mr m ON (floor(REGEXP_REPLACE(CI.CALLER,'\D','') / 10000000)) = (floor(m.rangeend / 10000000)) AND
                                                REGEXP_REPLACE(CI.CALLER,'\D','') BETWEEN m.rangestart AND m.rangeend
    LEFT JOIN TICKETS_D_REGIONS TDR ON LOWER(m.area) LIKE '%'||LOWER(TDR.NAME)||'%'
    LEFT JOIN ivr_sp ON ivr_sp.session_id=ci.session_id
    WHERE CI.SESSION_ID IS NOT NULL
      AND CI.RN = 1 --DELETE DUBLI
    ORDER BY CI.CALL_CREATED;
    
    
   TYPE t_rep_inc_call IS TABLE OF cur_rep_inc_call%rowtype;

  FUNCTION fnc_rep_inc_call
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  ) RETURN t_rep_inc_call pipelined;   
  
 
 
  -------------------------------------------------------------------------------
  --                    ОБЩИЙ ОТЧЕТ ПО ЗВОНКАМ                                 --
  ------------------------------------------------------------------------------- 
  
   CURSOR cur_rep_general_calls (
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_GROUP VARCHAR2 DEFAULT NULL
    , I_STEP NUMBER DEFAULT 1
  )
  IS
          WITH
     GIS_ZHKH AS (SELECT * FROM DUAL),
     PERIODS AS
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

      
--        SELECT
--          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
--          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
--          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
--          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
--        FROM TABLE(
--              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
--                NVL2(
--                      LOWER(:I_GROUP),
--                      CAST(TRUNC(I_INIT_TIME) AS TIMESTAMP),
--                      I_INIT_TIME
--                    ),
--                I_FINISH_TIME, NVL(LOWER(:I_GROUP), 'year'),
--                DECODE(:I_GROUP,'minute',15,1)
--                ))
      ),
     DATA_INC_CALL AS --
     (
      SELECT tab.*,
             ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
      FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_INIT_TIME, I_FINISH_TIME)) tab
      WHERE (I_FINISH_TIME > TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
      UNION ALL
      SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
      WHERE (I_FINISH_TIME <= TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
        AND (tab.CALL_CREATED >= I_INIT_TIME AND tab.CALL_CREATED < I_FINISH_TIME)
     ),
     DATA_INC_CALL_2 AS --DELETE DUBLI
      (
        SELECT
          P.START_PERIOD
        , P.VIEW_PERIOD
        , TAB.SESSION_ID
        , TAB.CALLER
        , TAB.DST_ID
        , TAB.CALL_CREATED
        , TAB.CONNECT_RESULT
        , TAB.CONNECT_RESULT_NUM
        , TAB.CONNECT_RESULT_NUM_SECOND
        , TAB.WELCOME_DUR
        , TAB.BUSY_DUR
        , TAB.BUSY_DUR_SECOND
        , TAB.CALL_RESULT
        , TAB.CALL_RESULT_NUM
        , TAB.CALL_RESULT_NUM_SECOND
        , TAB.OPR_CREATED
        , TAB.RINGING_DUR
        , TAB.TALK_DUR
        , TAB.TALK_DUR_SECOND
        , TAB.HOLD_DUR
        , TAB.HOLD_DUR_SECOND
        , TAB.WRAPUP_DUR
        , TAB.WRAPUP_DUR_SECOND
        , TAB.SERVISE_CALL_DUR
        , TAB.SERVISE_CALL_DUR_SECOND
        , TAB.SERVISE_CALL_DUR2
        , TAB.SERVISE_CALL_DUR2_SECOND
        , TAB.OPR_LOGIN
        , TAB.COMPLET_CALL_TIME
        , TAB.SEP_INIT
        , TAB.CALL_IN_WORK_TIME
        , TAB.SECOND_LINE        
        FROM DATA_INC_CALL TAB
        LEFT JOIN PERIODS P        
         ON TAB.CALL_CREATED BETWEEN P.START_PERIOD AND P.STOP_PERIOD
        WHERE RN = 1
      ),
        REPORT_PRP AS
      (
        SELECT
         START_PERIOD,
         VIEW_PERIOD, --Период
          COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM != 3
                  THEN SESSION_ID
                END) AS CALL_COUNT, --Объем
          COUNT(DISTINCT
                CASE
                  WHEN DST_ID IN ('4957392507','5555319863','5555319862')
                    AND CONNECT_RESULT_NUM != 3
                  THEN SESSION_ID
                    END) AS CALL_COUNT_LINE_8800,
          COUNT(DISTINCT
                CASE
                  WHEN DST_ID IN ('4957392209','5555392209','5555392210')
                    AND CONNECT_RESULT_NUM != 3
                  THEN SESSION_ID
                    END) AS CALL_COUNT_LINE_8495,
          COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM = 3
                  THEN SESSION_ID
                END) AS CALL_IN_NOT_WT, --Завершенные в IVR в нерабочее время
     
          COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM = 2
                  THEN SESSION_ID
                END) AS TO_OPR_CALL, --Направлено на операторов (1-я линия)
                    COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2
                  THEN SESSION_ID
                END) AS TO_OPR_CALL_SECOND, --Направлено на операторов (2-я линия)
                
          COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM = 4
                  THEN SESSION_ID
                END) AS LOST_IVR, --Завершенные в IVR
                
          COUNT(DISTINCT
                CASE
                  WHEN CONNECT_RESULT_NUM = 4 AND DST_ID NOT IN ('4957392209','5555392209','5555392210')
                  THEN SESSION_ID
                END) AS LOST_IVR_NOTIN_8495, --Завершенные в IVR      
                
          COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM = 1
                    AND CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS ANS_CALL, --Отвеченные операторами (1-я линия)
         COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                    AND CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS ANS_CALL_SECOND, --Отвеченные операторами (2-я линия)
                
          COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM IN (2, 3)
                    AND CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS LOST_QUEUE, --Потерянные в очереди (1-я линия)
          COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM IN (2, 3) AND CALL_RESULT_NUM_SECOND IN (2, 3)
                    AND CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS LOST_QUEUE_SECOND, --Потерянные в очереди (2-я линия)                
                
          COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM IN (2, 3)
                    AND CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                    AND BUSY_DUR <= 5
                  THEN SESSION_ID
                END) AS LOST_QUEUE_5, --Потерянные в очереди до 5 секунд (1-я линия)
          COUNT(DISTINCT
                CASE
                  WHEN CALL_RESULT_NUM IN (2, 3) AND CALL_RESULT_NUM_SECOND IN (2, 3)
                    AND CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                    AND BUSY_DUR_SECOND <= 5
                  THEN SESSION_ID
                END) AS LOST_QUEUE_5_SECOND, --Потерянные в очереди до 5 секунд (2-я линия)                
                
          COUNT(DISTINCT
                CASE
                  WHEN BUSY_DUR <= 30
                    AND CALL_RESULT_NUM = 1
                    AND CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS ANS_CALL_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
          COUNT(DISTINCT
                CASE
                  WHEN BUSY_DUR_SECOND <= 30
                    AND CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                    AND CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN SESSION_ID
                END) AS ANS_CALL_30_SECOND, --Отвеченные до 30 секунд ожидания в очереди (2-я линия)
                
          SUM(CASE
                WHEN CALL_RESULT_NUM = 1
                THEN TALK_DUR+HOLD_DUR
              END) - 
                     SUM(CASE
                          WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                          THEN (TALK_DUR_SECOND+HOLD_DUR_SECOND)
                          ELSE 0
                        END) AS SPEEK_TIME, --Суммарное время разговора (1-я линия)
                                                
         SUM(CASE
                WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                THEN (TALK_DUR_SECOND+HOLD_DUR_SECOND)
                ELSE 0
              END) AS SPEEK_TIME_SECOND, --Суммарное время разговора (2-я линия)              
              
          SUM(CASE
                WHEN CONNECT_RESULT_NUM = 2
                THEN BUSY_DUR
              END) - 
               SUM(CASE
                WHEN CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2
                THEN BUSY_DUR_SECOND
                ELSE 0
                   END) AS BUSY_DUR, --Суммарное время нахождения в очереди (1-я линия)
          SUM(CASE
                WHEN CONNECT_RESULT_NUM = 2 AND CONNECT_RESULT_NUM_SECOND = 2
                THEN BUSY_DUR_SECOND
                ELSE 0
              END) AS BUSY_DUR_SECOND, --Суммарное время нахождения в очереди (2-я линия)              
              
              
          SUM(CASE
                WHEN CALL_RESULT_NUM = 1
                THEN WRAPUP_DUR
              END) - 
              SUM(CASE
                WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                THEN WRAPUP_DUR_SECOND
                ELSE 0
              END)AS SUM_WRAPUP, --Суммарное время поствызывной обработки (1-я линия)
          SUM(CASE
                WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                THEN WRAPUP_DUR_SECOND
                ELSE 0
              END) AS SUM_WRAPUP_SECOND, --Суммарное время поствызывной обработки (2-я линия)              
              
          SUM(CASE
                WHEN CALL_RESULT_NUM = 1
                THEN SERVISE_CALL_DUR
              END) - 
              SUM(CASE
                WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                THEN SERVISE_CALL_DUR_SECOND
                ELSE 0
                 END) AS WORK_TIME, -- Суммарное время обработки вызова (1-я линия)
          SUM(CASE
                WHEN CALL_RESULT_NUM = 1 AND CALL_RESULT_NUM_SECOND = 1
                THEN SERVISE_CALL_DUR_SECOND
                ELSE 0
              END) AS WORK_TIME_SECOND -- Суммарное время обработки вызова (1-я линия)              
        FROM DATA_INC_CALL_2
        WHERE --CALLER NOT IN ('4957392201','957392201')
        --По заявке ZHKKH-490:
        --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
        --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
          (
          (CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and CALLER NOT IN ('4957392201','957392201'))
       OR ((CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
            CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(CALLER, -10) NOT IN ('4957392201'))
       OR (CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
          )

        GROUP BY VIEW_PERIOD, START_PERIOD
      )
  , REPORT_PRP_ITOGO AS --СУММЫ КОЛЛИЧЕСТВЕННЫХ ПОКАЗАТЕЛЕЙ
     (SELECT
      SUM(CALL_COUNT) AS CALL_COUNT, --Объем
      SUM(CALL_COUNT_LINE_8800) AS CALL_COUNT_LINE_8800, --Объем ПО 8800
      SUM(CALL_COUNT_LINE_8495) AS CALL_COUNT_LINE_8495, --Объем ПО 495
      SUM(CALL_IN_NOT_WT) AS CALL_IN_NOT_WT, --Завершенные в IVR в нерабочее время
      
      SUM(TO_OPR_CALL) AS TO_OPR_CALL, --Направлено на операторов (1-я линия)
      SUM(TO_OPR_CALL_SECOND) AS TO_OPR_CALL_SECOND, --Направлено на операторов (2-я линия)
      
      SUM(LOST_IVR) AS LOST_IVR, --Завершенные в IVR
      SUM(LOST_IVR_NOTIN_8495) AS LOST_IVR_NOTIN_8495,
      
      SUM(ANS_CALL) AS ANS_CALL, --Отвеченные операторами (1-я линия)
      SUM(ANS_CALL_SECOND) AS ANS_CALL_SECOND, --Отвеченные операторами (2-я линия)
      
      SUM(LOST_QUEUE) AS LOST_QUEUE, --Потерянные в очереди (1-я линия)
      SUM(LOST_QUEUE_SECOND) AS LOST_QUEUE_SECOND, --Потерянные в очереди (2-я линия)
      
      SUM(LOST_QUEUE_5) AS LOST_QUEUE_5, --Потерянные в очереди до 5 секунд (1-я линия)
      SUM(LOST_QUEUE_5_SECOND) AS LOST_QUEUE_5_SECOND, --Потерянные в очереди до 5 секунд (2-я линия)
      
      SUM(ANS_CALL_30) AS ANS_CALL_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
      SUM(ANS_CALL_30_SECOND) AS ANS_CALL_30_SECOND, --Отвеченные до 30 секунд ожидания в очереди (2-я линия)
      
      SUM(SPEEK_TIME) AS SPEEK_TIME, --Суммарное время разговора (1-я линия)
      SUM(SPEEK_TIME_SECOND) AS SPEEK_TIME_SECOND, --Суммарное время разговора (2-я линия)

      
      SUM(BUSY_DUR) AS BUSY_DUR, --Суммарное время нахождения в очереди (1-я линия)
      SUM(BUSY_DUR_SECOND) AS BUSY_DUR_SECOND, --Суммарное время нахождения в очереди (2-я линия)
      
      SUM(SUM_WRAPUP) AS SUM_WRAPUP, --Суммарное время поствызывной обработки (1-я линия)
      SUM(SUM_WRAPUP_SECOND) AS SUM_WRAPUP_SECOND, --Суммарное время поствызывной обработки (2-я линия)
      
      SUM(WORK_TIME) AS WORK_TIME, -- Суммарное время обработки вызова (1-я линия)
      SUM(WORK_TIME_SECOND) AS WORK_TIME_SECOND -- Суммарное время обработки вызова (2-я линия)


     FROM
     REPORT_PRP

     )--"OLD" - ГОВОРИТ О ТОМ, ЧТО ЭТОТ ПОКАЗАТЕЛЬ СОВПАДАЕТ СО СТАРОЙ ВЕРСИЕЙ ОТЧЕТА
    SELECT 
      PR.START_PERIOD as START_PERIOD,
      to_char(PR.VIEW_PERIOD) AS VIEW_PERIOD, --Период
      NVL(CALL_COUNT,0) AS CALL_COUNT, --Объем
      NVL(CALL_COUNT_LINE_8800,0) AS CALL_COUNT_LINE_8800, --Объем ПО 8800
      NVL(CALL_COUNT_LINE_8495,0) AS CALL_COUNT_LINE_8495, --Объем ПО 495
      NVL(CALL_IN_NOT_WT,0) AS CALL_IN_NOT_WT, --Завершенные в IVR в нерабочее время
      
      NVL(LOST_IVR,0) AS LOST_IVR, --Завершенные в IVR
      NVL(LOST_IVR_NOTIN_8495,0) AS LOST_IVR_NOTIN_8495,
      REPLACE(TRIM(TO_CHAR(NVL(
                      LOST_IVR_NOTIN_8495/
                                 DECODE((CALL_COUNT-CALL_COUNT_LINE_8495),0,1,(CALL_COUNT-CALL_COUNT_LINE_8495))
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_IVR_NOTIN_8495, --Доля Завершенных В Ivr    
      
      
      NVL(TO_OPR_CALL,0)+NVL(TO_OPR_CALL_SECOND,0) AS TO_OPR_CALL_ALL, --Направлено на операторов ( линия)
      NVL(TO_OPR_CALL,0) AS TO_OPR_CALL, --Направлено на операторов (1-я линия)
      NVL(TO_OPR_CALL_SECOND,0) AS TO_OPR_CALL_SECOND, --Направлено на операторов (2-я линия)
      
      NVL(ANS_CALL,0) + NVL(ANS_CALL_SECOND,0) AS ANS_CALL_ALL, --Отвеченные операторами ( линия)
      NVL(ANS_CALL,0) AS ANS_CALL, --Отвеченные операторами (1-я линия)
      NVL(ANS_CALL_SECOND,0) AS ANS_CALL_SECOND, --Отвеченные операторами (2-я линия)
      
      NVL(LOST_QUEUE,0) + NVL(LOST_QUEUE_SECOND,0) AS LOST_QUEUE_ALL, --Потерянные в очереди ( линия)
      NVL(LOST_QUEUE,0) AS LOST_QUEUE, --Потерянные в очереди (1-я линия)
      NVL(LOST_QUEUE_SECOND,0) AS LOST_QUEUE_SECOND, --Потерянные в очереди (1-я линия)
      
      NVL(LOST_QUEUE_5,0) + NVL(LOST_QUEUE_5_SECOND,0) AS LOST_QUEUE_5_ALL, --Потерянные в очереди до 5 секунд ( линия)
      NVL(LOST_QUEUE_5,0) AS LOST_QUEUE_5, --Потерянные в очереди до 5 секунд (1-я линия)
      NVL(LOST_QUEUE_5_SECOND,0) AS LOST_QUEUE_5_SECOND, --Потерянные в очереди до 5 секунд (1-я линия)
      
      NVL(ANS_CALL_30,0) + NVL(ANS_CALL_30_SECOND,0) AS ANS_CALL_30_ALL, --Отвеченные до 30 секунд ожидания в очереди ( линия)
      NVL(ANS_CALL_30,0) AS ANS_CALL_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
      NVL(ANS_CALL_30_SECOND,0) AS ANS_CALL_30_SECOND, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
      
      REPLACE(TRIM(TO_CHAR(NVL(
                      ((LOST_QUEUE + LOST_QUEUE_SECOND)-(LOST_QUEUE_5 + LOST_QUEUE_5_SECOND))/
                                 DECODE((TO_OPR_CALL+TO_OPR_CALL_SECOND),0,1,(TO_OPR_CALL+TO_OPR_CALL_SECOND))
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL_ALL, --Процент пропущенных звонков ( линия)      
      REPLACE(TRIM(TO_CHAR(NVL(
                      (LOST_QUEUE-LOST_QUEUE_5)/
                                 DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL)
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL, --Процент пропущенных звонков (1-я линия)--OLD
      REPLACE(TRIM(TO_CHAR(NVL(
                      (LOST_QUEUE_SECOND-LOST_QUEUE_5_SECOND)/
                                 DECODE(TO_OPR_CALL_SECOND,0,1,TO_OPR_CALL_SECOND)
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL_SECOND, --Процент пропущенных звонков (2-я линия)                                            
                                            
      '5%' as PEC_LOST_CALL_PURPOSE, --Процент пропущенных звонков, целевой максимум
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when (ANS_CALL_30 + ANS_CALL_30_SECOND) = 0 and (TO_OPR_CALL + TO_OPR_CALL_SECOND)-(LOST_QUEUE_5+LOST_QUEUE_5_SECOND) = 0 then 1 else (ANS_CALL_30 + ANS_CALL_30_SECOND) end)/
                                      DECODE(((TO_OPR_CALL + TO_OPR_CALL_SECOND)-(LOST_QUEUE_5 + LOST_QUEUE_5_SECOND)),0,1,((TO_OPR_CALL+TO_OPR_CALL_SECOND)-(LOST_QUEUE_5+ LOST_QUEUE_5_SECOND)))*100
                                  ,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL_ALL, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (Обе линии)
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when ANS_CALL_30 = 0 and TO_OPR_CALL-LOST_QUEUE_5 = 0 then 1 else ANS_CALL_30 end)/DECODE((TO_OPR_CALL-LOST_QUEUE_5),0,1,(TO_OPR_CALL-LOST_QUEUE_5))*100,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (1-я линия)
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when ANS_CALL_30_SECOND = 0 and TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND = 0 then 1 else ANS_CALL_30_SECOND end)/DECODE((TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND),0,1,(TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND))*100,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL_SECOND, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (2-я линия)
      
      '90%' AS SL_PURPOSE, --Уровень сервиса цель
      
      ROUND(NVL((SPEEK_TIME + SPEEK_TIME_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_SPEEK_OLD, --Среднее время диалога --OLD
      ROUND(NVL((SPEEK_TIME + SPEEK_TIME_SECOND)/DECODE((ANS_CALL + ANS_CALL_SECOND),0,1,(ANS_CALL + ANS_CALL_SECOND)),0)) AS AVG_SPEEK_ALL, --Среднее время диалога  (1-я и 2-я линия)
      ROUND(NVL(SPEEK_TIME/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_SPEEK, --Среднее время диалога  (1-я линия)
      ROUND(NVL(SPEEK_TIME_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_SPEEK_SECOND, --Среднее время диалога  (2-я линия)
      
      ROUND(NVL((BUSY_DUR+BUSY_DUR_SECOND)/DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL),0)) AS AVG_QUEUE_OLD, --Среднее время ожидания в очереди OLD
      ROUND(NVL((BUSY_DUR+BUSY_DUR_SECOND)/DECODE((TO_OPR_CALL+TO_OPR_CALL_SECOND),0,1,(TO_OPR_CALL+TO_OPR_CALL_SECOND)),0)) AS AVG_QUEUE_ALL, --Среднее время ожидания в очереди  (1-я и 2-я линия)
      ROUND(NVL(BUSY_DUR/DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL),0)) AS AVG_QUEUE, --Среднее время ожидания в очереди  (1-я линия)
      ROUND(NVL(BUSY_DUR_SECOND/DECODE(TO_OPR_CALL_SECOND,0,1,TO_OPR_CALL_SECOND),0)) AS AVG_QUEUE_SECOND, --Среднее время ожидания в очереди  (2-я линия)
      
      ROUND(NVL((SUM_WRAPUP+SUM_WRAPUP_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WRAPUP_OLD, --Среднее время поствызывной обработки OLD
      ROUND(NVL((SUM_WRAPUP+SUM_WRAPUP_SECOND)/DECODE((ANS_CALL+ANS_CALL_SECOND),0,1,(ANS_CALL+ANS_CALL_SECOND)),0)) AS AVG_WRAPUP_ALL, --Среднее время поствызывной обработки  (1-я и 2-я  линия)
      ROUND(NVL(SUM_WRAPUP/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WRAPUP, --Среднее время поствызывной обработки  (1-я линия)
      ROUND(NVL(SUM_WRAPUP_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_WRAPUP_SECOND, --Среднее время поствызывной обработки  (1-я линия)
      
      ROUND(NVL((WORK_TIME+WORK_TIME_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WORK_OLD, --Среднее время обработки звонка  OLD
      ROUND(NVL((WORK_TIME+WORK_TIME_SECOND)/DECODE((ANS_CALL+ANS_CALL_SECOND),0,1,(ANS_CALL+ANS_CALL_SECOND)),0)) AS AVG_WORK_ALL, --Среднее время обработки звонка  (1-я и 2-я линия)
      ROUND(NVL(WORK_TIME/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WORK, --Среднее время обработки звонка  (1-я линия)
      ROUND(NVL(WORK_TIME_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_WORK_SECOND, --Среднее время обработки звонка  (2-я линия)
      
      '8 мин : 00 сек' AS AVG_WORK_PURPOSE --Среднее время обработки звонка (мин.) цель (1-я линия)
    FROM REPORT_PRP
    RIGHT JOIN PERIODS PR ON PR.VIEW_PERIOD = REPORT_PRP.VIEW_PERIOD

   UNION
    SELECT
    null as START_PERIOD,--нельзя писать null в union
    'Итого:' AS VIEW_PERIOD, --Период
      NVL(CALL_COUNT,0) AS CALL_COUNT, --Объем
      NVL(CALL_COUNT_LINE_8800,0) AS CALL_COUNT_LINE_8800, --Объем ПО 8800
      NVL(CALL_COUNT_LINE_8495,0) AS CALL_COUNT_LINE_8495, --Объем ПО 495
      NVL(CALL_IN_NOT_WT,0) AS CALL_IN_NOT_WT, --Завершенные в IVR в нерабочее время
      
      NVL(LOST_IVR,0) AS LOST_IVR, --Завершенные в IVR
      NVL(LOST_IVR_NOTIN_8495,0) AS LOST_IVR_NOTIN_8495,
      REPLACE(TRIM(TO_CHAR(NVL(
                LOST_IVR_NOTIN_8495/
                           DECODE((CALL_COUNT-CALL_COUNT_LINE_8495),0,1,(CALL_COUNT-CALL_COUNT_LINE_8495))
                                      ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_IVR_NOTIN_8495, --Доля Завершенных В Ivr  
      
      NVL(TO_OPR_CALL,0)+NVL(TO_OPR_CALL_SECOND,0) AS TO_OPR_CALL_ALL, --Направлено на операторов ( линия)
      NVL(TO_OPR_CALL,0) AS TO_OPR_CALL, --Направлено на операторов (1-я линия)
      NVL(TO_OPR_CALL_SECOND,0) AS TO_OPR_CALL_SECOND, --Направлено на операторов (2-я линия)
      
      NVL(ANS_CALL,0) + NVL(ANS_CALL_SECOND,0) AS ANS_CALL_ALL, --Отвеченные операторами ( линия)
      NVL(ANS_CALL,0) AS ANS_CALL, --Отвеченные операторами (1-я линия)
      NVL(ANS_CALL_SECOND,0) AS ANS_CALL_SECOND, --Отвеченные операторами (2-я линия)
      
      NVL(LOST_QUEUE,0) + NVL(LOST_QUEUE_SECOND,0) AS LOST_QUEUE_ALL, --Потерянные в очереди ( линия)
      NVL(LOST_QUEUE,0) AS LOST_QUEUE, --Потерянные в очереди (1-я линия)
      NVL(LOST_QUEUE_SECOND,0) AS LOST_QUEUE_SECOND, --Потерянные в очереди (1-я линия)
      
      NVL(LOST_QUEUE_5,0) + NVL(LOST_QUEUE_5_SECOND,0) AS LOST_QUEUE_5_ALL, --Потерянные в очереди до 5 секунд ( линия)
      NVL(LOST_QUEUE_5,0) AS LOST_QUEUE_5, --Потерянные в очереди до 5 секунд (1-я линия)
      NVL(LOST_QUEUE_5_SECOND,0) AS LOST_QUEUE_5_SECOND, --Потерянные в очереди до 5 секунд (1-я линия)
      
      NVL(ANS_CALL_30,0) + NVL(ANS_CALL_30_SECOND,0) AS ANS_CALL_30_ALL, --Отвеченные до 30 секунд ожидания в очереди ( линия)
      NVL(ANS_CALL_30,0) AS ANS_CALL_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
      NVL(ANS_CALL_30_SECOND,0) AS ANS_CALL_30_SECOND, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
      
      REPLACE(TRIM(TO_CHAR(NVL(
                      ((LOST_QUEUE + LOST_QUEUE_SECOND)-(LOST_QUEUE_5 + LOST_QUEUE_5_SECOND))/
                                 DECODE((TO_OPR_CALL+TO_OPR_CALL_SECOND),0,1,(TO_OPR_CALL+TO_OPR_CALL_SECOND))
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL_ALL, --Процент пропущенных звонков ( линия)      
      REPLACE(TRIM(TO_CHAR(NVL(
                      (LOST_QUEUE-LOST_QUEUE_5)/
                                 DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL)
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL, --Процент пропущенных звонков (1-я линия)--OLD
      REPLACE(TRIM(TO_CHAR(NVL(
                      (LOST_QUEUE_SECOND-LOST_QUEUE_5_SECOND)/
                                 DECODE(TO_OPR_CALL_SECOND,0,1,TO_OPR_CALL_SECOND)
                                            ,0)*100,'990D99')),'.',',')||'%' AS PEC_LOST_CALL_SECOND, --Процент пропущенных звонков (2-я линия)                                            
                                            
      '5%' as PEC_LOST_CALL_PURPOSE, --Процент пропущенных звонков, целевой максимум
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when (ANS_CALL_30 + ANS_CALL_30_SECOND) = 0 and (TO_OPR_CALL + TO_OPR_CALL_SECOND)-(LOST_QUEUE_5+LOST_QUEUE_5_SECOND) = 0 then 1 else (ANS_CALL_30 + ANS_CALL_30_SECOND) end)/
                                      DECODE(((TO_OPR_CALL + TO_OPR_CALL_SECOND)-(LOST_QUEUE_5 + LOST_QUEUE_5_SECOND)),0,1,((TO_OPR_CALL+TO_OPR_CALL_SECOND)-(LOST_QUEUE_5+ LOST_QUEUE_5_SECOND)))*100
                                  ,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL_ALL, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (Обе линии)
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when ANS_CALL_30 = 0 and TO_OPR_CALL-LOST_QUEUE_5 = 0 then 1 else ANS_CALL_30 end)/DECODE((TO_OPR_CALL-LOST_QUEUE_5),0,1,(TO_OPR_CALL-LOST_QUEUE_5))*100,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (1-я линия) --OLD
      
      CASE
        WHEN CALL_COUNT > 0
        THEN REPLACE(TRIM(TO_CHAR((case when ANS_CALL_30_SECOND = 0 and TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND = 0 then 1 else ANS_CALL_30_SECOND end)/DECODE((TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND),0,1,(TO_OPR_CALL_SECOND-LOST_QUEUE_5_SECOND))*100,'990D99')),'.',',')
        ELSE '100,00'
      END ||'%' AS SL_SECOND, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (2-я линия)
      
      '90%' AS SL_PURPOSE, --Уровень сервиса цель
      
      ROUND(NVL((SPEEK_TIME + SPEEK_TIME_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_SPEEK_OLD, --Среднее время диалога --OLD
      ROUND(NVL((SPEEK_TIME + SPEEK_TIME_SECOND)/DECODE((ANS_CALL + ANS_CALL_SECOND),0,1,(ANS_CALL + ANS_CALL_SECOND)),0)) AS AVG_SPEEK_ALL, --Среднее время диалога  (1-я и 2-я линия)
      ROUND(NVL(SPEEK_TIME/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_SPEEK, --Среднее время диалога  (1-я линия)
      ROUND(NVL(SPEEK_TIME_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_SPEEK_SECOND, --Среднее время диалога  (2-я линия)
      
      ROUND(NVL((BUSY_DUR+BUSY_DUR_SECOND)/DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL),0)) AS AVG_QUEUE_OLD, --Среднее время ожидания в очереди OLD
      ROUND(NVL((BUSY_DUR+BUSY_DUR_SECOND)/DECODE((TO_OPR_CALL+TO_OPR_CALL_SECOND),0,1,(TO_OPR_CALL+TO_OPR_CALL_SECOND)),0)) AS AVG_QUEUE_ALL, --Среднее время ожидания в очереди  (1-я и 2-я линия)
      ROUND(NVL(BUSY_DUR/DECODE(TO_OPR_CALL,0,1,TO_OPR_CALL),0)) AS AVG_QUEUE, --Среднее время ожидания в очереди  (1-я линия)
      ROUND(NVL(BUSY_DUR_SECOND/DECODE(TO_OPR_CALL_SECOND,0,1,TO_OPR_CALL_SECOND),0)) AS AVG_QUEUE_SECOND, --Среднее время ожидания в очереди  (2-я линия)
      
      ROUND(NVL((SUM_WRAPUP+SUM_WRAPUP_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WRAPUP_OLD, --Среднее время поствызывной обработки OLD
      ROUND(NVL((SUM_WRAPUP+SUM_WRAPUP_SECOND)/DECODE((ANS_CALL+ANS_CALL_SECOND),0,1,(ANS_CALL+ANS_CALL_SECOND)),0)) AS AVG_WRAPUP_ALL, --Среднее время поствызывной обработки  (1-я и 2-я  линия)
      ROUND(NVL(SUM_WRAPUP/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WRAPUP, --Среднее время поствызывной обработки  (1-я линия)
      ROUND(NVL(SUM_WRAPUP_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_WRAPUP_SECOND, --Среднее время поствызывной обработки  (1-я линия)
      
      ROUND(NVL((WORK_TIME+WORK_TIME_SECOND)/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WORK_OLD, --Среднее время обработки звонка  OLD
      ROUND(NVL((WORK_TIME+WORK_TIME_SECOND)/DECODE((ANS_CALL+ANS_CALL_SECOND),0,1,(ANS_CALL+ANS_CALL_SECOND)),0)) AS AVG_WORK_ALL, --Среднее время обработки звонка  (1-я и 2-я линия)
      ROUND(NVL(WORK_TIME/DECODE(ANS_CALL,0,1,ANS_CALL),0)) AS AVG_WORK, --Среднее время обработки звонка  (1-я линия)
      ROUND(NVL(WORK_TIME_SECOND/DECODE(ANS_CALL_SECOND,0,1,ANS_CALL_SECOND),0)) AS AVG_WORK_SECOND, --Среднее время обработки звонка  (2-я линия)
      
      '8 мин : 00 сек' AS AVG_WORK_PURPOSE --Среднее время обработки звонка (мин.) цель (1-я линия)
    FROM REPORT_PRP_ITOGO
    ORDER BY START_PERIOD ASC NULLS LAST
    ;
    

   TYPE t_rep_general_calls IS TABLE OF cur_rep_general_calls%rowtype;

  FUNCTION fnc_rep_general_calls
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL
      , I_STEP NUMBER DEFAULT 1

  ) RETURN t_rep_general_calls pipelined; 
  
  
  
  
  -------------------------------------------------------------------------------
  --                       ОТЧЕТ ПО НАГРУЗКЕ                                   --
  -------------------------------------------------------------------------------
  
   CURSOR cur_loading_report (
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_GROUP VARCHAR2 DEFAULT NULL
    , I_STEP NUMBER DEFAULT 1
  )
  IS  
   WITH gis_zhkh AS (SELECT * FROM dual)
, periods AS (
        SELECT
                CAST(greatest(period_start_time, CAST( I_INIT_TIME AS TIMESTAMP)) AS TIMESTAMP) AS start_period,
                CAST(period_finish_time AS TIMESTAMP) AS stop_period,
                to_char(greatest(period_start_time, CAST( I_INIT_TIME AS TIMESTAMP)),'dd.mm.yyyy hh24:mi') || ' - ' ||
                to_char(period_finish_time,'dd.mm.yyyy hh24:mi') AS view_period
        FROM TABLE(
                common_v2.pkg_datetime_utils.fnc_get_periods_of_time(
                CAST( I_INIT_TIME AS TIMESTAMP),
                CAST( I_FINISH_TIME AS TIMESTAMP),
                nvl(lower( I_GROUP), 'year'),
                decode( I_GROUP,'minute',15,1)
                )
        )
)
, data_inc_call AS (
        SELECT tab.*,
               row_number()OVER(PARTITION BY tab.session_id ORDER BY tab.call_created DESC)   AS rn
        FROM TABLE(PKG_GENERAL_REPORTS.fnc_data_inc_call(CAST( I_INIT_TIME AS TIMESTAMP), CAST( I_FINISH_TIME AS TIMESTAMP))) tab
        WHERE (CAST( I_FINISH_TIME AS TIMESTAMP) > trunc(SYSTIMESTAMP) AND CAST( I_INIT_TIME AS TIMESTAMP) <= CAST( I_FINISH_TIME AS TIMESTAMP))
        UNION ALL
        SELECT tab.*, 1 AS rn FROM TABLE_DATA_INC_CALL tab
        WHERE (CAST( I_FINISH_TIME AS TIMESTAMP) <= trunc(SYSTIMESTAMP) AND CAST( I_INIT_TIME AS TIMESTAMP) <= CAST( I_FINISH_TIME AS TIMESTAMP))
        AND (tab.call_created >= CAST( I_INIT_TIME AS TIMESTAMP) AND tab.call_created < CAST( I_FINISH_TIME AS TIMESTAMP))
)
------------------------------------
--ПОЛУЧЕНИЕ ИНФОРМАЦИИ ПО ЗВОНКАМ
------------------------------------
, data_inc_call_2 AS (--DELETE DUBLI
        SELECT
                tab.session_id
                , tab.caller
                , tab.dst_id
                , tab.call_created
                , tab.connect_result
                , tab.connect_result_num
                , tab.connect_result_num_second
                , tab.welcome_dur
                , tab.busy_dur
                , tab.busy_dur_second
                , tab.call_result
                , tab.call_result_num
                , tab.call_result_num_second
                , tab.opr_created
                , tab.ringing_dur
                , tab.talk_dur
                , tab.talk_dur_second
                , tab.hold_dur
                , tab.hold_dur_second
                , tab.wrapup_dur
                , tab.wrapup_dur_second
                , tab.servise_call_dur
                , tab.servise_call_dur_second
                , tab.servise_call_dur2
                , tab.servise_call_dur2_second
                , tab.opr_login
                , tab.complet_call_time
                , tab.sep_init
                , tab.call_in_work_time
                , tab.second_line
        FROM data_inc_call tab
        WHERE tab.rn = 1
                --CALLER NOT IN ('4957392201','957392201')
                --По заявке ZHKKH-490:
                --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
                --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
                AND  (
                (tab.call_created >= to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') AND tab.caller NOT IN ('4957392201','957392201'))
                OR ((tab.call_created <  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') AND
                tab.call_created >= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) AND substr(tab.caller, -10) NOT IN ('4957392201'))
                OR (tab.call_created <  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
                )
)
--------------------------------------------
-- Теперь считаем время обработки письма
--------------------------------------------
  , BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом --ZHKKH-1017
        SELECT 
         MAD.FID_MESSAGE,
         MAX('BLOCK_MAIL') AS MAIL_ADDRESS
        FROM mail_change_log clg
        JOIN MAIL_ADDRESSES MAD
         ON MAD.FID_MESSAGE = clg.FID_MESSAGE
        WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
          AND (clg.action_time >= CAST( I_INIT_TIME AS TIMESTAMP) AND clg.action_time < CAST( I_FINISH_TIME AS TIMESTAMP) +1)
        GROUP BY MAD.FID_MESSAGE
  )
, all_change AS  (--ВСЕ ИЗМЕНЕНИЯ ПИСЬМА
        SELECT
                clg.id_change_log AS id_change_log
                , clg.fid_message AS fid_message
                , us.login AS login
                , clg.action_time AS action_time
                , act.code AS code
        FROM mail_change_log clg --ЛОГ ИЗМЕНЕНИЙ
                JOIN mail_d_action_types act --ТИПЫ ИЗМЕНЕНИЙ
                        ON act.id_action_type = clg.fid_action_type
                LEFT JOIN cis.nc_users us --ОПЕРАТОРЫ
                        ON us.id_user = clg.fid_user
                LEFT JOIN BLOCK_MAILS BML
                 ON BML.FID_MESSAGE = clg.fid_message
        WHERE
        (clg.action_time >= CAST( I_INIT_TIME AS TIMESTAMP) AND clg.action_time < CAST( I_FINISH_TIME AS TIMESTAMP) +1)
        AND act.code IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
        AND us.login NOT IN ('i.a.strapko_gis_zhkh_Vol',
                'v.v.iliykhin_gis_zhkh_Vol',
                'o.i.ruskhanova_gis_zhkh_Vol',
                's.v.srybnaia_gis_zhkh_Vol',
                'a.horolskiy',
                'v.v.iliykhin_gis_zhkh_Vol',
                't.aitkaliev',
                'y.dudkin') --не нужно учитывать эти логины -- ДЛЯ ЗАЯВКИ ZHKKH-473
        AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017        
                
)
, intervals_mails AS (--Формирование писем с их временем обработки
        SELECT
                fid_message
                , login
                , action_time
                , code
                , lag (action_time,1) OVER (ORDER BY fid_message, action_time) AS prev_order_date
                , lag(code,1) OVER (ORDER BY fid_message, action_time) AS prev_code
        FROM all_change
        ORDER BY
                fid_message
                , action_time
)
, mails_statistica AS (--Статистика по временным интервалам
        SELECT 
                p.start_period
                , p.view_period --Период
                , count(DISTINCT fid_message) AS messages_count
                , /*round(ceil(SUM((NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)))),2)*/0 AS all_time
                , count(DISTINCT login ) AS count_login_email
        FROM periods p         
                JOIN intervals_mails ml
                        ON ml.prev_order_date BETWEEN p.start_period AND p.stop_period
        WHERE code IN ('assign') AND prev_code = 'open'
                AND (prev_order_date >= CAST( I_INIT_TIME AS TIMESTAMP) AND prev_order_date < CAST( I_FINISH_TIME AS TIMESTAMP))
        GROUP BY 
                p.start_period
                , p.view_period
) 
, mails_statistica_sum AS ( --Суммарное значение
        SELECT 
                sum(messages_count) AS messages_count
                , /*SUM(ALL_TIME)*/0 AS all_time
                ,tab.count_login_email as count_login_email
        FROM mails_statistica,
            (select count(distinct login) as count_login_email
             from intervals_mails
             WHERE code IN ('assign')
              AND prev_code = 'open'
              AND (prev_order_date >= CAST( I_INIT_TIME AS TIMESTAMP)
              AND prev_order_date < CAST( I_FINISH_TIME AS TIMESTAMP))
            ) tab
            
        GROUP BY tab.count_login_email        
)
-----------------------------------------------------
-- Информация по поступившим письмам
-----------------------------------------------------
, BLOCK_MAILS_2 AS ( --Так ограничиваем письма с определенным адресом --ZHKKH-1017
SELECT 
 FID_MESSAGE,
 MAX('BLOCK_MAIL') AS MAIL_ADDRESS
FROM MAIL_MESSAGES MSG
JOIN MAIL_ADDRESSES MAD
 ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
  AND MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME
GROUP BY MAD.FID_MESSAGE
)
, CALCULATION_MAILS AS (
SELECT
    PR.START_PERIOD
  , PR.VIEW_PERIOD
  , COUNT(DISTINCT MSG.ID_MESSAGE) AS MESSAGES_COUNT
   
FROM
     PERIODS PR
  JOIN MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
   ON MSG.CREATED_AT >= PR.START_PERIOD AND MSG.CREATED_AT < PR.STOP_PERIOD
  JOIN MAIL_D_MSG_STATUSES MST --СТАТУСЫ ПИСЕМ
   ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
  JOIN MAIL_D_REQUESTER_TYPES RTP --ЗАЯВИТЕЛЬ
   ON RTP.ID_REQUESTER_TYPE = MSG.FID_REQUESTER_TYPE
  JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
   ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
  LEFT JOIN BLOCK_MAILS_2 BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE
   
   
  WHERE MTP.DIRECTION = 'in'
    AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017
    GROUP BY PR.START_PERIOD, PR.VIEW_PERIOD
)

, CALCULATION_MAILS_SUM AS (
  SELECT 
    SUM(MESSAGES_COUNT) AS MESSAGES_COUNT
  FROM CALCULATION_MAILS  
)

---------------------------------------------
--     Информация по активным операторам
---------------------------------------------
--, operators_info AS ( --Собираем всю активность операторов
--        SELECT 
--                p.start_period
--                , p.stop_period  
--                , p.view_period --Период
--                , tab.opr_login AS login
--        FROM periods p         
--                JOIN data_inc_call_2 tab
--                        ON tab.call_created BETWEEN p.start_period AND p.stop_period
--
--) 
--, operators_info_stat AS (--Делаем по операторам статистику по интермалам
--        SELECT 
--                  start_period
--                , view_period --Период
--                , count(DISTINCT login ) AS count_login_CALLS
--        FROM operators_info
--        GROUP BY 
--                start_period
--                , view_period
--)
--, operators_info_sum AS  ( --Получаем итоговое значение по активным операторам
--        SELECT 
--                  count(DISTINCT login ) AS count_login_CALLS
--        FROM operators_info
--)
-------------------------------------------------------
-------------------------------------------------------
, operators_info_stat AS (
    SELECT
          start_period
        , view_period --Период 
        , COUNT(DISTINCT (CASE WHEN US.SKILL_GROUP = 'Голос' THEN US.LOGIN ELSE NULL  END)) AS count_login_CALLS
        , COUNT(DISTINCT (CASE WHEN US.SKILL_GROUP = 'E-mail' THEN US.LOGIN ELSE NULL  END)) AS count_login_EMAIL
        FROM naucrm.status_changes sc
        JOIN USERS_TEST US ON US.login = sc.login
        JOIN periods p ON sc.entered >= p.start_period AND sc.entered < p.stop_period
                  OR p.start_period >= sc.entered AND p.start_period < CAST(entered+duration/86400 AS TIMESTAMP)
        WHERE 
                entered >= CAST( I_INIT_TIME AS TIMESTAMP)-1 AND   entered< CAST( I_FINISH_TIME AS TIMESTAMP)
                AND CAST(entered+duration/86400 AS TIMESTAMP) > CAST( I_INIT_TIME AS TIMESTAMP)
                AND sc.status != 'offline'
        GROUP BY 
                p.start_period
              , p.view_period        
 )
, operators_info_sum AS (
    SELECT
          COUNT(DISTINCT (CASE WHEN US.SKILL_GROUP = 'Голос' THEN US.LOGIN ELSE NULL  END)) AS count_login_CALLS
        , COUNT(DISTINCT (CASE WHEN US.SKILL_GROUP = 'E-mail' THEN US.LOGIN ELSE NULL  END)) AS count_login_EMAIL
        FROM naucrm.status_changes sc
        JOIN USERS_TEST US ON US.login = sc.login
        
        WHERE 
                entered >= CAST( I_INIT_TIME AS TIMESTAMP)-1 AND   entered< CAST( I_FINISH_TIME AS TIMESTAMP)
                AND CAST(entered+duration/86400 AS TIMESTAMP) > CAST( I_INIT_TIME AS TIMESTAMP)
                AND sc.status != 'offline'
 ) 
, operators_list AS (
       SELECT DISTINCT
         opr_login AS login
       FROM data_inc_call_2
  )
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
, work_hours_prep AS (
        SELECT
                greatest(sc.entered, p.start_period) AS entered, 
                least(p.stop_period, CAST(sc.entered+sc.duration/86400 AS TIMESTAMP)) AS ended,
                sc.status, sc.login, sc.reason, sc.duration AS old_duration, 
                naucrm.intervaltosec(least(p.stop_period, CAST(sc.entered+sc.duration/86400 AS TIMESTAMP))-greatest(sc.entered, p.start_period)) AS duration
        FROM naucrm.status_changes sc
                JOIN operators_list opl ON opl.login = sc.login
                JOIN common.d_project_work_time pw
                        ON pw.fid_project_phones_id = 2401--in (2401, 2402, 2403, 2404, 2405, 2406, 2407, 2381)--иначе умножается на 8
                        AND sc.entered  >= pw.init_time AND sc.entered < pw.final_time
                        AND ((mod(to_number(to_char(sc.entered,'J')), 7)+1 IN (1, 2, 3, 4, 5) AND (sc.entered >= trunc(sc.entered) + pw.begin_operating_time_weekdays AND  sc.entered < trunc(sc.entered) + pw.end_operating_time_weekdays))
                        OR (mod(to_number(to_char(sc.entered,'J')), 7)+1 IN (6, 7) AND sc.entered >= trunc(sc.entered) + pw.begin_operating_time_holidays AND sc.entered < trunc(sc.entered) + pw.end_operating_time_holidays)
                         )
                   JOIN periods p ON sc.entered >= p.start_period AND sc.entered < p.stop_period
                          OR p.start_period >= sc.entered AND p.start_period < CAST(entered+duration/86400 AS TIMESTAMP)
        WHERE 
                entered >= CAST( I_INIT_TIME AS TIMESTAMP)-1 AND   entered< CAST( I_FINISH_TIME AS TIMESTAMP)
                AND CAST(entered+duration/86400 AS TIMESTAMP) > CAST( I_INIT_TIME AS TIMESTAMP)
                AND sc.status = 'normal'
)
, work_hours AS (
        SELECT 
                start_period
                , view_period --Период
                , sum(wh.duration) AS all_time
        FROM periods p
        JOIN work_hours_prep wh ON wh.entered >= p.start_period AND wh.entered < p.stop_period
        GROUP BY 
                p.view_period
                , p.start_period
)
, work_hours_sum AS (
        SELECT 
                sum(all_time) AS all_time
        FROM work_hours
)

------------------------------------------------
-- Объединяем результаты
------------------------------------------------     
, report_prp AS (
        SELECT
                p.start_period,
                p.view_period, --Период
                count(DISTINCT
                      CASE
                        WHEN connect_result_num != 3
                        THEN session_id
                      END) AS call_count, --Объем
                count(DISTINCT
                      CASE
                        WHEN dst_id IN ('4957392507','5555319863','5555319862')
                          AND connect_result_num != 3
                        THEN session_id
                          END) AS call_count_line_8800,
                count(DISTINCT
                      CASE
                        WHEN dst_id IN ('4957392209','5555392209','5555392210')
                          AND connect_result_num != 3
                        THEN session_id
                          END) AS call_count_line_8495,
                count(DISTINCT
                      CASE
                        WHEN connect_result_num = 3
                        THEN session_id
                      END) AS call_in_not_wt, --Завершенные в IVR в нерабочее время
                count(DISTINCT
                      CASE
                        WHEN connect_result_num = 2
                        THEN session_id
                      END) AS to_opr_call, --Направлено на операторов (1-я линия)
                count(DISTINCT
                      CASE
                        WHEN connect_result_num = 2 AND connect_result_num_second = 2
                        THEN session_id
                      END) AS to_opr_call_second, --Направлено на операторов (2-я линия)
                      
                count(DISTINCT
                      CASE
                        WHEN connect_result_num = 4
                        THEN session_id
                      END) AS lost_ivr, --Завершенные в IVR
                count(DISTINCT
                      CASE
                        WHEN call_result_num = 1
                          AND connect_result_num = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS ans_call, --Отвеченные операторами (1-я линия)
                count(DISTINCT
                      CASE
                        WHEN call_result_num = 1 AND call_result_num_second = 1
                          AND connect_result_num = 2 AND connect_result_num_second = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS ans_call_second, --Отвеченные операторами (2-я линия)
                      
                count(DISTINCT
                      CASE
                        WHEN call_result_num IN (2, 3)
                          AND connect_result_num = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS lost_queue, --Потерянные в очереди (1-я линия)
                count(DISTINCT
                      CASE
                        WHEN call_result_num IN (2, 3) AND call_result_num_second IN (2, 3)
                          AND connect_result_num = 2 AND connect_result_num_second = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS lost_queue_second, --Потерянные в очереди (2-я линия)                
                      
                count(DISTINCT
                      CASE
                        WHEN call_result_num IN (2, 3)
                          AND connect_result_num = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                          AND busy_dur <= 5
                        THEN session_id
                      END) AS lost_queue_5, --Потерянные в очереди до 5 секунд (1-я линия)
                count(DISTINCT
                      CASE
                        WHEN call_result_num IN (2, 3) AND call_result_num_second IN (2, 3)
                          AND connect_result_num = 2 AND connect_result_num_second = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                          AND busy_dur_second <= 5
                        THEN session_id
                      END) AS lost_queue_5_second, --Потерянные в очереди до 5 секунд (2-я линия)                
                      
                count(DISTINCT
                      CASE
                        WHEN busy_dur <= 30
                          AND call_result_num = 1
                          AND connect_result_num = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS ans_call_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
                count(DISTINCT
                      CASE
                        WHEN busy_dur_second <= 30
                          AND call_result_num = 1 AND call_result_num_second = 1
                          AND connect_result_num = 2 AND connect_result_num_second = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN session_id
                      END) AS ans_call_30_second, --Отвеченные до 30 секунд ожидания в очереди (2-я линия)
                      
                sum(CASE
                      WHEN call_result_num = 1
                      THEN talk_dur
                    END) - 
                           sum(CASE
                                WHEN call_result_num = 1 AND call_result_num_second = 1
                                THEN talk_dur_second
                                ELSE 0
                              END) AS speek_time, --Суммарное время разговора (1-я линия)
                                                      
                sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN talk_dur_second
                      ELSE 0
                    END) AS speek_time_second, --Суммарное время разговора (2-я линия)              
                
                sum(CASE
                      WHEN call_result_num = 1
                      THEN hold_dur
                    END) - 
                           sum(CASE
                                WHEN call_result_num = 1 AND call_result_num_second = 1
                                THEN hold_dur_second
                                ELSE 0
                              END) AS hold_time, --HOLD (сек) (1-я линия)
                                                      
                sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN hold_dur_second
                      ELSE 0
                    END) AS hold_time_second, --HOLD (сек) (2-я линия)           
                    
                sum(CASE
                      WHEN connect_result_num = 2
                      THEN busy_dur
                    END) - 
                     sum(CASE
                      WHEN connect_result_num = 2 AND connect_result_num_second = 2
                      THEN busy_dur_second
                      ELSE 0
                         END) AS busy_dur, --Суммарное время нахождения в очереди (1-я линия)
                sum(CASE
                      WHEN connect_result_num = 2 AND connect_result_num_second = 2
                      THEN busy_dur_second
                      ELSE 0
                    END) AS busy_dur_second, --Суммарное время нахождения в очереди (2-я линия)              
                
                
                sum(CASE
                      WHEN connect_result_num = 2
                       AND call_result_num IN (2, 3)
                      THEN busy_dur
                    END) - 
                     sum(CASE
                      WHEN connect_result_num = 2 AND connect_result_num_second = 2
                       AND call_result_num IN (2, 3) AND call_result_num_second IN (2, 3)                
                      THEN busy_dur_second
                      ELSE 0
                         END) AS busy_dur_of_lost, --Суммарное время нахождения в очереди (1-я линия) --ONLY LOST
                sum(CASE
                      WHEN connect_result_num = 2 AND connect_result_num_second = 2
                       AND call_result_num IN (2, 3) AND call_result_num_second IN (2, 3)
                      THEN busy_dur_second
                      ELSE 0
                    END) AS busy_dur_second_of_lost, --Суммарное время нахождения в очереди (2-я линия)  --ONLY LOST
                
                MAX(CASE
                      WHEN connect_result_num = 2
                       AND call_result_num IN (2, 3)
                      THEN busy_dur
                    END) AS max_busy_dur_of_lost,--Максимальное время ожидания потерянного вызова , сек
                
                 MIN(CASE
                      WHEN connect_result_num = 2
                       AND call_result_num IN (2, 3)
                      THEN busy_dur
                    END) AS min_busy_dur_of_lost,--Минимальное время ожидания потерянного вызова , сек
                    
                sum(CASE
                      WHEN call_result_num = 1
                      THEN wrapup_dur
                    END) - 
                    sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN wrapup_dur_second
                      ELSE 0
                    END)AS sum_wrapup, --Суммарное время поствызывной обработки (1-я линия)
                sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN wrapup_dur_second
                      ELSE 0
                    END) AS sum_wrapup_second, --Суммарное время поствызывной обработки (2-я линия)              
                    
                sum(CASE
                      WHEN call_result_num = 1
                      THEN servise_call_dur
                    END) - 
                    sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN servise_call_dur_second
                      ELSE 0
                       END) AS work_time, -- Суммарное время обработки вызова (1-я линия)
                sum(CASE
                      WHEN call_result_num = 1 AND call_result_num_second = 1
                      THEN servise_call_dur_second
                      ELSE 0
                    END) AS work_time_second, -- Суммарное время обработки вызова (1-я линия)  
                sum( CASE
                        WHEN call_result_num = 1
                          AND connect_result_num = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                        THEN ringing_dur
                      END) AS ringing_time --Ринг (сек)    --Время реакции на вызов
        FROM periods p
                JOIN data_inc_call_2
                        ON data_inc_call_2.call_created BETWEEN p.start_period AND p.stop_period
        GROUP BY 
                p.view_period
                , p.start_period
        ORDER BY p.view_period
)
, report_prp_itogo AS (--СУММЫ КОЛЛИЧЕСТВЕННЫХ ПОКАЗАТЕЛЕЙ
        SELECT
                sum(call_count) AS call_count, --Объем
                sum(call_count_line_8800) AS call_count_line_8800, --Объем ПО 8800
                sum(call_count_line_8495) AS call_count_line_8495, --Объем ПО 495
                sum(call_in_not_wt) AS call_in_not_wt, --Завершенные в IVR в нерабочее время
                sum(to_opr_call) AS to_opr_call, --Направлено на операторов (1-я линия)
                sum(to_opr_call_second) AS to_opr_call_second, --Направлено на операторов (2-я линия)
                sum(lost_ivr) AS lost_ivr, --Завершенные в IVR
                sum(ans_call) AS ans_call, --Отвеченные операторами (1-я линия)
                sum(ans_call_second) AS ans_call_second, --Отвеченные операторами (2-я линия)
                sum(lost_queue) AS lost_queue, --Потерянные в очереди (1-я линия)
                sum(lost_queue_second) AS lost_queue_second, --Потерянные в очереди (2-я линия)
                sum(lost_queue_5) AS lost_queue_5, --Потерянные в очереди до 5 секунд (1-я линия)
                sum(lost_queue_5_second) AS lost_queue_5_second, --Потерянные в очереди до 5 секунд (2-я линия)
                sum(ans_call_30) AS ans_call_30, --Отвеченные до 30 секунд ожидания в очереди (1-я линия)
                sum(ans_call_30_second) AS ans_call_30_second, --Отвеченные до 30 секунд ожидания в очереди (2-я линия)
                sum(speek_time) AS speek_time, --Суммарное время разговора (1-я линия)
                sum(speek_time_second) AS speek_time_second, --Суммарное время разговора (2-я линия)
                sum(hold_time) AS hold_time, --HOLD (1-я линия)
                sum(hold_time_second) AS hold_time_second, --HOLD (2-я линия)
                sum(busy_dur_of_lost) AS busy_dur_of_lost, --Суммарное время нахождения в очереди (1-я линия)
                sum(busy_dur_second_of_lost) AS busy_dur_second_of_lost, --Суммарное время нахождения в очереди (2-я линия)
                MAX(max_busy_dur_of_lost) AS max_busy_dur_of_lost,--Максимальное время ожидания потерянного вызова , сек
                MIN(min_busy_dur_of_lost) AS min_busy_dur_of_lost,--Минимальное время ожидания потерянного вызова , сек
                sum(sum_wrapup) AS sum_wrapup, --Суммарное время поствызывной обработки (1-я линия)
                sum(sum_wrapup_second) AS sum_wrapup_second, --Суммарное время поствызывной обработки (2-я линия)
                sum(work_time) AS work_time, -- Суммарное время обработки вызова (1-я линия)
                sum(work_time_second) AS work_time_second, -- Суммарное время обработки вызова (2-я линия)
                sum(ringing_time) AS ringing_time --Ринг (сек)    --Время реакции на вызов
        FROM
        report_prp
)--"OLD" - ГОВОРИТ О ТОМ, ЧТО ЭТОТ ПОКАЗАТЕЛЬ СОВПАДАЕТ СО СТАРОЙ ВЕРСИЕЙ ОТЧЕТА
---------------------------------------------------
--      Вывод результатов
---------------------------------------------------
SELECT 
        pr.start_period AS start_period,
        to_char(pr.view_period) AS view_period, --Период
        nvl(call_count,0) AS call_count, --Объем      
        nvl(ans_call,0) + nvl(ans_call_second,0) AS ans_call_all, --Отвеченные операторами 
        CASE
        WHEN call_count > 0
        THEN REPLACE(trim(to_char((CASE WHEN (ans_call_30 + ans_call_30_second) = 0 AND (to_opr_call + to_opr_call_second)-(lost_queue_5+lost_queue_5_second) = 0 THEN 1 ELSE (ans_call_30 + ans_call_30_second) END)/
                              decode(((to_opr_call + to_opr_call_second)-(lost_queue_5 + lost_queue_5_second)),0,1,((to_opr_call+to_opr_call_second)-(lost_queue_5+ lost_queue_5_second)))*100
                          ,'990D99')),'.',',')
        ELSE '100,00'
        END ||'%' AS sl_all, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (Обе линии)
        REPLACE(trim(to_char(nvl(
              ((lost_queue + lost_queue_second)-(lost_queue_5 + lost_queue_5_second))/
                         decode((to_opr_call+to_opr_call_second),0,1,(to_opr_call+to_opr_call_second))
                                    ,0)*100,'990D99')),'.',',')||'%' AS pec_lost_call_all, --LCR, %
        nvl(wh.all_time,0) AS idle, --Idle (сек)
        round(nvl(wh.all_time/decode((nvl(ans_call,0) + nvl(ans_call_second,0)),0,1,(nvl(ans_call,0) + nvl(ans_call_second,0))),0)) AS avg_idle, --Idle (сек) среднее
        round(nvl((busy_dur_of_lost+busy_dur_second_of_lost)/decode((lost_queue+lost_queue_second),0,1,(lost_queue+lost_queue_second)),0)) AS avg_queue_all, --Среднее время ожидания ПОТЕРЯННОГО ВЫЗОВА в очереди  (1-я и 2-я линия)
        nvl(max_busy_dur_of_lost,0) AS max_busy_dur_of_lost,--Максимальное время ожидания потерянного вызова , сек
        nvl(min_busy_dur_of_lost,0) AS min_busy_dur_of_lost,--Минимальное время ожидания потерянного вызова , сек
        round(nvl((work_time+work_time_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_work_all, --AHT "Голос" (мин)
        round(nvl(ringing_time/decode((ans_call + ans_call_second),0,1,(ans_call + ans_call_second)),0)) AS avg_ring_all, -- Ринг (сек)
        round(nvl((speek_time + speek_time_second)/decode((ans_call + ans_call_second),0,1,(ans_call + ans_call_second)),0)) AS avg_speek_all, --Среднее время диалога _Разговор (сек) (1-я и 2-я линия)
        round(nvl((hold_time+hold_time_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_hold_all, 
        round(nvl((sum_wrapup+sum_wrapup_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_wrapup_all, --Среднее время поствызывной обработки  (1-я и 2-я  линия)
        nvl(CM.messages_count,0) AS messages_count_rec,
        nvl(MS.messages_count,0) AS messages_count,
        nvl(op.count_login_CALLS,0) AS count_login_CALLS,
        nvl(op.count_login_EMAIL,0) AS count_login_EMAIL
FROM periods pr
        LEFT JOIN report_prp
                ON report_prp.view_period = pr.view_period
        LEFT JOIN mails_statistica MS
                ON MS.view_period = pr.view_period
        LEFT JOIN operators_info_stat op
                ON op.view_period = pr.view_period
        LEFT JOIN work_hours wh
                ON wh.view_period = pr.view_period
        LEFT JOIN CALCULATION_MAILS CM
                ON CM.view_period = pr.view_period
                
--where PR.START_PERIOD < PR.STOP_PERIOD--костыль
UNION ALL
SELECT
        NULL AS start_period,
        'Итого:' AS view_period, --Период
        nvl(call_count,0) AS call_count, --Объем      
        nvl(ans_call,0) + nvl(ans_call_second,0) AS ans_call_all, --Отвеченные операторами 
        CASE
        WHEN call_count > 0
        THEN REPLACE(trim(to_char((CASE WHEN (ans_call_30 + ans_call_30_second) = 0 AND (to_opr_call + to_opr_call_second)-(lost_queue_5+lost_queue_5_second) = 0 THEN 1 ELSE (ans_call_30 + ans_call_30_second) END)/
                              decode(((to_opr_call + to_opr_call_second)-(lost_queue_5 + lost_queue_5_second)),0,1,((to_opr_call+to_opr_call_second)-(lost_queue_5+ lost_queue_5_second)))*100
                          ,'990D99')),'.',',')
        ELSE '100,00'
        END ||'%' AS sl_all, --Уровень сервиса (90/30) --когда деление 0/0 - писать 100%  (Обе линии)
        REPLACE(trim(to_char(nvl(
              ((lost_queue + lost_queue_second)-(lost_queue_5 + lost_queue_5_second))/
                         decode((to_opr_call+to_opr_call_second),0,1,(to_opr_call+to_opr_call_second))
                                    ,0)*100,'990D99')),'.',',')||'%' AS pec_lost_call_all, --LCR, %
        nvl(wh.all_time,0) AS idle, --Idle (сек)
        round(nvl(wh.all_time/decode((nvl(ans_call,0) + nvl(ans_call_second,0)),0,1,(nvl(ans_call,0) + nvl(ans_call_second,0))),0)) AS avg_idle, --Idle (сек) среднее
        round(nvl((busy_dur_of_lost+busy_dur_second_of_lost)/decode((lost_queue+lost_queue_second),0,1,(lost_queue+lost_queue_second)),0)) AS avg_queue_all, --Среднее время ожидания ПОТЕРЯННОГО ВЫЗОВА в очереди  (1-я и 2-я линия)
        nvl(max_busy_dur_of_lost,0) AS max_busy_dur_of_lost,--Максимальное время ожидания потерянного вызова , сек
        nvl(min_busy_dur_of_lost,0) AS min_busy_dur_of_lost,--Минимальное время ожидания потерянного вызова , сек
        round(nvl((work_time+work_time_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_work_all, --AHT "Голос" (мин)
        round(nvl(ringing_time/decode((ans_call + ans_call_second),0,1,(ans_call + ans_call_second)),0)) AS avg_ring_all, -- Ринг (сек)
        round(nvl((speek_time + speek_time_second)/decode((ans_call + ans_call_second),0,1,(ans_call + ans_call_second)),0)) AS avg_speek_all, --Среднее время диалога _Разговор (сек) (1-я и 2-я линия)
        round(nvl((hold_time+hold_time_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_hold_all, 
        round(nvl((sum_wrapup+sum_wrapup_second)/decode((ans_call+ans_call_second),0,1,(ans_call+ans_call_second)),0)) AS avg_wrapup_all, --Среднее время поствызывной обработки  (1-я и 2-я  линия)
        nvl(CM.messages_count,0) AS messages_count_rec,
        nvl(ms.messages_count,0) AS messages_count,
        nvl(OP.count_login_CALLS,0) AS count_login_CALLS,
        nvl(OP.count_login_EMAIL,0) AS count_login_EMAIL
FROM report_prp_itogo
        , mails_statistica_sum ms
        , operators_info_sum OP
        , work_hours_sum wh
        , CALCULATION_MAILS_SUM CM
        
ORDER BY start_period ASC NULLS LAST
;



   TYPE t_loading_report IS TABLE OF cur_loading_report%rowtype;

  FUNCTION fnc_loading_report
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL
      , I_STEP NUMBER DEFAULT 1

  ) RETURN t_loading_report pipelined;   
  
  
  
  --------------------------------------------------------------------------------
  --                  ОТЧЕТ ПО НЕСТАНДАРТНЫМ ВОПРОСАМ                           --
  --------------------------------------------------------------------------------
  
   CURSOR cur_ns_questions (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
  )
IS
   WITH GIS_ZHKH AS (SELECT * FROM DUAL)
, CALLS_TYPE AS ( --Первый выбранный тип при ответе на вопросы --ZHKKH-917
        SELECT 
                CL.SESSION_ID         
                , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK FIRST ORDER BY QST.ID_QUESTION) AS TYPE_NAME 
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
        GROUP BY CL.SESSION_ID 
)
SELECT 
        CCL.ID_CALL, --ID звонка
        CCL.SESSION_ID, --Запись разговора
        CCL.CALLER AS ABONENT_NUM, --Номер абонента
        TO_CHAR(CCL.CREATED_AT,'dd.mm.yyyy hh24:mi') AS CREATED_AT, --Время начала разговора
        TO_CHAR(CCL.CLOSED_AT,'dd.mm.yyyy hh24:mi') AS CLOSED_AT, --Время завершения разговора
        CCL.OPERATOR_LOGIN, --Оператор
        ICCD.EMAIL_FROM, --E-mail от кого
        ICCD.EMAIL_SUBJECT AS THEME, --Тема
        ICCD.EMAIL_BODY AS TEXT, --Текст письма
        ICCD.COMPANY_NAME, --Наименование организации
        TDCTP.NAME AS ELIGIBLE_ORGANIZATION, --Полномочие организации
        DECODE(ICCD.REFUSED_TO_COMPANY_REGION, 1, 'Отказался называть', TDRG.NAME) AS REGION_NAME, --Регион организации
        ICCD.COMPANY_INN, --ИНН
        ICCD.COMPANY_KPP, --КПП
        ICCD.COMPANY_OGRN, --ОГРН/ОГРНИП
        ICCD.COMPANY_COMMENTS AS EX_ELIGIBLE, --Дополнительные сведения о полномочиях
        ICCD.CONTACT_NAME, --Ф.И.О. контактного лица
        ICCD.CONTACT_PHONE, --Номер контактного телефона (с кодом региона)
        ICCD.CONTACT_EMAIL, --Электронная почта
        /*TDTP.NAME*/ NVL(CTP.TYPE_NAME, TDTP.NAME) AS TYPE_NAME, --Тип обращения -- ZHKKH-917
        CTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2, --Тип обращения 2  --ZHKKH-917
        CTP.ADMIN_TYPE AS ADMIN_TYPE, --Административный тип --ZHKKH-917
        ICCD.DESCRIPTION AS APPEAL, --Краткое описание обращения
        ICCD.BODY AS EX_APPEAL, --Описание обращения
        TDOS.NAME AS OS, --Версия операционной системы
        ICCD.BROWSER, --Название браузера и его версия
        to_char(iccd.issue_date, 'dd.mm.yyyy') AS issue_date,
        iccd.issue_time
FROM CORE_CALLS CCL
        JOIN INC_CALL_CONTACT_DATA ICCD
            ON ICCD.FID_CALL=CCL.ID_CALL
        LEFT JOIN TICKETS_D_REGIONS TDRG
            ON TDRG.ID_REGION=ICCD.FID_COMPANY_REGION
        LEFT JOIN TICKETS_D_COMPANY_TYPES TDCTP
            ON TDCTP.ID_COMPANY_TYPE=ICCD.FID_COMPANY_TYPE
        LEFT JOIN TICKETS_D_TYPES TDTP
            ON TDTP.ID_TYPE=ICCD.FID_TYPE
        LEFT JOIN TICKETS_D_OS TDOS
            ON TDOS.ID_OS=ICCD.FID_OS
        LEFT JOIN CALLS_TYPE CTP
            ON CTP.SESSION_ID = CCL.SESSION_ID --ZHKKH-917
WHERE 
        CCL.CREATED_AT BETWEEN I_INIT_TIME AND I_FINISH_TIME
        AND LOWER(CCL.DIRECTION) = 'in'
        AND ICCD.FID_MESSAGE_MAIL IS NOT NULL
ORDER BY CCL.ID_CALL;


   TYPE t_ns_questions IS TABLE OF cur_ns_questions%rowtype;

  FUNCTION fnc_ns_questions
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  ) RETURN t_ns_questions pipelined; 


  --------------------------------------------------------------------------------
  --                  ЗВОНКОВАЯ НАГРУЗКА ПО РЕГИОНАМ                            --
  --------------------------------------------------------------------------------
  
   CURSOR cur_calls_stat_by_regions (
        i_init_time TIMESTAMP,
        i_finish_time TIMESTAMP,
        i_projectid VARCHAR2,
        i_phonenumbers VARCHAR2
    )
  IS
   WITH
    GIS_ZHKH AS (SELECT * FROM DUAL),
      --Все входящие звонки (как в рабочее время, так и в нерабочее)
      --если раскомментить код в этом представлении, то будет учитываться только в рабочее время
      nau_ic AS
        (SELECT /*+ parallel (c 4) */
            c.*,
            --substr(c.src_id, -10) AS src_phone,
            c.src_id AS src_phone,
            p.ID AS project_id
         FROM naucrm.call_legs c
           JOIN common.d_project_phones ph ON ph.phone = dst_id
                                              AND c.created BETWEEN ph.begin_time AND ph.end_time
           JOIN common.d_projectsaddinf pi ON pi.ID = ph.fid_projectsaddinf_id
           JOIN naucrm.projects p ON p.ID = pi.fid_project_id
       --    JOIN common.d_project_work_time pw ON pw.fid_project_phones_id = ph.ID
       --                                       AND c.created BETWEEN pw.init_time AND pw.final_time
         WHERE c.created >= I_init_time
           AND c.created <= I_finish_time
           and c.src_id not in ('4957392201','957392201')
           --AND (substr(c.src_id, -10) NOT IN ('4957392201') OR c.src_id IS NULL OR LENGTH(c.src_id) < 10 /**/)
           AND pi.status = 'ACTIVE'
           AND (pi.fid_project_id = I_projectid OR I_projectid = NULL)
           --AND pi.dirrection  = 'IN'--ЭТУ ФИГНЮ НАДО БУДЕТ ИСПРАВИТЬ В КРУТОЙ ТАБЛИЦЕ
           AND (I_phonenumbers IS NULL OR dst_id IN (SELECT * FROM TABLE(common.strutils.fnc_gettblfromstring(I_phonenumbers, ','))))
           AND c.src_abonent_type = 'UNKNOWN'
           AND c.incoming = '1'
           AND ((mod(to_number(to_char(c.created,'J')),7)+1 IN (1,2,3,4,5) /*AND (c.created BETWEEN trunc(c.created) + pw.begin_operating_time_weekdays AND trunc(c.created) + pw.end_operating_time_weekdays)*/)
               OR (mod(to_number(to_char(c.created,'J')),7)+1 IN (6,7)/* AND c.created BETWEEN trunc(c.created) + pw.begin_operating_time_holidays AND trunc(c.created) + pw.end_operating_time_holidays*/))
         ORDER BY c.session_id) ,

--      cisco_ic AS (
--         SELECT
--            substr(b.phone, -10) AS src_phone,
--            da.fid_project_id AS project_id,
--            b.call_id,
--            b.opr_created_time,
--            b.opr_connected_time,
--            b.enqueued_time,
--            b.dequeued_time - b.enqueued_time AS queue_time
--         FROM cisco.cisco_calls b
--           JOIN common.d_project_phones dp ON dp.phone = substr(b.projectphone, -10)
--           JOIN common.d_projectsaddinf da ON dp.fid_projectsaddinf_id = da.ID AND
--                                        (da.fid_project_id = I_projectid OR (I_projectid IS NULL AND da.fid_project_id IN ('project245')))
--         WHERE b.call_init_time BETWEEN I_init_time AND I_finish_time AND
--              (I_phonenumbers IS NULL OR substr(b.projectphone, -10) IN (SELECT * FROM TABLE(common.strutils.fnc_gettblfromstring(I_phonenumbers, ','))))
--      ),
      calls AS (
         SELECT
           A.src_phone,
           A.project_id,
           A.session_id AS call_id,
           cl2.created AS opr_created_time,
           cl2.connected AS opr_connected_time,
           qc.unblocked_time AS enqueued_time, --время поступления в очередь
           qc.dequeued_time - qc.unblocked_time AS queue_time
         FROM nau_ic A
           LEFT JOIN (SELECT *
                      FROM
                       (SELECT cl.*, row_number() OVER(PARTITION BY cl.session_id ORDER BY cl.connected NULLS LAST, cl.created NULLS LAST) AS rn
                        FROM nau_ic i
                          JOIN naucrm.call_legs cl ON cl.session_id = i.session_id
                                               AND cl.src_abonent_type = 'SS'
                                               AND ((cl.dst_abonent_type = 'SP') OR (cl.dst_abonent_type = 'UNKNOWN' AND REGEXP_LIKE(cl.dst_id, '^-?[[:digit:].,]*$') )))
                      WHERE rn = 1) cl2 ON cl2.session_id = A.session_id
           LEFT JOIN (SELECT *
                      FROM (
                        SELECT q.*, row_number() OVER(PARTITION BY q.session_id ORDER BY ivr_leg_id) AS rn
                        FROM naucrm.queued_calls q
                          JOIN nau_ic ON nau_ic.session_id = q.session_id)
                      WHERE rn = 1) qc ON qc.session_id = A.session_id
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
      calls2 AS (
         SELECT c.*,
                to_number( REGEXP_REPLACE(src_phone,'\D','') ) AS phone,
                CASE WHEN instr(src_phone, '9') = 1 THEN 1
                     ELSE 0 END AS is_sps,
                count(c.call_id) OVER() AS all_count
         FROM calls c
         JOIN DATA_INC_CALL_2 tab--Это нужно, поскольку РАНЬШЕ отчет фиксировал ДУБЛИ
          ON c.call_id = tab.session_id    --
          where REGEXP_LIKE(src_phone, '^-?[[:digit:].,]*$')
      ),

      region_sq AS (
       SELECT b.*,
              CASE WHEN m.area IS NULL THEN 'Не найдено в плане нумерации'
                   WHEN instr(m.area, '|') = 0 THEN m.area
                   WHEN instr(m.area, '|', -1) > 0 THEN trim(substr(m.area, -1 * (LENGTH(m.area) - instr(m.area, '|', -1))))
                   ELSE 'Не определено'
              END AS area,
            ROW_NUMBER()OVER(PARTITION BY b.call_id ORDER BY (CASE WHEN m.area IS NULL THEN 'Не найдено в плане нумерации'
                                                                   WHEN instr(m.area, '|') = 0 THEN m.area
                                                                   WHEN instr(m.area, '|', -1) > 0 THEN trim(substr(m.area, -1 * (LENGTH(m.area) - instr(m.area, '|', -1))))
                                                                   ELSE 'Не определено'
                                                                 END)
                                                                 ASC)   AS RN

       FROM calls2 b
         LEFT JOIN common.d_phonecodes_mr m ON (floor(REGEXP_REPLACE(b.phone,'\D','') / 10000000)) = (floor(m.rangeend / 10000000)) AND
                                                REGEXP_REPLACE(b.phone,'\D','') BETWEEN m.rangestart AND m.rangeend
       WHERE b.phone IS NOT NULL
      ),

   sq_region_names AS (
       SELECT
         area,
         kpr.kladr_region_name,
         mr.macroregion_name,
         mr.macroregion_short_name
       FROM (select distinct area
             from region_sq where RN = 1) reg
         LEFT JOIN common.d_kladr_phonecodes_regions kpr ON kpr.phonecodess_area = reg.area
         LEFT JOIN common.d_rf_macroregions mr ON mr.kladr_objectcode = kpr.kladr_objectcode
      )

    SELECT
      GROUPING(c.area) AS gr,
      decode(GROUPING(c.area),1,'Итого',MAX(macroregion_name)) AS macroregion, --Макрорегион
     DECODE(GROUPING(c.area),
                   1,'Итого',
                      decode(rtrim(ltrim(regexp_replace(c.area,'[[:space:]]',' '))), 'Республика Татарстан (Татарстан)', 'Татарстан Почтасы', c.area)
                            ) AS region, --Регион
      sum(CASE WHEN call_id IS NOT NULL AND
                    is_sps = 0 THEN 1
               ELSE 0 END) AS volume, -- Volume (СПС)
      sum(CASE WHEN call_id IS NOT NULL AND
                    is_sps = 1 THEN 1
               ELSE 0 END) AS volume_sps, --Volume (СПС)
      sum(CASE WHEN enqueued_time IS NOT NULL AND
                    is_sps = 0 THEN 1
               ELSE 0 END) AS incomingcalls, --IncomingCalls (стац. телефоны)
      sum(CASE WHEN enqueued_time IS NOT NULL AND
                    is_sps = 1 THEN 1
               ELSE 0 END) AS incomingcalls_sps, --IncomingCalls (СПС)
      sum(CASE WHEN opr_connected_time IS NOT NULL AND
                    is_sps = 0 THEN 1
               ELSE 0 END) AS answeredcalls, --AnsweredCalls (стац. телефоны)
      sum(CASE WHEN opr_connected_time IS NOT NULL AND
                    is_sps = 1 THEN 1
               ELSE 0 END) AS answeredcalls_sps, --AnsweredCalls (СПС)
      sum(CASE WHEN opr_connected_time IS NULL AND
                    enqueued_time IS NOT NULL AND
                    is_sps = 0 THEN 1
               ELSE 0 END) AS abandomedcalls, --AbandomedCalls (стац. телефоны)
      sum(CASE WHEN opr_connected_time IS NULL AND
                    enqueued_time IS NOT NULL AND
                    is_sps = 1 THEN 1
               ELSE 0 END) AS abandomedcalls_sps,--AbandomedCalls (СПС)
     decode(GROUPING(c.area),
       1,'100',
          REPLACE(TRIM(TO_CHAR(round(count(call_id)/decode(MAX(all_count), 0, 1, MAX(all_count))*100, 2),'9990D99')),'.',',')
                                                                                                                    ) AS reg_rank --Доля региона в общей нагрузке(%)

    FROM region_sq c
      LEFT JOIN sq_region_names m
           ON m.area = c.area
      where c.RN = 1
    GROUP BY ROLLUP(c.area)
    ORDER BY 1, 2, 3;

   TYPE t_calls_stat_by_regions  IS TABLE OF cur_calls_stat_by_regions %rowtype;

  FUNCTION fnc_calls_stat_by_regions 
  (
      I_INIT_TIME TIMESTAMP,
      I_FINISH_TIME TIMESTAMP,
      I_PROJECTID VARCHAR2,
      I_LINEFILTER NUMBER := 1

  ) RETURN t_calls_stat_by_regions pipelined; 



  -------------------------------------------------------------------------------
  --               ОТЧЕТ ПО CRM
  -------------------------------------------------------------------------------
 
 CURSOR CUR_REPORT_ON_CRM (
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_CHECK_REPORT NUMBER :=0
    , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
   )
  IS
  WITH
    GIS_ZHKH AS (SELECT * FROM DUAL)
  , CALLS_TYPE AS ( --Первый выбранный тип при ответе на вопросы --ZHKKH-917
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
        WHERE TDT_LEV_2.NAME != 'Тестовое обращение'      --В этом отчете не нужны тестовые звонки
--        UNION ALL
--        SELECT 
--                1001 AS ID_TYPE_LEVEL_1 --ID ТИПА ПЕРВОГО УРОВНЯ
--                , 1001 AS ID_TYPE_LEVEL_2 --ID ТИПА ВТОРОГО УРОВНЯ  
--                , 'Посторонний звонок' AS TYPE_NAME_LEVEL_1 --ТИП ПЕРВОГО УРОВНЯ
--                , 'Посторонний звонок' AS TYPE_NAME_LEVEL_2 --ТИП ВТОРОГО УРОВНЯ
--                , '-' AS CLASS_TYPE --(ГРАЖДАНИН ИЛИ НЕ ГРАЖДАНИН)
--                , 3 AS ORD
--        FROM DUAL  
        --ORDER BY (case when act.code = 'not_citizen' then 1 else 2 end),TDT_LEV_1.ID_TYPE, TDT_LEV_2.ID_TYPE
) 
, FORMAT AS (
        SELECT * 
        FROM /*PERIODS
                ,*/ ALL_TYPES_FOR_FORMAT TTP
--  ORDER BY START_PERIOD,ORD,(case when CLASS_TYPE = 'Гражданин' then 1 else 2 end), ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
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
    )
-- , FORMAT AS --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
--        (
--               SELECT DECODE(ID_TYPE,
--                                    13,9.5,
--                                    --12,1000,
--                                            ID_TYPE) AS ID_TYPE, NAME FROM TICKETS_D_TYPES
--                                            WHERE CODE !=  'test'
--              -- UNION
--              -- SELECT 1001 AS ID_TYPE, 'Посторонний звонок' AS NAME FROM DUAL
--              -- UNION
--              -- SELECT 1000 AS ID_TYPE, 'Тестовое обращение' AS NAME FROM DUAL
--               ORDER BY ID_TYPE
--         )
  , GROUP_VALUES AS (
  --Группировка по ОГРН
  SELECT
    CLT.TYPE_NAME_LEVEL_1
  , CLT.TYPE_NAME_LEVEL_2
  , MAX(CASE
        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
        ELSE 'НЕ гражданин'
     END) AS CLASS_TYPE
  , INC.COMPANY_OGRN AS GROUP_NUMBER,
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
  JOIN CALLS_TYPE CLT
    ON CLT.SESSION_ID = CL.SESSION_ID
  JOIN TICKETS_D_COMPANY_TYPES CPT
    ON CPT.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE   
  WHERE
       INC.REFUSED_TO_COMPANY_OGRN = 0
   AND LENGTH(REGEXP_REPLACE(INC.COMPANY_OGRN,'\D','')) > 0
   AND ACL.ANS_CALL = 1 --Отвеченные операторами
   AND INC.IS_LEGAL_ENTITY = 1 --Является ли юридическим лицом (0 - физическое, 1 - юридическое)
   AND (CLT.ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND CLT.ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
   AND I_CHECK_REPORT = 0 --OGRN
  GROUP BY CLT.TYPE_NAME_LEVEL_1
         , CLT.TYPE_NAME_LEVEL_2
         , INC.COMPANY_OGRN

  UNION ALL
  --Группировка по АОН
  SELECT
    CLT.TYPE_NAME_LEVEL_1
  , CLT.TYPE_NAME_LEVEL_2
  , MAX(CASE
        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
        ELSE 'НЕ гражданин'
     END) AS CLASS_TYPE
  , CL.CALLER AS GROUP_NUMBER,
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
  JOIN CALLS_TYPE CLT
   ON CLT.SESSION_ID = CL.SESSION_ID
  JOIN TICKETS_D_COMPANY_TYPES CPT
    ON CPT.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE 
  WHERE
    (INC.REFUSED_TO_COMPANY_OGRN = 1 OR LENGTH(REGEXP_REPLACE(INC.COMPANY_OGRN,'\D','')) = 0)
   AND ACL.ANS_CALL = 1 --Отвеченные операторами 
   AND INC.IS_LEGAL_ENTITY = 0 --Является ли юридическим лицом (0 - физическое, 1 - юридическое)
   AND (CLT.ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND CLT.ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
   AND I_CHECK_REPORT = 1 --AOH
  GROUP BY CLT.TYPE_NAME_LEVEL_1
         , CLT.TYPE_NAME_LEVEL_2
         , CL.CALLER

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
     TYPE_NAME_LEVEL_1
   , TYPE_NAME_LEVEL_2
   , CLASS_TYPE
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
   GROUP BY  TYPE_NAME_LEVEL_1
          ,  TYPE_NAME_LEVEL_2
          ,  CLASS_TYPE
)
, ITOG_DATA AS (
--СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
SELECT
    DECODE(GROUPING(FT.TYPE_NAME_LEVEL_1)
                ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
  , FT.TYPE_NAME_LEVEL_2
  , FT.CLASS_TYPE
  , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
  , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
  , MAX(FT.ORD) AS ORD
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

  FROM SUM_VALUES SV
  RIGHT JOIN FORMAT FT 
    ON FT.TYPE_NAME_LEVEL_1 = SV.TYPE_NAME_LEVEL_1
   AND FT.TYPE_NAME_LEVEL_2 = SV.TYPE_NAME_LEVEL_2
   AND FT.CLASS_TYPE = SV.CLASS_TYPE

  GROUP BY ROLLUP(FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)
  ORDER BY GROUPING(FT.TYPE_NAME_LEVEL_1),ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1, ID_TYPE_LEVEL_2
 )
 SELECT *
 FROM ITOG_DATA
 WHERE
        (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) 
        OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
 
;


    TYPE t_REPORT_ON_CRM IS TABLE OF cur_REPORT_ON_CRM%rowtype;

  FUNCTION fnc_REPORT_ON_CRM
  (
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_CHECK_REPORT NUMBER :=0
    , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

  ) RETURN t_REPORT_ON_CRM pipelined;



------------------------------------------------------------------
--5.11 Детализированный отчет по классификациям вопросов пользователей во входящем скрипте
------------------------------------------------------------------
 CURSOR CUR_REPORT_ON_questions (
  I_INIT_TIME TIMESTAMP,
  I_FINISH_TIME TIMESTAMP
   )
  IS
  WITH
GIS_ZHKH AS (
        SELECT * FROM DUAL
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
, ALL_CALLS AS (--ПОТОМУ ЧТО БЫВАЮТ ДУБЛИ. БЕРЕМ МАКСИМАЛЬНЫЙ ID
        SELECT 
                CL.SESSION_ID
                , MAX(CL.ID_CALL) AS ID_CALL
        FROM CORE_CALLS CL
                JOIN CORE_CALLS_RESULTS RES
                    ON RES.ID_RESULT = CL.FID_RESULT
        WHERE 
                RES.NAME IN ('Вопрос решен', 'Оформлено обращение', 'Не готов продиктовать данные, перезвонит', 'Звонок переведен')
                AND  (CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME)
        GROUP BY CL.SESSION_ID
)
, ALL_INFORMATION_CALLS AS (
        SELECT 
                CL.SESSION_ID
                , CL.CREATED_AT
                , CL.OPERATOR_LOGIN
                , CL.ID_CALL
                , REG.NAME AS REGION_NAME
                , CPT.NAME AS COMPANY_TYPE --Полномочие организации
                , RES.NAME AS CALL_RESULT --Статус звонка
                , INC.COMPANY_OGRN
                , INC.OGRN_REFUSE_REASON --Отказ ОГРН
        FROM ALL_CALLS ACL
                JOIN CORE_CALLS CL
                    ON CL.ID_CALL = ACL.ID_CALL
                JOIN CORE_CALLS_RESULTS RES
                    ON RES.ID_RESULT = CL.FID_RESULT 
                LEFT JOIN INC_CALL_CONTACT_DATA INC
                    ON INC.FID_CALL = CL.ID_CALL
                LEFT JOIN TICKETS_D_REGIONS REG
                    ON REG.ID_REGION = INC.FID_COMPANY_REGION
                LEFT JOIN TICKETS_D_COMPANY_TYPES CPT
                    ON CPT.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE   
)
, ALL_QUESTIONS AS (
        SELECT 
                ACL.ID_CALL
                , TP.TYPE_NAME_LEVEL_1 
                , TP.TYPE_NAME_LEVEL_2
                , ADT.NAME AS ADMIN_TYPE
                , ROW_NUMBER()OVER(PARTITION BY ACL.ID_CALL ORDER BY CLQ.ID_QUESTION ASC)  AS RN
        FROM ALL_CALLS ACL
                JOIN INC_CALL_QUESTIONS CLQ
                    ON CLQ.FID_CALL = ACL.ID_CALL
                JOIN ALL_TYPES TP
                    ON TP.ID_TYPE_LEVEL_2 = CLQ.FID_TICKET_TYPE
                JOIN TICKETS_D_ADM_TYPES ADT 
                    ON ADT.ID_TYPE = CLQ.FID_TICKET_ADM_TYPE
) 
, QUESTIONS_STATISTIC AS (
        SELECT 
                ID_CALL
                , MAX(DECODE(RN,1,TYPE_NAME_LEVEL_1,NULL)) AS  QS_1_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,1,TYPE_NAME_LEVEL_2,NULL)) AS  QS_1_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,1,ADMIN_TYPE,NULL)) AS  QS_1_ADMIN_TYPE
                
                , MAX(DECODE(RN,2,TYPE_NAME_LEVEL_1,NULL)) AS  QS_2_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,2,TYPE_NAME_LEVEL_2,NULL)) AS  QS_2_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,2,ADMIN_TYPE,NULL)) AS  QS_2_ADMIN_TYPE
                
                , MAX(DECODE(RN,3,TYPE_NAME_LEVEL_1,NULL)) AS  QS_3_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,3,TYPE_NAME_LEVEL_2,NULL)) AS  QS_3_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,3,ADMIN_TYPE,NULL)) AS  QS_3_ADMIN_TYPE
                
                , MAX(DECODE(RN,4,TYPE_NAME_LEVEL_1,NULL)) AS  QS_4_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,4,TYPE_NAME_LEVEL_2,NULL)) AS  QS_4_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,4,ADMIN_TYPE,NULL)) AS  QS_4_ADMIN_TYPE
                
                , MAX(DECODE(RN,5,TYPE_NAME_LEVEL_1,NULL)) AS  QS_5_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,5,TYPE_NAME_LEVEL_2,NULL)) AS  QS_5_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,5,ADMIN_TYPE,NULL)) AS  QS_5_ADMIN_TYPE
                
                , MAX(DECODE(RN,6,TYPE_NAME_LEVEL_1,NULL)) AS  QS_6_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,6,TYPE_NAME_LEVEL_2,NULL)) AS  QS_6_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,6,ADMIN_TYPE,NULL)) AS  QS_6_ADMIN_TYPE
                
                , MAX(DECODE(RN,7,TYPE_NAME_LEVEL_1,NULL)) AS  QS_7_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,7,TYPE_NAME_LEVEL_2,NULL)) AS  QS_7_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,7,ADMIN_TYPE,NULL)) AS  QS_7_ADMIN_TYPE
                
                , MAX(DECODE(RN,8,TYPE_NAME_LEVEL_1,NULL)) AS  QS_8_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,8,TYPE_NAME_LEVEL_2,NULL)) AS  QS_8_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,8,ADMIN_TYPE,NULL)) AS  QS_8_ADMIN_TYPE
                
                , MAX(DECODE(RN,9,TYPE_NAME_LEVEL_1,NULL)) AS  QS_9_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,9,TYPE_NAME_LEVEL_2,NULL)) AS  QS_9_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,9,ADMIN_TYPE,NULL)) AS  QS_9_ADMIN_TYPE
                
                , MAX(DECODE(RN,10,TYPE_NAME_LEVEL_1,NULL)) AS  QS_10_TYPE_NAME_LEVEL_1
                , MAX(DECODE(RN,10,TYPE_NAME_LEVEL_2,NULL)) AS  QS_10_TYPE_NAME_LEVEL_2
                , MAX(DECODE(RN,10,ADMIN_TYPE,NULL)) AS  QS_10_ADMIN_TYPE
        FROM ALL_QUESTIONS
        WHERE RN<=10
        GROUP BY ID_CALL
)
SELECT 
        CL.SESSION_ID
        , TO_CHAR(CL.CREATED_AT,'dd.mm.yyyy hh24:mi:ss') AS CREATED_AT
        , CL.OPERATOR_LOGIN
        , CL.ID_CALL
        , CL.REGION_NAME
        , CL.COMPANY_TYPE --Полномочие организации
        , CL.CALL_RESULT --Статус звонка
        , CL.COMPANY_OGRN
        , CL.OGRN_REFUSE_REASON --Отказ ОГРН
        
        , QS.QS_1_TYPE_NAME_LEVEL_1 --Вопрос 1. Классификатор, первый уровень
        , QS.QS_1_TYPE_NAME_LEVEL_2 --Вопрос 1. Классификатор, второй уровень
        , QS.QS_1_ADMIN_TYPE --Вопрос 1. Административный тип
        
        , QS.QS_2_TYPE_NAME_LEVEL_1 
        , QS.QS_2_TYPE_NAME_LEVEL_2
        , QS.QS_2_ADMIN_TYPE
        
        , QS.QS_3_TYPE_NAME_LEVEL_1 
        , QS.QS_3_TYPE_NAME_LEVEL_2
        , QS.QS_3_ADMIN_TYPE 
        
        , QS.QS_4_TYPE_NAME_LEVEL_1 
        , QS.QS_4_TYPE_NAME_LEVEL_2
        , QS.QS_4_ADMIN_TYPE 
        
        , QS.QS_5_TYPE_NAME_LEVEL_1 
        , QS.QS_5_TYPE_NAME_LEVEL_2
        , QS.QS_5_ADMIN_TYPE 
        
        , QS.QS_6_TYPE_NAME_LEVEL_1 
        , QS.QS_6_TYPE_NAME_LEVEL_2
        , QS.QS_6_ADMIN_TYPE 
        
        , QS.QS_7_TYPE_NAME_LEVEL_1 
        , QS.QS_7_TYPE_NAME_LEVEL_2
        , QS.QS_7_ADMIN_TYPE 
        
        , QS.QS_8_TYPE_NAME_LEVEL_1 
        , QS.QS_8_TYPE_NAME_LEVEL_2
        , QS.QS_8_ADMIN_TYPE 
        
        , QS.QS_9_TYPE_NAME_LEVEL_1 
        , QS.QS_9_TYPE_NAME_LEVEL_2
        , QS.QS_9_ADMIN_TYPE 
        
        , QS.QS_10_TYPE_NAME_LEVEL_1 
        , QS.QS_10_TYPE_NAME_LEVEL_2
        , QS.QS_10_ADMIN_TYPE 
FROM ALL_INFORMATION_CALLS CL
        JOIN QUESTIONS_STATISTIC QS
            ON QS.ID_CALL = CL.ID_CALL
ORDER BY CL.CREATED_AT 
;
   


  TYPE t_REPORT_ON_questions IS TABLE OF cur_REPORT_ON_questions%rowtype;

  FUNCTION fnc_REPORT_ON_questions
  (
    I_INIT_TIME TIMESTAMP,
    I_FINISH_TIME TIMESTAMP

  ) RETURN t_REPORT_ON_questions pipelined;  


END PKG_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_REPORTS AS

  --------------------------------------------------------------------------------
  --          ДЕТАЛИЗИРОВАННЫЙ ОТЧЕТ ПО ВХОДЯЩИМ ЗВОНКАМ                        --
  --------------------------------------------------------------------------------
  
    FUNCTION fnc_rep_inc_call
    (
         I_INIT_TIME TIMESTAMP
       , I_FINISH_TIME TIMESTAMP
    
    ) RETURN t_rep_inc_call pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_rep_inc_call(I_INIT_TIME, I_FINISH_TIME)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_rep_inc_call;  

  -------------------------------------------------------------------------------
  --                    ОБЩИЙ ОТЧЕТ ПО ЗВОНКАМ                                 --
  -------------------------------------------------------------------------------

    FUNCTION fnc_rep_general_calls
    (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL
      , I_STEP NUMBER DEFAULT 1
    
    ) RETURN t_rep_general_calls pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_rep_general_calls(I_INIT_TIME, I_FINISH_TIME,I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_rep_general_calls;  

  -------------------------------------------------------------------------------
  --                       ОТЧЕТ ПО НАГРУЗКЕ                                   --
  -------------------------------------------------------------------------------

  FUNCTION fnc_loading_report
    (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL
      , I_STEP NUMBER DEFAULT 1
    
    ) RETURN t_loading_report pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_loading_report(I_INIT_TIME, I_FINISH_TIME,I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_loading_report;  
  
 
  
  --------------------------------------------------------------------------------
  --          ДЕТАЛИЗИРОВАННЫЙ ОТЧЕТ ПО ВХОДЯЩИМ ЗВОНКАМ                        --
  --------------------------------------------------------------------------------
  
    FUNCTION fnc_ns_questions
    (
         I_INIT_TIME TIMESTAMP
       , I_FINISH_TIME TIMESTAMP
    
    ) RETURN t_ns_questions pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_ns_questions(I_INIT_TIME, I_FINISH_TIME)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_ns_questions;   
  
  
  --------------------------------------------------------------------------------
  --                  ЗВОНКОВАЯ НАГРУЗКА ПО РЕГИОНАМ                            --
  --------------------------------------------------------------------------------
  
    FUNCTION fnc_calls_stat_by_regions
    (
         I_INIT_TIME TIMESTAMP
       , I_FINISH_TIME TIMESTAMP
       , I_PROJECTID VARCHAR2
       , I_LINEFILTER NUMBER := 1
    
    ) RETURN t_calls_stat_by_regions pipelined AS
    
    v_project_id VARCHAR2(100);
    v_phones VARCHAR(300);
   
   BEGIN
    
   IF (i_linefilter = 1) THEN
      v_project_id := 'project245';
      v_phones     := '4957392507,5555319863,5555319862';--correct
   ELSIF (i_linefilter = 2) THEN  --Если нужно будет учитывать звонки с 2-х линий
      v_project_id := 'project245';
      v_phones     := '4957392507,5555319863,5555319862,4957392209,5555392209,5555392210';
   END IF;

    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
    EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_calls_stat_by_regions(I_INIT_TIME, I_FINISH_TIME,I_PROJECTID , V_PHONES)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_calls_stat_by_regions;  


--------------------------------------------------------------
--               ОТЧЕТ ПО CRM                          --
--------------------------------------------------------------

  FUNCTION fnc_REPORT_ON_CRM
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_CHECK_REPORT NUMBER :=0
    , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

) RETURN t_REPORT_ON_CRM pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_REPORT_ON_CRM(I_INIT_TIME, I_FINISH_TIME, I_CHECK_REPORT, I_ADMIN_TYPE)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_REPORT_ON_CRM;   
  
  
  
  --------------------------------------------------------------
  --      Детализированный отчет по классификациям вопросов пользователей во входящем скрипте                         --
  --------------------------------------------------------------

  FUNCTION fnc_REPORT_ON_questions
  (
    I_INIT_TIME TIMESTAMP,
    I_FINISH_TIME TIMESTAMP

  ) RETURN t_REPORT_ON_questions pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_REPORT_ON_questions(I_INIT_TIME, I_FINISH_TIME)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_REPORT_ON_questions; 


END PKG_REPORTS;
/
