CREATE OR REPLACE PACKAGE PKG_GENERAL_REPORTS AS 

  ----------------------------------------------------------------------------------
  --      ПАКЕТ ДЛЯ ГЛОБАЛЬНЫХ ВЫБОРОК ПО ОТЧЕТАМ
  ----------------------------------------------------------------------------------
  --
  --------------------------------------------------------------------------------
  --        ОСНОВНАЯ ВЫБОРКА ДЛЯ ОТЧЕТОВ ПО ВХОДЯЩИМ ЗВОНКАМ                    --
  --------------------------------------------------------------------------------  

    CURSOR cur_data_inc_call (
                                I_INIT_TIME TIMESTAMP
                              , I_FINISH_TIME TIMESTAMP
                              , I_GROUP      VARCHAR2 DEFAULT NULL
                              , I_STEP       NUMBER DEFAULT 1
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
                NVL2(
                      LOWER(I_GROUP),
                      CAST(TRUNC(I_INIT_TIME) AS TIMESTAMP),
                      I_INIT_TIME
                    ),
                I_FINISH_TIME, NVL(LOWER(I_GROUP), 'year'),
                DECODE(I_GROUP,'minute',15,1)
                ))
      ),      
    ALL_LEGS as
      (
        SELECT /*+ MATERIALIZE*/
          NCL.ID as CL_ID,
          NCL.SESSION_ID ,
          NCL.LEG_ID,
          NCL.SRC_ID ,
          NCL.DST_ID,
          NCL.SRC_ABONENT ,
          NCL.DST_ABONENT ,
          NCL.SRC_ABONENT_TYPE ,
          NCL.DST_ABONENT_TYPE ,
          NCL.CREATED ,
          NCL.CONNECTED ,
          NCL.ENDED ,
          NCL.VOIP_REASON ,
          NQC.ID,
          NQC.NEXT_LEG_ID ,
          NQC.UNBLOCKED_TIME ,
          NQC.DEQUEUED_TIME ,
          NQC.FIRST_LEG_ID ,
          NQC.PROJECT_ID PROJECT_ID,
          NQC.UNBLOCKED_TIME_DURATION ,
          NQC.ENQUEUED_TIME ,
          NQC.FINAL_STAGE ,
          NQC.IVR_LEG_ID,
          NVL2(NVL(CDPWT1.INIT_TIME,CDPWT2.INIT_TIME),1,0) AS CALL_IN_WORK_TIME,
          MAX(NCL.LEG_ID) over (partition by NCL.SESSION_ID, NCL.DST_ABONENT_TYPE) as MAX_LEG_ID
        FROM NAUCRM.CALL_LEGS NCL
        JOIN NAUCRM.QUEUED_CALLS NQC ON NQC.SESSION_ID=NCL.SESSION_ID --AND NQC.FIRST_LEG_ID=NCL.LEG_ID
        LEFT JOIN COMMON.D_PROJECT_PHONES CDPP ON NCL.DST_ID LIKE CDPP.PHONE
                                              AND CDPP.FID_PROJECTSADDINF_ID = 2905     
        LEFT JOIN COMMON.D_PROJECT_WORK_TIME CDPWT1 ON CDPWT1.FID_PROJECT_PHONES_ID = CDPP.ID
                                                    AND TO_NUMBER(TO_CHAR(NCL.CREATED,'d')) BETWEEN 1 AND 5
                                                    AND CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT1.INIT_TIME AND CDPWT1.FINAL_TIME
                                                    AND NCL.CREATED-CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT1.BEGIN_OPERATING_TIME_WEEKDAYS AND CDPWT1.END_OPERATING_TIME_WEEKDAYS
        LEFT JOIN COMMON.D_PROJECT_WORK_TIME CDPWT2 ON CDPWT2.FID_PROJECT_PHONES_ID = CDPP.ID
                                                    AND TO_NUMBER(TO_CHAR(NCL.CREATED,'d')) > 5
                                                    AND CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT2.INIT_TIME AND CDPWT2.FINAL_TIME
                                                    AND NCL.CREATED-CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT2.BEGIN_OPERATING_TIME_HOLIDAYS AND CDPWT2.END_OPERATING_TIME_HOLIDAYS
        WHERE NCL.CREATED BETWEEN I_INIT_TIME AND I_FINISH_TIME
          AND NQC.PROJECT_ID = 'project245'
      ),     
      
      CALLS_LIST AS
      (SELECT
        CL_ID,
        SESSION_ID ,
        LEG_ID,
        SRC_ID ,
        DST_ID,
        SRC_ABONENT ,
        DST_ABONENT ,
        SRC_ABONENT_TYPE ,
        DST_ABONENT_TYPE ,
        CREATED ,
        CONNECTED ,
        ENDED ,
        VOIP_REASON ,
        ID,
        NEXT_LEG_ID ,
        UNBLOCKED_TIME ,
        DEQUEUED_TIME ,
        FIRST_LEG_ID ,
        PROJECT_ID PROJECT_ID,
        UNBLOCKED_TIME_DURATION ,
        ENQUEUED_TIME ,
        FINAL_STAGE ,
        IVR_LEG_ID,
        CALL_IN_WORK_TIME,
        MAX_LEG_ID
      FROM
        ALL_LEGS
      WHERE
        FIRST_LEG_ID=LEG_ID
      ),
      H_NCL AS
      (
        SELECT /*+ MATERIALIZE*/ SESSION_ID, MAX(LEG_ID) AS LEG_ID
        FROM NAUCRM.CALL_LEGS
        WHERE DST_ABONENT_TYPE='SP'
          AND SESSION_ID IN (SELECT DISTINCT SESSION_ID FROM CALLS_LIST WHERE SESSION_ID IS NOT NULL)
          AND CREATED BETWEEN I_INIT_TIME AND I_FINISH_TIME
        GROUP BY SESSION_ID
      ),    
      
      NCP AS
      (
        SELECT /*+ MATERIALIZE*/ SESSION_ID,
          MIN(CHANGED) AS BUSY_IVR_START
        FROM NAUCRM.CALL_PARAMS
        WHERE UPPER(PARAM_NAME)='IVR'
          AND PARAM_VALUE LIKE '"Busy_"'
          AND SESSION_ID IN (SELECT DISTINCT SESSION_ID FROM CALLS_LIST WHERE SESSION_ID IS NOT NULL)
          AND CHANGED BETWEEN I_INIT_TIME AND I_FINISH_TIME + INTERVAL '30' MINUTE 
        GROUP BY SESSION_ID
      ),
      HNCS AS
      (
        SELECT /*+ MATERIALIZE*/ SESSION_ID,
          SUM(COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(ENDED-ENTERED)) AS HOLD_TIME
        FROM NAUCRM.CALL_STATUS
        WHERE STATE='hold'
          AND LENGTH(DESTINATION_ID)<16
          AND SESSION_ID IN (SELECT DISTINCT SESSION_ID FROM CALLS_LIST WHERE SESSION_ID IS NOT NULL)
          AND ENTERED BETWEEN I_INIT_TIME AND I_FINISH_TIME + INTERVAL '30' MINUTE
        GROUP BY SESSION_ID
      ),

      NSC AS
      (
        SELECT /*+ MATERIALIZE*/
          LOGIN, REASON, DURATION, ENTERED+NUMTODSINTERVAL(DURATION, 'second') AS WRAPUP_END_TIME
        FROM NAUCRM.STATUS_CHANGES
        WHERE ENTERED BETWEEN I_INIT_TIME AND I_FINISH_TIME
          AND STATUS = 'wrapup'
      ),
      
     CALLS_SECOND AS( --ДЛЯ ПЕРЕВЕДЕННЫХ ЗВОНКОВ
        SELECT DISTINCT
          IVR_NCL.LEG_ID AS FIRST_LEG_ID,
          OPR_NCL_2.LEG_ID,
          OPR_NCL_2.SESSION_ID ,
          OPR_NCL_2.SRC_ID ,
          OPR_NCL_2.DST_ID,
          OPR_NCL_2.SRC_ABONENT ,
          OPR_NCL_2.DST_ABONENT ,
          OPR_NCL_2.SRC_ABONENT_TYPE ,
          OPR_NCL_2.DST_ABONENT_TYPE ,
          OPR_NCL_2.CREATED ,
          OPR_NCL_2.CONNECTED ,
          OPR_NCL_2.ENDED ,
          IVR_NCL.VOIP_REASON,
          IVR_NCL.CONNECTED AS IVR_CONNECTED, --Время соединения с IVR
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(OPR_NCL_2.CONNECTED - IVR_NCL.CREATED ) AS BUSY_IVR_DUR --Длительность IVR ожидания (Для второй линии)
        FROM CALLS_LIST CL       
        JOIN NAUCRM.CALL_LEGS IVR_NCL_PREV ON IVR_NCL_PREV.SESSION_ID=CL.SESSION_ID AND IVR_NCL_PREV.LEG_ID=CL.NEXT_LEG_ID AND IVR_NCL_PREV.DST_ABONENT_TYPE='SP'
        JOIN NAUCRM.CALL_LEGS IVR_NCL ON IVR_NCL.SESSION_ID=IVR_NCL_PREV.SESSION_ID 
                                     AND IVR_NCL.SRC_ABONENT_TYPE = IVR_NCL_PREV.DST_ABONENT_TYPE
                                     AND IVR_NCL.SRC_ABONENT = IVR_NCL_PREV.DST_ABONENT                                                                              
        JOIN H_NCL ON H_NCL.SESSION_ID=CL.SESSION_ID 
        JOIN NAUCRM.CALL_LEGS OPR_NCL_2 ON OPR_NCL_2.SESSION_ID=CL.SESSION_ID 
                                       AND OPR_NCL_2.LEG_ID=H_NCL.LEG_ID
                                       AND OPR_NCL_2.DST_ABONENT_TYPE='SP'     
      ),
      
      ALL_LEGS_2 as  --project291
      (
        SELECT /*+ MATERIALIZE*/
          NCL.ID as CL_ID,
          NCL.SESSION_ID ,
          NCL.LEG_ID,
          NCL.SRC_ID ,
          NCL.DST_ID,
          NCL.SRC_ABONENT ,
          NCL.DST_ABONENT ,
          NCL.SRC_ABONENT_TYPE ,
          NCL.DST_ABONENT_TYPE ,
          NCL.CREATED ,
          NCL.CONNECTED ,
          NCL.ENDED ,
          NCL.VOIP_REASON ,
          NQC.ID,
          NQC.NEXT_LEG_ID ,
          NQC.UNBLOCKED_TIME ,
          NQC.DEQUEUED_TIME ,
          NQC.FIRST_LEG_ID ,
          NQC.PROJECT_ID PROJECT_ID,
          NQC.UNBLOCKED_TIME_DURATION ,
          NQC.ENQUEUED_TIME ,
          NQC.FINAL_STAGE ,
          NQC.IVR_LEG_ID,
          NVL2(NVL(CDPWT1.INIT_TIME,CDPWT2.INIT_TIME),1,0) AS CALL_IN_WORK_TIME,
          MAX(NCL.LEG_ID) over (partition by NCL.SESSION_ID, NCL.DST_ABONENT_TYPE) as MAX_LEG_ID
        FROM NAUCRM.CALL_LEGS NCL
        JOIN NAUCRM.QUEUED_CALLS NQC ON NQC.SESSION_ID=NCL.SESSION_ID --AND NQC.FIRST_LEG_ID=NCL.LEG_ID
        LEFT JOIN COMMON.D_PROJECT_PHONES CDPP ON NCL.DST_ID LIKE CDPP.PHONE
                                              AND CDPP.FID_PROJECTSADDINF_ID = 2905     
        LEFT JOIN COMMON.D_PROJECT_WORK_TIME CDPWT1 ON CDPWT1.FID_PROJECT_PHONES_ID = CDPP.ID
                                                    AND TO_NUMBER(TO_CHAR(NCL.CREATED,'d')) BETWEEN 1 AND 5
                                                    AND CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT1.INIT_TIME AND CDPWT1.FINAL_TIME
                                                    AND NCL.CREATED-CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT1.BEGIN_OPERATING_TIME_WEEKDAYS AND CDPWT1.END_OPERATING_TIME_WEEKDAYS
        LEFT JOIN COMMON.D_PROJECT_WORK_TIME CDPWT2 ON CDPWT2.FID_PROJECT_PHONES_ID = CDPP.ID
                                                    AND TO_NUMBER(TO_CHAR(NCL.CREATED,'d')) > 5
                                                    AND CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT2.INIT_TIME AND CDPWT2.FINAL_TIME
                                                    AND NCL.CREATED-CAST(TRUNC(NCL.CREATED) AS TIMESTAMP) BETWEEN CDPWT2.BEGIN_OPERATING_TIME_HOLIDAYS AND CDPWT2.END_OPERATING_TIME_HOLIDAYS
        WHERE NCL.CREATED BETWEEN I_INIT_TIME AND I_FINISH_TIME + INTERVAL '30' MINUTE
          AND NQC.PROJECT_ID = 'project291'

      ),     
      
      CALLS_LIST_2 AS --project291
      (SELECT DISTINCT
          NULL AS FIRST_LEG_ID,
          OPR_NCL.LEG_ID,
          OPR_NCL.SESSION_ID ,
          OPR_NCL.SRC_ID ,
          OPR_NCL.DST_ID,
          OPR_NCL.SRC_ABONENT ,
          OPR_NCL.DST_ABONENT ,
          OPR_NCL.SRC_ABONENT_TYPE ,
          OPR_NCL.DST_ABONENT_TYPE ,
          OPR_NCL.CREATED ,
          OPR_NCL.CONNECTED ,
          OPR_NCL.ENDED ,
          IVR_NCL.VOIP_REASON,
          IVR_NCL.CONNECTED AS IVR_CONNECTED, --Время соединения с IVR
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(OPR_NCL.CONNECTED - IVR_NCL.CREATED ) AS BUSY_IVR_DUR --Длительность IVR ожидания (Для второй линии)
      FROM ALL_LEGS_2 CL
      LEFT JOIN NAUCRM.CALL_LEGS IVR_NCL ON IVR_NCL.ID=CL.SESSION_ID || '_' || CL.IVR_LEG_ID
      LEFT JOIN H_NCL ON H_NCL.SESSION_ID=CL.SESSION_ID
      LEFT JOIN NAUCRM.CALL_LEGS OPR_NCL ON OPR_NCL.ID=CL.SESSION_ID  || '_' || NVL(CL.NEXT_LEG_ID,H_NCL.LEG_ID)
                                            AND OPR_NCL.DST_ABONENT_TYPE='SP'
      WHERE CL.FIRST_LEG_ID=CL.LEG_ID
        AND CL.FIRST_LEG_ID NOT IN (SELECT FIRST_LEG_ID FROM CALLS_SECOND)
      ),
            
    CALLS_SECOND_LINE AS(
      SELECT *
      FROM CALLS_SECOND
      UNION
      SELECT *
      FROM CALLS_LIST_2
      ),
    HNCS_SECOND AS --удержание для 2-й линии
      (
        SELECT /*+ MATERIALIZE*/ SESSION_ID,
          SUM(COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(ENDED-ENTERED)) AS HOLD_TIME
        FROM NAUCRM.CALL_STATUS
        WHERE STATE='hold'
          AND LENGTH(DESTINATION_ID)<16
          AND (SESSION_ID,INITIATOR_ID) IN (SELECT DISTINCT SESSION_ID,DST_ABONENT FROM CALLS_SECOND_LINE WHERE SESSION_ID IS NOT NULL)
        GROUP BY SESSION_ID
      ),

      
      RESULT AS
      (
        SELECT /*+ MATERIALIZE*/
          P.START_PERIOD,
          P.VIEW_PERIOD,
          CL.SESSION_ID,
          CL.SRC_ID AS CALLER, --Номер звонящего (абонента)
          CL.DST_ID AS DST_ID, -- Наш номер (номер call-центра)
          CL.CREATED AS CALL_CREATED, --Дата и время поступления звонка,
          CL.ENDED AS CALL_ENDED, --Дата и время окончания звонка,
          CL.VOIP_REASON, --Результат звонка
          OPR_NCL_2.VOIP_REASON AS VOIP_REASON_SECOND, --Результат звонка (FOR 2 - LINE)
          IVR_NCL.CREATED AS IVR_CREATED, --Время звонка на IVR
          IVR_NCL.CONNECTED AS IVR_CONNECTED, --Время соединения с IVR
          (CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN NULL
            ELSE OPR_NCL_2.IVR_CONNECTED
           END) AS IVR_CONNECTED_SECOND, --Время соединения с IVR (ДЛЯ ВТОРОЙ ЛИНИИ)
          IVR_NCL.ENDED AS IVR_ENDED, --Время окончания работы IVR
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(COALESCE(NCP.BUSY_IVR_START,OPR_NCL.CREATED,IVR_NCL.ENDED)-IVR_NCL.CONNECTED) AS WELCOME_IVR_DUR, --Длительность IVR приветствия
          NCP.BUSY_IVR_START, --Время IVR ожидания       
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(NVL(OPR_NCL.CREATED,IVR_NCL.ENDED)-NCP.BUSY_IVR_START)+
          nvl((CASE 
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE OPR_NCL_2.BUSY_IVR_DUR
           END),0)  AS BUSY_IVR_DUR, --Длительность IVR ожидания
           
          nvl((CASE 
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE OPR_NCL_2.BUSY_IVR_DUR
           END),0) AS BUSY_IVR_DUR_SECOND, --Длительность IVR ожидания (ДЛЯ ВТОРОЙ ЛИНИИ)
           
          CL.UNBLOCKED_TIME_DURATION, --Длительность после разблокировки на оператора
          OPR_NCL.CREATED AS OPR_CREATED, --Время перевода на оператора
          OPR_NCL.CONNECTED AS OPR_CONNECTED, --Время соединения с оператором
          OPR_NCL_2.CREATED AS OPR_CREATED_SECOND, --Время перевода на оператора (ДЛЯ 2-Й ЛИНИИ)
          OPR_NCL_2.CONNECTED AS OPR_CONNECTED_SECOND, --Время соединения с оператором (ДЛЯ 2-Й ЛИНИИ)      
          OPR_NCL_2.ENDED AS OPR_ENDED_SECOND, --Время отключения от оператора (ДЛЯ 2-Й ЛИНИИ)  
          OPR_NCL.ENDED AS OPR_ENDED, --Время отключения от оператора
          NVL(OPR_NCL.DST_ABONENT, OPR_NCL.DST_ID) AS OPR_LOGIN, --Логин принявшего звонок оператора
          NVL(OPR_NCL_2.DST_ABONENT, OPR_NCL_2.DST_ID) AS OPR_LOGIN_SECOND, --Логин принявшего звонок оператора (Для второй линии)
          
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(NVL(OPR_NCL.CONNECTED,OPR_NCL.ENDED)-OPR_NCL.CREATED) + 
          nvl((CASE 
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(NVL(OPR_NCL_2.CONNECTED,OPR_NCL_2.ENDED)-OPR_NCL_2.CREATED)
           END),0)  AS RINGING_DUR, --Длительность вызова оператора
           nvl((CASE 
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(NVL(OPR_NCL_2.CONNECTED,OPR_NCL_2.ENDED)-OPR_NCL_2.CREATED)
           END),0)  AS RINGING_DUR_SECOND, --Длительность вызова оператора (ДЛЯ ВТОРОЙ ЛИНИИ)
           
          COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(OPR_NCL.ENDED-OPR_NCL.CONNECTED) + 
          nvl((CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(OPR_NCL_2.ENDED-OPR_NCL_2.CONNECTED) 
           END),0) AS WORK_DUR, --Длительность работы оператора с клиентом 
           
           nvl((CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE COMMON_V2.PKG_INTERVAL_UTILS.FNC_INTERVALTOSEC(OPR_NCL_2.ENDED-OPR_NCL_2.CONNECTED) 
           END),0) AS WORK_DUR_SECOND, --Длительность работы оператора с клиентом (ДЛЯ ВТОРОЙ ЛИНИИ)
           
          HNCS.HOLD_TIME AS HOLD_DUR, --Длительность удержания
          nvl((CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE HNCS_SECOND.HOLD_TIME 
           END),0) AS HOLD_DUR_SECOND, --Длительность удержания (ДЛЯ ВТОРОЙ ЛИНИИ)
          
          COALESCE( NSC_2.WRAPUP_END_TIME,NSC.WRAPUP_END_TIME) AS WRAPUP_END_TIME, --Время завершения поствызывной обработки--С УЧЕТОМ 2-Й ЛИНИИ
          NVL(NSC.DURATION,0)+
          (CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE  NVL(NSC_2.DURATION,0)
           END) AS WRAPUP_DUR, --Длительность поствызывной обработки
           
           (CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE  NVL(NSC_2.DURATION,0)
           END) AS WRAPUP_DUR_SECOND, --Длительность поствызывной обработки (ДЛЯ ВТОРОЙ ЛИНИИ)
           
          CL.CALL_IN_WORK_TIME, --Звонок в рабочее время проекта
          
          (CASE
            WHEN OPR_NCL.LEG_ID = OPR_NCL_2.LEG_ID or OPR_NCL_2.LEG_ID is null
            THEN 0
            ELSE 1 
           END) AS SECOND_LINE --БЫЛА ЛИ ВТОРАЯ ЛИНИЯ 1 - ДА, 0 - НЕТ
        FROM CALLS_LIST CL
        LEFT JOIN NAUCRM.CALL_LEGS IVR_NCL ON IVR_NCL.ID=CL.SESSION_ID || '_' || CL.IVR_LEG_ID
        LEFT JOIN NCP ON NCP.SESSION_ID=CL.SESSION_ID
        LEFT JOIN H_NCL ON H_NCL.SESSION_ID=CL.SESSION_ID
        LEFT JOIN NAUCRM.CALL_LEGS OPR_NCL ON OPR_NCL.ID=CL.SESSION_ID  || '_' || NVL(CL.NEXT_LEG_ID,H_NCL.LEG_ID)
                                              AND OPR_NCL.DST_ABONENT_TYPE='SP'
        LEFT JOIN CALLS_SECOND_LINE OPR_NCL_2 ON OPR_NCL_2.SESSION_ID=CL.SESSION_ID
       
        LEFT JOIN HNCS ON HNCS.SESSION_ID=CL.SESSION_ID
        LEFT JOIN HNCS_SECOND ON HNCS_SECOND.SESSION_ID=OPR_NCL_2.SESSION_ID
        LEFT JOIN NSC ON NSC.LOGIN=NVL(OPR_NCL.DST_ABONENT, OPR_NCL.DST_ID) AND NSC.REASON=CL.SESSION_ID
        LEFT JOIN NSC NSC_2 ON NSC_2.LOGIN=NVL(OPR_NCL_2.DST_ABONENT, OPR_NCL_2.DST_ID) AND NSC_2.REASON=OPR_NCL_2.SESSION_ID
        LEFT JOIN PERIODS P ON CL.CREATED BETWEEN P.START_PERIOD AND P.STOP_PERIOD
      ),
      MAIN_REPORT AS
      (
        SELECT
          RP.START_PERIOD,
          RP.VIEW_PERIOD,
          RP.SESSION_ID, --Запись разговора
          RP.CALLER, --Номер абонента
          RP.DST_ID, --Наш номер (номер call-центра)
          CALL_CREATED, --Время поступления вызова
          CASE
            WHEN CALL_IN_WORK_TIME = 0 THEN 'Завершен в IVR'
            WHEN RP.VOIP_REASON NOT IN(200, 201) THEN 'Блокирован'
            WHEN RP.BUSY_IVR_START IS NOT NULL
              OR OPR_CREATED IS NOT NULL THEN 'Распределен в очередь'
            WHEN IVR_CONNECTED IS NOT NULL THEN 'Завершен в IVR'
          END AS CONNECT_RESULT, --Результат соединения
          
          CASE
            WHEN CALL_IN_WORK_TIME = 0 THEN 3
            WHEN RP.VOIP_REASON NOT IN(200, 201) THEN 1
            WHEN RP.BUSY_IVR_START IS NOT NULL
              OR OPR_CREATED IS NOT NULL THEN 2
            WHEN IVR_CONNECTED IS NOT NULL THEN 4
          END AS CONNECT_RESULT_NUM, --Результат соединения код
          
          CASE
            WHEN CALL_IN_WORK_TIME = 0 THEN 3
            WHEN RP.VOIP_REASON_SECOND NOT IN(200, 201) THEN 1
            WHEN OPR_CREATED_SECOND IS NOT NULL THEN 2
            WHEN IVR_CONNECTED_SECOND IS NOT NULL THEN 4
          END AS CONNECT_RESULT_NUM_SECOND, --Результат соединения код (ДЛЯ ВТОРОЙ ЛИНИИ)
          
          NVL(RP.WELCOME_IVR_DUR,0) AS WELCOME_DUR, --Время в IVR, сек
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1
            THEN NVL(RP.BUSY_IVR_DUR,0)
          END AS BUSY_DUR, --Время ожидания в очереди (сек)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1
            THEN NVL(RP.BUSY_IVR_DUR_SECOND,0)
          END AS BUSY_DUR_SECOND, --Время ожидания в очереди (сек) (ДЛЯ ВТОРОЙ ЛИНИИ)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CONNECTED IS NOT NULL THEN 'Вызов принят оператором'
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CREATED IS NOT NULL
              AND RP.OPR_CONNECTED IS NULL THEN 'Потерян в ringing'
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.BUSY_IVR_START IS NOT NULL THEN 'Потерян в очереди'
          END AS CALL_RESULT, --Результат звонка

         CASE
            WHEN SECOND_LINE = 0 THEN ''
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.IVR_CONNECTED_SECOND IS NOT NULL THEN 'Вызов принят оператором'
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CREATED_SECOND IS NOT NULL
              AND RP.OPR_CONNECTED_SECOND IS NULL THEN 'Потерян в ringing'
            WHEN CALL_IN_WORK_TIME = 1 THEN 'Потерян в очереди'
          END AS CALL_RESULT_SECOND,
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CONNECTED IS NOT NULL THEN 1
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CREATED IS NOT NULL
              AND RP.OPR_CONNECTED IS NULL THEN 2
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.BUSY_IVR_START IS NOT NULL THEN 3
          END AS CALL_RESULT_NUM, --Результат звонка код
          
         CASE
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.IVR_CONNECTED_SECOND IS NOT NULL THEN 1
            WHEN CALL_IN_WORK_TIME = 1
              AND RP.OPR_CREATED_SECOND IS NOT NULL
              AND RP.OPR_CONNECTED_SECOND IS NULL THEN 2
            WHEN CALL_IN_WORK_TIME = 1 THEN 3
          END AS CALL_RESULT_NUM_SECOND, --Результат звонка код (ДЛЯ ВТОРОЙ ЛИНИИ)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN RP.OPR_CREATED
          END AS OPR_CREATED, --Время распределения на оператора
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN RP.OPR_CREATED_SECOND
          END AS OPR_CREATED_SECOND, --Время распределения на оператора (ДЛЯ ВТОРОЙ ЛИНИИ)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.RINGING_DUR,0),NULL)
          END AS RINGING_DUR, --Время реакции на вызов(сек)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED_SECOND,NVL(RP.RINGING_DUR_SECOND,0),NULL)
          END AS RINGING_DUR_SECOND, --Время реакции на вызов(сек) (Для второй линии)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.WORK_DUR,0) - NVL(RP.HOLD_DUR,0),NULL)
          END AS TALK_DUR, --Время разговора оператора(сек)
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED_SECOND,NVL(RP.WORK_DUR_SECOND,0) - NVL(RP.HOLD_DUR_SECOND,0),NULL)
          END AS TALK_DUR_SECOND, --Время разговора оператора(сек)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.HOLD_DUR,0),NULL)
          END AS HOLD_DUR, --Время удержания вызова(сек)
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED_SECOND,NVL(RP.HOLD_DUR_SECOND,0),NULL)
          END AS HOLD_DUR_SECOND, --Время удержания вызова(сек)
          
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.WRAPUP_DUR,0),NULL)
          END AS WRAPUP_DUR, --Поствызывная обработка
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED_SECOND,NVL(RP.WRAPUP_DUR_SECOND,0),NULL)
          END AS WRAPUP_DUR_SECOND, --Поствызывная обработка (ДЛЯ 2-Й ЛИНИИ)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.RINGING_DUR,0) + NVL(RP.WORK_DUR,0) + NVL(RP.WRAPUP_DUR,0),NULL)
          END AS SERVISE_CALL_DUR, --Время обслуживания звонка, сек
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,NVL(RP.RINGING_DUR_SECOND,0) + NVL(RP.WORK_DUR_SECOND,0) + NVL(RP.WRAPUP_DUR_SECOND,0),NULL)
          END AS SERVISE_CALL_DUR_SECOND, --Время обслуживания звонка, сек   (ДЛЯ 2-Й ЛИНИИ)
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,CEIL((NVL(RP.RINGING_DUR,0) + NVL(RP.WORK_DUR,0) + NVL(RP.WRAPUP_DUR,0))/60),NULL)
          END AS SERVISE_CALL_DUR2, --Округлённая длительность обработки вызова, мин
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN NVL2(RP.OPR_CREATED,CEIL((NVL(RP.RINGING_DUR_SECOND,0) + NVL(RP.WORK_DUR_SECOND,0) + NVL(RP.WRAPUP_DUR_SECOND,0))/60),NULL)
          END AS SERVISE_CALL_DUR2_SECOND, --Округлённая длительность обработки вызова, мин
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN RP.OPR_LOGIN
          END AS OPR_LOGIN, --Логин оператора
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN RP.OPR_LOGIN_SECOND
          END AS OPR_LOGIN_SECOND, --Логин оператора для второй линии
          
          
          CASE
            WHEN CALL_IN_WORK_TIME = 1 THEN COALESCE(RP.WRAPUP_END_TIME, RP.CALL_ENDED, RP.OPR_ENDED_SECOND)
          END AS COMPLET_CALL_TIME, --Время завершения вызова
          CASE
            WHEN RP.VOIP_REASON=201 THEN 'Абонент'
            WHEN RP.OPR_CONNECTED IS NOT NULL
              AND RP.VOIP_REASON=200 THEN 'Оператор'
            WHEN RP.OPR_CREATED IS NULL
              AND RP.BUSY_IVR_START IS NOT NULL THEN 'Истекло время нахождения в очереди'
            ELSE 'Система'
          END AS SEP_INIT, --Инициатор разъединения          
          
          CALL_IN_WORK_TIME, --(1 - в рабочее время, 0 - НЕТ)
          SECOND_LINE --БЫЛА ЛИ ВТОРАЯ ЛИНИЯ 1 - ДА, 0 - НЕТ
        FROM RESULT RP
      )

    SELECT MR.START_PERIOD, MR.VIEW_PERIOD, MR.SESSION_ID, MR.CALLER,TO_CHAR(MR.DST_ID) AS DST_ID, MR.CALL_CREATED, MR.CONNECT_RESULT,
      MR.CONNECT_RESULT_NUM,DECODE(MR.SECOND_LINE,1,MR.CONNECT_RESULT_NUM_SECOND,NULL) AS CONNECT_RESULT_NUM_SECOND, MR.WELCOME_DUR,
      MR.BUSY_DUR,MR.BUSY_DUR_SECOND, MR.CALL_RESULT, MR.CALL_RESULT_NUM, 
      DECODE(MR.SECOND_LINE,1,MR.CALL_RESULT_NUM_SECOND,NULL) AS CALL_RESULT_NUM_SECOND,
      MR.OPR_CREATED, MR.RINGING_DUR, MR.TALK_DUR, MR.TALK_DUR_SECOND, MR.HOLD_DUR, MR.HOLD_DUR_SECOND, 
      MR.WRAPUP_DUR,MR.WRAPUP_DUR_SECOND, MR.SERVISE_CALL_DUR,MR.SERVISE_CALL_DUR_SECOND, 
      MR.SERVISE_CALL_DUR2, MR.SERVISE_CALL_DUR2_SECOND , MR.OPR_LOGIN, MR.COMPLET_CALL_TIME, MR.SEP_INIT,
      MR.CALL_IN_WORK_TIME, MR.SECOND_LINE, MR.OPR_LOGIN_SECOND AS OPR_LOGIN_SECOND,
      MR.CALL_RESULT_SECOND, MR.OPR_CREATED_SECOND, MR.RINGING_DUR_SECOND
    FROM MAIN_REPORT MR

    where MR.SESSION_ID not in (select distinct SESSION_ID from BLOCK_SESSIONS)
--    where MR.SESSION_ID not in
--    (
--'nauss4_1449651951_578_2147720',
--'nauss5_1449652865_43_31452',
--'nauss8_1449653506_665_2217470',
--'nauss8_1449653812_496_2218550',
--'nauss4_1449655043_293_2156074',
--'nauss4_1449655220_740_2156658',
--'nauss7_1449655249_64_2225890',
--'nauss6_1449655601_464_99260',
--'nauss2_1449656786_854_375458',
--'nauss5_1449657321_896_44134',
--'nauss_1449659048_927_386682',
--'nauss5_1449659694_471_50256',
--'nauss7_1449659756_286_2237204',
--'nauss5_1449660008_929_51144',
--'nauss3_1449663137_748_176906',
--'nauss5_1449663296_120_61096',
--'nauss2_1449663569_629_388636',
--'nauss3_1449667038_781_188448',
--'nauss5_1449667298_725_73332',
--'nauss8_1449667388_243_2255844',
--'nauss3_1449667823_896_190790',
--'nauss3_1449781387_525_358434',
--'nauss5_1449781475_584_442',
--'nauss2_1449794594_85_127972',
--'nauss8_1449815096_558_2425442',
--'nauss4_1449819122_77_2368384',
--'nauss2_1449819608_116_142248',
--'nauss4_1449821386_908_2372918',
--'nauss7_1449821628_862_2440552',
--'nauss2_1449822306_67_148022',
--'nauss_1449823002_415_149798',
--'nauss8_1449824213_834_2444288',
--'nauss6_1449824637_931_17384',
--'nauss3_1449826758_776_390106',
--'nauss7_1449834009_292_2468080',
--'nauss7_1449835448_104_2471346',
--'nauss8_1449836133_189_2471350',
--'nauss3_1449837973_251_415650',
--'nauss7_1449839008_990_2479586',
--'nauss3_1449727389_799_238542',
--'nauss_1449727880_666_6034',
--'nauss2_1449728415_947_6936',
--'nauss7_1449729112_378_2307340',
--'nauss4_1449730611_616_2244084',
--'nauss8_1449730762_429_2308380',
--'nauss3_1449731007_532_246002',
--'nauss3_1449731045_617_246102',
--'nauss4_1449731665_122_2247050',
--'nauss8_1449731874_748_2311552',
--'nauss4_1449732277_859_2248708',
--'nauss2_1449732849_669_18306',
--'nauss_1449733031_794_19130',
--'nauss2_1449733192_739_19326',
--'nauss3_1449733391_80_253806',
--'nauss2_1449736050_335_28920',
--'nauss8_1449740472_653_2339262',
--'nauss_1449740566_39_43720',
--'nauss4_1449741892_984_2278276',
--'nauss4_1449742380_496_2279806',
--'nauss8_1449743591_42_2349218',
--'nauss8_1449743611_596_2349294',
--'nauss8_1449744330_831_2351518',
--'nauss2_1449744675_957_55938',
--'nauss_1449746272_475_61140',
--'nauss3_1449746309_103_295168',
--'nauss3_1449746564_913_295972',
--'nauss3_1449747958_300_300326',
--'nauss2_1449748673_917_68290',
--'nauss8_1449750795_554_2370992',
--'nauss3_1449750819_254_307922',
--'nauss3_1449751405_37_309474',
--'nauss_1449754506_625_85106',
--'nauss2_1449756790_827_91004',
--'nauss7_1449758787_118_2391708',
--'nauss3_1449760706_166_332630')
    ORDER BY CALL_CREATED;
    
    
   TYPE t_data_inc_call IS TABLE OF cur_data_inc_call%rowtype;

FUNCTION fnc_data_inc_call (
                              I_INIT_TIME TIMESTAMP
                            , I_FINISH_TIME TIMESTAMP
                            , I_GROUP      VARCHAR2 DEFAULT NULL
                            , I_STEP       NUMBER DEFAULT 1
                        ) RETURN t_data_inc_call pipelined; 
                        

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

END PKG_GENERAL_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_GENERAL_REPORTS AS

  --------------------------------------------------------------------------------
  --        ОСНОВНАЯ ВЫБОРКА ДЛЯ ОТЧЕТОВ ПО ВХОДЯЩИМ ЗВОНКАМ                    --
  --------------------------------------------------------------------------------
  FUNCTION fnc_data_inc_call
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_GROUP      VARCHAR2 DEFAULT NULL
    , I_STEP       NUMBER DEFAULT 1
) RETURN t_data_inc_call pipelined AS
   BEGIN
   FOR L IN cur_data_inc_call(I_INIT_TIME, I_FINISH_TIME, I_GROUP, I_STEP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_data_inc_call;


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


  ---------------------------------------------------------------
  -- Выбока звонков для пакета PKG_ACSI_REPORTS
  --------------------------------------------------------------

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

END PKG_GENERAL_REPORTS;
/
