CREATE OR REPLACE PACKAGE PKG_WIKI_REPORTS AS 
    --                                                            --
    --  Отчетность для статистики по используемым шаблонам        --
    --  Заявка ZHKKH-726                                          --
    --
-------------------------------------------------------
--  Детализированный отчет по используемым шаблонам
-------------------------------------------------------

CURSOR cur_wiki_answers_log (
        I_INIT_TIME TIMESTAMP
        , I_FINISH_TIME TIMESTAMP
        , I_REGION NUMBER
        , I_COMPANY_TYPE NUMBER
        , I_ADMIN_TYPE VARCHAR2 := NULL
        , I_CHANNEL VARCHAR2 := NULL
        , I_TYPE NUMBER DEFAULT NULL
)
IS
WITH 
GIS_ZHKH AS (
        SELECT * FROM DUAL
)
--------------------------------------------------
--WIEWS ДЛЯ КАНАЛА ГОЛОС
---------------------------------------------------
, ALL_CALLS_TYPES AS ( --Выбранные типы при ответе на вопросы --ZHKKH-917
          SELECT 
                  CL.ID_CALL
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
          GROUP BY CL.ID_CALL 
)
, IND_FIRST_LINE AS (
        SELECT 
                  SESSION_ID,
                  MAX(CASE WHEN FID_RESULT !=9 THEN ID_CALL END) AS ID_CALL_BOTH_LINE,
                  MAX(CASE WHEN FID_RESULT =9 THEN ID_CALL END) AS ID_CALL_FIRST_LINE
        FROM CORE_CALLS
        WHERE CREATED_AT >= I_INIT_TIME AND CREATED_AT < I_FINISH_TIME
        AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)
        GROUP BY SESSION_ID  
)
, MODIFY_CORE_CALLS AS (
        SELECT 
        CL.*,
        (CASE
                WHEN ID_CALL_FIRST_LINE IS NULL
                THEN '1-я линия'
                ELSE '2-я линия'
        END) AS LINE
        FROM CORE_CALLS CL
                JOIN IND_FIRST_LINE IND
                    ON IND.ID_CALL_BOTH_LINE = CL.ID_CALL
        UNION
        SELECT 
                CL.*,
                '1-я линия' AS LINE
        FROM CORE_CALLS CL
                JOIN IND_FIRST_LINE IND
                    ON IND.ID_CALL_FIRST_LINE = CL.ID_CALL
)
--------------------------------------------------
--WIEWS ДЛЯ КАНАЛА e-mail
---------------------------------------------------
, ALL_NEW_TYPES AS ( --Пригодится, если появится TICKETS.FID_TYPE --ZHKKH-917
        SELECT 
                TDT_LEV_2.ID_TYPE,--ID ДЛЯ СВЯЗИ С БУДУЩИМ TICKETS.FID_TYPE
                TDT_LEV_1.NAME AS CLASSIFIER_NEW_LEV_1,
                TDT_LEV_2.NAME AS CLASSIFIER_NEW_LEV_2
        FROM TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
                JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
                    ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT
                    AND TDT_LEV_1.IS_ACTIVE = 1
                    AND TDT_LEV_2.IS_ACTIVE = 1 
)
, ALL_TICKETS_TYPES AS (--КлассификаторЫ И ПОЛНОМОЧИЯ
        SELECT
                ID_TICKET
                , LISTAGG(TYPE_NAME,',  ') WITHIN GROUP (order by ID_TYPE_HAS) AS SELECTED_TYPES 
                , MAX(CLASSIFIER_NEW_LEV_1) AS CLASSIFIER_NEW_LEV_1  --Классификатор-- 1 LEVEL --ZHKKH-917
                , MAX(CLASSIFIER_NEW_LEV_2) AS CLASSIFIER_NEW_LEV_2  --Классификатор-- 2 LEVEL --ZHKKH-917
                , MAX(ADMIN_TYPE) AS ADMIN_TYPE  --Административный тип  --ZHKKH-917
                
                , MAX(TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_TYPE_HAS) AS LAST_TYPE
                , MAX(COMPANY_TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_COMPANY_TYPE
                , MAX(ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_ID_COMPANY_TYPE
        FROM ( 
                SELECT DISTINCT
                        TCK.ID_TICKET
                        , TDT.NAME AS TYPE_NAME
                        , TDT_LEV_1.NAME AS CLASSIFIER_NEW_LEV_1 --Классификатор-- 1 LEVEL --ZHKKH-917
                        , TDT_LEV_2.NAME AS CLASSIFIER_NEW_LEV_2 --Классификатор-- 2 LEVEL --ZHKKH-917
                        , TTP.ID_HAS AS ID_TYPE_HAS
                        , DCTP.ID_COMPANY_TYPE
                        , COALESCE(DCTP.SHORT_NAME, DCTP.NAME) AS COMPANY_TYPE_NAME
                        , CTP.ID_HAS AS ID_COMPANY_TYPE_HAS
                        , ADT.NAME AS ADMIN_TYPE --Административный тип  --ZHKKH-917
                FROM USER_ACTION_RELATIONS ACR
                        JOIN USER_ACTIONS_LOG ALC
                            ON ALC.ID_ACTION = ACR.FID_ACTION
                        JOIN WIKI_ANSWER WAN
                            ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
                        JOIN TICKETS TCK
                            ON TCK.ID_TICKET = ALC.LOGGABLE_ID
                        JOIN TICKETS_HAS_TYPES TTP
                            ON TTP.FID_TICKET = TCK.ID_TICKET
                        LEFT JOIN TICKETS_D_TYPES TDT
                            ON TDT.ID_TYPE = TTP.FID_TYPE 
                            AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
                        LEFT JOIN TICKETS_D_TYPES_TEST TDT_LEV_2  --MUST JOIN
                            ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE 
                            AND TDT_LEV_2.IS_ACTIVE = 1
                        LEFT JOIN TICKETS_D_TYPES_TEST TDT_LEV_1  --MUST JOIN
                            ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT 
                            AND TDT_LEV_1.IS_ACTIVE = 1 
                        LEFT JOIN TICKETS_HAS_CMP_TPS CTP
                            ON CTP.FID_TICKET = TCK.ID_TICKET
                        LEFT JOIN TICKETS_D_COMPANY_TYPES DCTP
                            ON DCTP.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
                        LEFT JOIN TICKETS_D_ADM_TYPES ADT--ZHKKH-917 
                            ON ADT.ID_TYPE = TCK.FID_ADM_TYPE 
                WHERE ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                AND ALC.LOGGABLE_TYPE = 'TICKETS'
                AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL) 
        )
        GROUP BY ID_TICKET
)
, ALL_TICKETS_TASKS AS (--достает список задач в JIRA контакта
        SELECT 
        ID_TICKET,
        LISTAGG(TASK_CODE,',  ') WITHIN GROUP (order by ID_TASK) AS TICKETS_TASKS
        FROM (
                SELECT DISTINCT
                        TCK.ID_TICKET
                        , TTS.TASK_CODE 
                        , TTS.ID_TASK
                FROM USER_ACTION_RELATIONS ACR
                        JOIN USER_ACTIONS_LOG ALC
                            ON ALC.ID_ACTION = ACR.FID_ACTION
                        JOIN WIKI_ANSWER WAN
                            ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
                        JOIN TICKETS TCK
                            ON TCK.ID_TICKET = ALC.LOGGABLE_ID
                        JOIN TICKETS_TASKS TTS
                            ON TTS.FID_TICKET = TCK.ID_TICKET
                WHERE 
                        ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                        AND ALC.LOGGABLE_TYPE = 'TICKETS'
                        AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                        AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL)
        )
        GROUP BY ID_TICKET
)
--  SELECT * FROM DUAL;
, GET_WIKI_SUBSTANCES_CALLS AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА ГОЛОС
        SELECT DISTINCT -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
                CL.CREATED_AT --Дата и время
                , TO_CHAR(CL.CREATED_AT,'dd.mm.yyyy hh24:mi:ss') as CREATED_TIME  
                , CL.ID_CALL AS ID_ACTION --№ обращения / ID звонка
                , 'Голос' AS CHANNEL --Канал
                , CL.LINE --Линия
                , CL.OPERATOR_LOGIN --Operator
                , '' AS TICKETS_TASKS  --№ заявок в JIRA
                , RES.NAME AS STATUS_NAME--Статус
                , COALESCE(CTP.SHORT_NAME, CTP.NAME,'Не определено') AS COMPANY_TYPE --Полномочие
                , COALESCE(CLTP.TYPE_NAME,TTP.NAME,'Тип не указан') AS TICKET_TYPE--Тип обращения--ZHKKH-917
                , NVL(CLTP.TYPE_NAME_LEVEL_2,'Тип не указан') AS TICKET_TYPE_LEVEL_2 --Тип обращения 2 LEVEL--ZHKKH-917
                , NVL(CLTP.ADMIN_TYPE,'Тип не указан') AS ADMIN_TYPE --Административный тип--ZHKKH-917
                , NVL(TRG.NAME,'Регион не указан') AS REGION_NAME--Регион
                , WCL.CLASSIFICATOR_NAME --Шаблон ответа_Классификатор     
                , WSC.SUBSECTION_NAME --Шаблон ответа_Подраздел
                , replace(WSB.SUBSTANCE_NAME,CHR(34),'') AS SUBSTANCE_NAME --Шаблон ответа_Суть вопроса
                , WAN.INFORMATION_SOURCE --Ссылка на шаблон wiki
        FROM CALLS_WIKI_ANSWERS CWA
                JOIN MODIFY_CORE_CALLS CL
                    ON CL.ID_CALL = CWA.FID_CALL
                JOIN WIKI_ANSWER WAN
                    ON WAN.ID_ANSWER = CWA.FID_ANSWER
                JOIN WIKI_D_CLASSIFICATOR WCL
                    ON WCL.ID_CLASSIFICATOR = WAN.FID_CLASSIFICATOR
                LEFT JOIN WIKI_D_SUBSECTION WSC
                    ON WSC.ID_SUBSECTION = WAN.FID_SUBSECTION
                JOIN WIKI_D_SUBSTANCE WSB
                    ON WSB.ID_SUBSTANCE = WAN.FID_SUBSTANCE
                LEFT JOIN CORE_CALLS_RESULTS RES
                    ON RES.ID_RESULT = CL.FID_RESULT
                LEFT JOIN INC_CALL_CONTACT_DATA INC
                    ON INC.FID_CALL = CL.ID_CALL
                LEFT JOIN TICKETS_D_COMPANY_TYPES CTP
                    ON CTP.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE
                LEFT JOIN TICKETS_D_TYPES TTP 
                    ON TTP.ID_TYPE = INC.FID_COMPANY_TYPE and ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
                LEFT JOIN TICKETS_D_REGIONS TRG
                    ON TRG.ID_REGION = INC.FID_COMPANY_REGION
                LEFT JOIN ALL_CALLS_TYPES CLTP --ZHKKH-917
                    ON CLTP.ID_CALL = CL.ID_CALL
        WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
        AND (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
        AND (NVL(CTP.ID_COMPANY_TYPE,1000) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
        AND (CLTP.ADMIN_TYPE LIKE '%'|| I_ADMIN_TYPE || '%' OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND CLTP.ADMIN_TYPE IS NULL))--ZHKKH-917
        AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)
)   
 , PREV_WIKI_SUBSTANCES_E_MAIL AS ( -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
        SELECT          
            MAX(ACR.ID_RELATION) AS ID_RELATION
          , ACR.RELATIONABLE_ID
          , TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi') as CREATED_TIME

        FROM USER_ACTION_RELATIONS ACR
                JOIN USER_ACTIONS_LOG ALC
                    ON ALC.ID_ACTION = ACR.FID_ACTION                
        WHERE 
                ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                AND ALC.LOGGABLE_TYPE = 'TICKETS'
                AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                
        GROUP BY ACR.RELATIONABLE_ID, TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi')
      )  
, GET_WIKI_SUBSTANCES_E_MAIL AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА E-MAIL
        SELECT  
                  ACR.CREATED_AT AS CREATED_AT --Дата и время
                , TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi:ss') as CREATED_TIME
                , TCK.ID_TICKET AS ID_ACTION --№ обращения / ID звонка
                , 'E-mail' AS CHANNEL --Канал
                , '' AS LINE--Линия
                , US.LOGIN --Operator
                , TTS.TICKETS_TASKS  --№ заявок в JIRA
                , TST.NAME AS STATUS_NAME--Статус
                , NVL(TTP.LAST_COMPANY_TYPE,'Не определено') AS COMPANY_TYPE --Полномочие
                , COALESCE(TTP.CLASSIFIER_NEW_LEV_1,/**/TTP.SELECTED_TYPES,'Тип не указан') AS TICKET_TYPE--Тип обращения--ZHKKH-917
                , NVL(TTP.CLASSIFIER_NEW_LEV_2, 'Тип не указан') AS TICKET_TYPE_LEVEL_2--Тип обращения 2 LEVEL--ZHKKH-917
                , NVL(TTP.ADMIN_TYPE, 'Тип не указан') AS ADMIN_TYPE --АДМИНИСТРАТИВНЫЙ ТИП--ZHKKH-917
                , NVL(TRG.NAME,'Регион не указан') AS REGION_NAME--Регион
                , WCL.CLASSIFICATOR_NAME --Шаблон ответа_Классификатор     
                , WSC.SUBSECTION_NAME --Шаблон ответа_Подраздел
                , replace(WSB.SUBSTANCE_NAME,CHR(34),'') AS SUBSTANCE_NAME --Шаблон ответа_Суть вопроса
                , WAN.INFORMATION_SOURCE --Ссылка на шаблон wiki
        FROM PREV_WIKI_SUBSTANCES_E_MAIL PREV
        JOIN USER_ACTION_RELATIONS ACR
         ON ACR.ID_RELATION = PREV.ID_RELATION
        JOIN USER_ACTIONS_LOG ALC
            ON ALC.ID_ACTION = ACR.FID_ACTION
        JOIN WIKI_ANSWER WAN
            ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
        JOIN WIKI_D_CLASSIFICATOR WCL
            ON WCL.ID_CLASSIFICATOR = WAN.FID_CLASSIFICATOR
        LEFT JOIN WIKI_D_SUBSECTION WSC
            ON WSC.ID_SUBSECTION = WAN.FID_SUBSECTION
        JOIN WIKI_D_SUBSTANCE WSB
            ON WSB.ID_SUBSTANCE = WAN.FID_SUBSTANCE
        JOIN TICKETS TCK
            ON TCK.ID_TICKET = ALC.LOGGABLE_ID
        LEFT JOIN TICKETS_D_STATUSES TST
            ON TST.ID_STATUS = TCK.FID_STATUS
        LEFT JOIN TICKETS_D_REGIONS TRG
            ON TRG.ID_REGION = TCK.FID_COMPANY_REGION
        LEFT JOIN CIS.NC_USERS US
            ON US.ID_USER = ALC.FID_USER
        LEFT JOIN ALL_TICKETS_TYPES TTP
            ON TTP.ID_TICKET = TCK.ID_TICKET
        LEFT JOIN ALL_TICKETS_TASKS TTS
            ON TTS.ID_TICKET = TCK.ID_TICKET
        
        WHERE 
                    (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
                AND (NVL(TTP.LAST_ID_COMPANY_TYPE,16) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)            
                AND (TTP.ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND TTP.ADMIN_TYPE IS NULL))--ZHKKH-917
                AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL)

)
SELECT * 
FROM GET_WIKI_SUBSTANCES_CALLS
UNION ALL
SELECT * 
FROM GET_WIKI_SUBSTANCES_E_MAIL
ORDER BY 1
;
 
 TYPE t_wiki_answers_log IS TABLE OF cur_wiki_answers_log%rowtype;

FUNCTION fnc_wiki_answers_log
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_COMPANY_TYPE NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL
      , I_CHANNEL VARCHAR2 := NULL
      
) RETURN t_wiki_answers_log pipelined;



-------------------------------------------------------
--    Статистика шаблонов в разрезе каналов
-------------------------------------------------------

CURSOR cur_wiki_statistic_channel (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_TYPE NUMBER
      , I_TYPE NUMBER
      , I_REGION NUMBER
      , I_CLASSIFICATOR NUMBER
      , I_ACTIVE NUMBER
      , I_ORDER NUMBER
      , I_CHANNEL VARCHAR2 DEFAULT NULL

      
      )
      IS
  WITH 
  GIS_ZHKH AS (SELECT * FROM DUAL),
 --------------------------------------------------
 --WIEWS ДЛЯ КАНАЛА ГОЛОС
 ---------------------------------------------------
IND_FIRST_LINE AS 
 (SELECT 
  SESSION_ID,
  MAX(CASE WHEN FID_RESULT !=9 THEN ID_CALL END) AS ID_CALL_BOTH_LINE,
  MAX(CASE WHEN FID_RESULT =9 THEN ID_CALL END) AS ID_CALL_FIRST_LINE

FROM CORE_CALLS
WHERE CREATED_AT >= I_INIT_TIME AND CREATED_AT < I_FINISH_TIME
 AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)
 GROUP BY SESSION_ID  
 ),
 
 MODIFY_CORE_CALLS AS (
 SELECT 
   CL.*,
   (CASE
     WHEN ID_CALL_FIRST_LINE IS NULL
     THEN '1-я линия'
     ELSE '2-я линия'
    END) AS LINE
 FROM CORE_CALLS CL
 JOIN IND_FIRST_LINE IND
  ON IND.ID_CALL_BOTH_LINE = CL.ID_CALL
  
  UNION
  
  SELECT 
   CL.*,
   '1-я линия' AS LINE
 FROM CORE_CALLS CL
 JOIN IND_FIRST_LINE IND
  ON IND.ID_CALL_FIRST_LINE = CL.ID_CALL
 )
 --------------------------------------------------
 --WIEWS ДЛЯ КАНАЛА e-mail
 ---------------------------------------------------

, ALL_TICKETS_TYPES AS --КлассификаторЫ И ПОЛНОМОЧИЯ
  (
  SELECT
    ID_TICKET
  , LISTAGG(TYPE_NAME,',  ') WITHIN GROUP (order by ID_TYPE_HAS) AS SELECTED_TYPES 
  , MAX(ID_TYPE) KEEP (DENSE_RANK LAST ORDER BY ID_TYPE_HAS) AS LAST_ID_TYPE
  , MAX(TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_TYPE_HAS) AS LAST_TYPE
  , MAX(COMPANY_TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_COMPANY_TYPE
  , MAX(ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_ID_COMPANY_TYPE
  FROM ( 
            SELECT DISTINCT
              TCK.ID_TICKET
            , TDT.ID_TYPE
            , TDT.NAME AS TYPE_NAME
            , TTP.ID_HAS AS ID_TYPE_HAS
            , DCTP.ID_COMPANY_TYPE
            , COALESCE(DCTP.SHORT_NAME, DCTP.NAME) AS COMPANY_TYPE_NAME
            , CTP.ID_HAS AS ID_COMPANY_TYPE_HAS

            FROM USER_ACTION_RELATIONS ACR
            JOIN USER_ACTIONS_LOG ALC
             ON ALC.ID_ACTION = ACR.FID_ACTION
            JOIN WIKI_ANSWER WAN
             ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
            JOIN TICKETS TCK
             ON TCK.ID_TICKET = ALC.LOGGABLE_ID
            JOIN TICKETS_HAS_TYPES TTP
             ON TTP.FID_TICKET = TCK.ID_TICKET
            LEFT JOIN TICKETS_D_TYPES TDT
             ON TDT.ID_TYPE = TTP.FID_TYPE and TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
            LEFT JOIN TICKETS_HAS_CMP_TPS CTP
             ON CTP.FID_TICKET = TCK.ID_TICKET
            LEFT JOIN TICKETS_D_COMPANY_TYPES DCTP
             ON DCTP.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
            
            WHERE ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                      AND ALC.LOGGABLE_TYPE = 'TICKETS'
                      AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                      AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL) 
       )               
  GROUP BY ID_TICKET
  )   
  , GET_WIKI_SUBSTANCES_CALLS AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА ГОЛОС

          SELECT DISTINCT -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
            null as FID_ACTION,--CL.CREATED_AT --Дата и время
            CL.ID_CALL AS ID_ACTION --№ обращения / ID звонка
          , 'Голос' AS CHANNEL --Канал
          , CL.LINE --Линия
          , WAN.ID_ANSWER
 
          FROM CALLS_WIKI_ANSWERS CWA
          JOIN MODIFY_CORE_CALLS CL
           ON CL.ID_CALL = CWA.FID_CALL
          JOIN WIKI_ANSWER WAN
           ON WAN.ID_ANSWER = CWA.FID_ANSWER
--          LEFT JOIN CORE_CALLS_RESULTS RES
--           ON RES.ID_RESULT = CL.FID_RESULT
          LEFT JOIN INC_CALL_CONTACT_DATA INC
           ON INC.FID_CALL = CL.ID_CALL
          LEFT JOIN TICKETS_D_COMPANY_TYPES CTP
           ON CTP.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE
          LEFT JOIN TICKETS_D_TYPES TTP 
           ON TTP.ID_TYPE = INC.FID_COMPANY_TYPE and TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
          LEFT JOIN TICKETS_D_REGIONS TRG
           ON TRG.ID_REGION = INC.FID_COMPANY_REGION
           
          WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
            AND (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
            AND (NVL(CTP.ID_COMPANY_TYPE,16) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
            AND (NVL(TTP.ID_TYPE,1000) = I_TYPE OR I_TYPE IS NULL)
            AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)

 )
 
 , PREV_WIKI_SUBSTANCES_E_MAIL AS ( -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
        SELECT          
            MAX(ACR.ID_RELATION) AS ID_RELATION
          , ACR.RELATIONABLE_ID
          , TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi') as CREATED_TIME

        FROM USER_ACTION_RELATIONS ACR
                JOIN USER_ACTIONS_LOG ALC
                    ON ALC.ID_ACTION = ACR.FID_ACTION                
        WHERE 
                ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                AND ALC.LOGGABLE_TYPE = 'TICKETS'
                AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                
        GROUP BY ACR.RELATIONABLE_ID, TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi')
      )  
, GET_WIKI_SUBSTANCES_E_MAIL AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА E-MAIL

          SELECT 
            ACR.FID_ACTION
          , TCK.ID_TICKET AS ID_ACTION --№ обращения / ID звонка
          , 'E-mail' AS CHANNEL --Канал
          , '' AS LINE--Линия
          , WAN.ID_ANSWER
          
          FROM PREV_WIKI_SUBSTANCES_E_MAIL PREV
          JOIN USER_ACTION_RELATIONS ACR
           ON ACR.ID_RELATION = PREV.ID_RELATION
          JOIN USER_ACTIONS_LOG ALC
           ON ALC.ID_ACTION = ACR.FID_ACTION
          JOIN WIKI_ANSWER WAN
           ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
          JOIN TICKETS TCK
           ON TCK.ID_TICKET = ALC.LOGGABLE_ID
          LEFT JOIN TICKETS_D_STATUSES TST
           ON TST.ID_STATUS = TCK.FID_STATUS
          LEFT JOIN TICKETS_D_REGIONS TRG
           ON TRG.ID_REGION = TCK.FID_COMPANY_REGION
          LEFT JOIN ALL_TICKETS_TYPES TTP
           ON TTP.ID_TICKET = TCK.ID_TICKET

                      
          WHERE 
                (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
            AND (NVL(TTP.LAST_ID_COMPANY_TYPE,16) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
            AND (NVL(TTP.LAST_ID_TYPE,1000) = I_TYPE OR I_TYPE IS NULL)            
            AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL)

 )
 , GET_WIKI_SUBSTANCES_ALL AS (
 SELECT * FROM GET_WIKI_SUBSTANCES_CALLS
 UNION ALL
 SELECT * FROM GET_WIKI_SUBSTANCES_E_MAIL
 )
 , WIKI_STATISTIC AS ( 
 SELECT 
   WCL.CLASSIFICATOR_NAME --Шаблон ответа_Классификатор     
 , WSC.SUBSECTION_NAME --Шаблон ответа_Подраздел
 , TRIM(replace(WSB.SUBSTANCE_NAME,CHR(34),'')) AS SUBSTANCE_NAME --Шаблон ответа_Суть вопроса
 , (CASE
     WHEN WAN.ACTIVE = 1
     THEN 'Активен'
     ELSE 'Не Активен'
    END) AS ACTIVE
 , WAN.ID_ANSWER
 , WAN.INFORMATION_SOURCE
 , SUM(CASE
     WHEN GWS.CHANNEL = 'Голос' AND GWS.LINE = '1-я линия'
     THEN 1
     ELSE 0
    END) AS COUNT_CALLS_FIRST_LINE
 , SUM(CASE
     WHEN GWS.CHANNEL = 'Голос' AND GWS.LINE = '2-я линия'
     THEN 1
     ELSE 0
    END) AS COUNT_CALLS_SECOND_LINE
 , SUM(CASE
     WHEN GWS.CHANNEL = 'Голос'
     THEN 1
     ELSE 0
    END) AS COUNT_CALLS_ALL_LINES
 , SUM(CASE
     WHEN GWS.CHANNEL = 'E-mail'
     THEN 1
     ELSE 0
    END) AS COUNT_E_MAIL
  , COUNT(GWS.CHANNEL) AS ITOGO 
  , ROW_NUMBER()OVER(PARTITION BY '' ORDER BY COUNT(GWS.CHANNEL) DESC)   AS RN
  , 0 as ind_sum
    
  FROM GET_WIKI_SUBSTANCES_ALL GWS
  RIGHT JOIN WIKI_ANSWER WAN
   ON WAN.ID_ANSWER = GWS.ID_ANSWER
  JOIN WIKI_D_CLASSIFICATOR WCL
   ON WCL.ID_CLASSIFICATOR = WAN.FID_CLASSIFICATOR
  LEFT JOIN WIKI_D_SUBSECTION WSC
   ON WSC.ID_SUBSECTION = WAN.FID_SUBSECTION
  JOIN WIKI_D_SUBSTANCE WSB
   ON WSB.ID_SUBSTANCE = WAN.FID_SUBSTANCE
  WHERE (WAN.ACTIVE = I_ACTIVE OR I_ACTIVE IS NULL)
    AND (WCL.ID_CLASSIFICATOR = I_CLASSIFICATOR OR I_CLASSIFICATOR IS NULL)
 GROUP BY WCL.CLASSIFICATOR_NAME, WSC.SUBSECTION_NAME, WSB.SUBSTANCE_NAME, WAN.ACTIVE , WAN.ID_ANSWER, WAN.INFORMATION_SOURCE 
 )

  SELECT * FROM (
     SELECT * 
     FROM WIKI_STATISTIC 
     WHERE (I_ORDER = 3 AND RN<=10) OR I_ORDER IN (1,2)
     ORDER BY
     CASE
      WHEN I_ORDER = 1
      THEN  ID_ANSWER
      ELSE null
     END nulls last,
     CASE
      WHEN I_ORDER = 2
      THEN  lower(SUBSTANCE_NAME)
      ELSE null
     END nulls last,
     CASE
      WHEN I_ORDER = 3
      THEN  ITOGO
      ELSE null
     END DESC nulls last
     ) TAB
     
     UNION ALL
     
    SELECT 
     'Всего' AS CLASSIFICATOR_NAME --Шаблон_Классификатор
   , '' AS SUBSECTION_NAME --Шаблон_Подраздел
   , '' AS SUBSTANCE_NAME --Шаблон_Суть вопроса
   , '' AS ACTIVE --Активность
   , NULL AS ID_ANSWER
   , NULL AS INFORMATION_SOURCE
   , SUM(COUNT_CALLS_FIRST_LINE) AS COUNT_CALLS_FIRST_LINE --Голос, 1-я линия
   , SUM(COUNT_CALLS_SECOND_LINE) AS COUNT_CALLS_SECOND_LINE --Голос, экспертная линия
   , SUM(COUNT_CALLS_ALL_LINES) AS COUNT_CALLS_ALL_LINES --Голос, всего
   , SUM(COUNT_E_MAIL) AS COUNT_E_MAIL --E-mail
   , SUM(ITOGO) AS ITOGO --Итого
   , NULL AS RN
   , 1 as ind_sum
   FROM WIKI_STATISTIC
   WHERE (I_ORDER = 3 AND RN<=10) OR I_ORDER IN (1,2)


ORDER BY 13
 ;
 
 
 TYPE t_wiki_statistic_channel IS TABLE OF cur_wiki_statistic_channel%rowtype;

FUNCTION fnc_wiki_statistic_channel
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_TYPE NUMBER
      , I_TYPE NUMBER
      , I_REGION NUMBER
      , I_CLASSIFICATOR NUMBER
      , I_ACTIVE NUMBER
      , I_ORDER NUMBER
      , I_CHANNEL VARCHAR2 DEFAULT NULL
      
) RETURN t_wiki_statistic_channel pipelined;


-------------------------------------------------------
--    Статистика шаблонов в разрезе полномочий
-------------------------------------------------------

CURSOR cur_wiki_statistic_company (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_REGION NUMBER
      , I_ACTIVE NUMBER
      , I_COMPANY_TYPE NUMBER DEFAULT NULL
      , I_TYPE NUMBER DEFAULT NULL
      , I_CLASSIFICATOR NUMBER DEFAULT NULL
      
      )
      IS
  WITH
  GIS_ZHKH AS (SELECT * FROM DUAL),
 --------------------------------------------------
 --WIEWS ДЛЯ КАНАЛА ГОЛОС
 ---------------------------------------------------
IND_FIRST_LINE AS 
 (SELECT 
  SESSION_ID,
  MAX(CASE WHEN FID_RESULT !=9 THEN ID_CALL END) AS ID_CALL_BOTH_LINE,
  MAX(CASE WHEN FID_RESULT =9 THEN ID_CALL END) AS ID_CALL_FIRST_LINE

FROM CORE_CALLS
WHERE CREATED_AT >= I_INIT_TIME AND CREATED_AT < I_FINISH_TIME
 AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)
 GROUP BY SESSION_ID  
 ),
 
 MODIFY_CORE_CALLS AS (
 SELECT 
   CL.*,
   (CASE
     WHEN ID_CALL_FIRST_LINE IS NULL
     THEN '1-я линия'
     ELSE '2-я линия'
    END) AS LINE
 FROM CORE_CALLS CL
 JOIN IND_FIRST_LINE IND
  ON IND.ID_CALL_BOTH_LINE = CL.ID_CALL
  
  UNION
  
  SELECT 
   CL.*,
   '1-я линия' AS LINE
 FROM CORE_CALLS CL
 JOIN IND_FIRST_LINE IND
  ON IND.ID_CALL_FIRST_LINE = CL.ID_CALL
 )
 --------------------------------------------------
 --WIEWS ДЛЯ КАНАЛА e-mail
 ---------------------------------------------------

, ALL_TICKETS_TYPES AS --КлассификаторЫ И ПОЛНОМОЧИЯ
  (
  SELECT
    ID_TICKET
--  , LISTAGG(TYPE_NAME,',  ') WITHIN GROUP (order by ID_TYPE_HAS) AS SELECTED_TYPES 
--  , MAX(ID_TYPE) KEEP (DENSE_RANK LAST ORDER BY ID_TYPE_HAS) AS LAST_ID_TYPE
--  , MAX(TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_TYPE_HAS) AS LAST_TYPE
  , MAX(COMPANY_TYPE_NAME) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_COMPANY_TYPE
  , MAX(ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY ID_COMPANY_TYPE_HAS) AS LAST_ID_COMPANY_TYPE
  FROM ( 
            SELECT DISTINCT
              TCK.ID_TICKET
--            , TDT.ID_TYPE
--            , TDT.NAME AS TYPE_NAME
--            , TTP.ID_HAS AS ID_TYPE_HAS
            , DCTP.ID_COMPANY_TYPE
            , COALESCE(DCTP.SHORT_NAME, DCTP.NAME) AS COMPANY_TYPE_NAME
            , CTP.ID_HAS AS ID_COMPANY_TYPE_HAS

            FROM USER_ACTION_RELATIONS ACR
            JOIN USER_ACTIONS_LOG ALC
             ON ALC.ID_ACTION = ACR.FID_ACTION
            JOIN WIKI_ANSWER WAN
             ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
            JOIN TICKETS TCK
             ON TCK.ID_TICKET = ALC.LOGGABLE_ID
--            JOIN TICKETS_HAS_TYPES TTP
--             ON TTP.FID_TICKET = TCK.ID_TICKET
--            JOIN TICKETS_D_TYPES TDT
--             ON TDT.ID_TYPE = TTP.FID_TYPE
            LEFT JOIN TICKETS_HAS_CMP_TPS CTP
             ON CTP.FID_TICKET = TCK.ID_TICKET
            LEFT JOIN TICKETS_D_COMPANY_TYPES DCTP
             ON DCTP.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
            
            WHERE ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                      AND ALC.LOGGABLE_TYPE = 'TICKETS'
                      AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                      AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL) 
       )               
  GROUP BY ID_TICKET
  )   
  , GET_WIKI_SUBSTANCES_CALLS AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА ГОЛОС

          SELECT DISTINCT -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
            null as FID_ACTION,--CL.CREATED_AT --Дата и время
            CL.ID_CALL AS ID_ACTION --№ обращения / ID звонка
          , 'Голос' AS CHANNEL --Канал
          , CL.LINE --Линия
          , NVL(CTP.ID_COMPANY_TYPE,16) AS ID_COMPANY_TYPE
          , WAN.ID_ANSWER
 
          FROM CALLS_WIKI_ANSWERS CWA
          JOIN MODIFY_CORE_CALLS CL
           ON CL.ID_CALL = CWA.FID_CALL
          JOIN WIKI_ANSWER WAN
           ON WAN.ID_ANSWER = CWA.FID_ANSWER
--          LEFT JOIN CORE_CALLS_RESULTS RES
--           ON RES.ID_RESULT = CL.FID_RESULT
          LEFT JOIN INC_CALL_CONTACT_DATA INC
           ON INC.FID_CALL = CL.ID_CALL
          LEFT JOIN TICKETS_D_COMPANY_TYPES CTP
           ON CTP.ID_COMPANY_TYPE = INC.FID_COMPANY_TYPE
          --LEFT JOIN TICKETS_D_TYPES TTP 
          -- ON TTP.ID_TYPE = INC.FID_COMPANY_TYPE
          LEFT JOIN TICKETS_D_REGIONS TRG
           ON TRG.ID_REGION = INC.FID_COMPANY_REGION
           
          WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
            AND (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
          --  AND (NVL(CTP.ID_COMPANY_TYPE,16) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
          --  AND (NVL(TTP.ID_TYPE,1000) = I_TYPE OR I_TYPE IS NULL)
            AND ('Голос' = I_CHANNEL OR I_CHANNEL IS NULL)

 ) 
  , PREV_WIKI_SUBSTANCES_E_MAIL AS ( -- ПОТОМУ ЧТО В USER_ACTION_RELATIONS ЕСТЬ ГЛЮК (НЕСКОЛЬКО РАЗ ВЫБИРАЕТСЯ ОДИН И ТОТ ЖЕ ШАБЛОН)
        SELECT          
            MAX(ACR.ID_RELATION) AS ID_RELATION
          , ACR.RELATIONABLE_ID
          , TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi') as CREATED_TIME

        FROM USER_ACTION_RELATIONS ACR
                JOIN USER_ACTIONS_LOG ALC
                    ON ALC.ID_ACTION = ACR.FID_ACTION                
        WHERE 
                ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
                AND ALC.LOGGABLE_TYPE = 'TICKETS'
                AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
                
        GROUP BY ACR.RELATIONABLE_ID, TO_CHAR(ACR.CREATED_AT,'dd.mm.yyyy hh24:mi')
      ) 
, GET_WIKI_SUBSTANCES_E_MAIL AS ( --Выбранные шаблоны ответов ДЛЯ КАНАЛА E-MAIL

          SELECT 
            ACR.FID_ACTION
          , TCK.ID_TICKET AS ID_ACTION --№ обращения / ID звонка
          , 'E-mail' AS CHANNEL --Канал
          , '' AS LINE--Линия
          , NVL(TTP.LAST_ID_COMPANY_TYPE,16) AS ID_COMPANY_TYPE
          , WAN.ID_ANSWER
          
          FROM PREV_WIKI_SUBSTANCES_E_MAIL PREV
          JOIN USER_ACTION_RELATIONS ACR
           ON ACR.ID_RELATION = PREV.ID_RELATION
          JOIN USER_ACTIONS_LOG ALC
           ON ALC.ID_ACTION = ACR.FID_ACTION
          JOIN WIKI_ANSWER WAN
           ON WAN.ID_ANSWER = ACR.RELATIONABLE_ID
          JOIN TICKETS TCK
           ON TCK.ID_TICKET = ALC.LOGGABLE_ID
          LEFT JOIN TICKETS_D_STATUSES TST
           ON TST.ID_STATUS = TCK.FID_STATUS
          LEFT JOIN TICKETS_D_REGIONS TRG
           ON TRG.ID_REGION = TCK.FID_COMPANY_REGION
          LEFT JOIN ALL_TICKETS_TYPES TTP
           ON TTP.ID_TICKET = TCK.ID_TICKET

                      
          WHERE ACR.RELATIONABLE_TYPE = 'WIKI_ANSWER'
            AND ALC.LOGGABLE_TYPE = 'TICKETS'
            AND (ACR.CREATED_AT >= I_INIT_TIME AND ACR.CREATED_AT < I_FINISH_TIME)
            AND (NVL(TRG.ID_REGION,85) = I_REGION OR I_REGION IS NULL)
          --  AND (NVL(TTP.LAST_ID_COMPANY_TYPE,16) = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
          --  AND (NVL(TTP.LAST_ID_TYPE,1000) = I_TYPE OR I_TYPE IS NULL)            
            AND ('e-mail' = I_CHANNEL OR I_CHANNEL IS NULL)

 )
 , GET_WIKI_SUBSTANCES_ALL AS (
 SELECT * FROM GET_WIKI_SUBSTANCES_CALLS
 UNION ALL
 SELECT * FROM GET_WIKI_SUBSTANCES_E_MAIL
 )
 , WIKI_STATISTIC_SUM AS ( 
 SELECT
   WAN.ID_ANSWER  
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 1 THEN 1 ELSE 0 END) AS C_1
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 2 THEN 1 ELSE 0 END) AS C_2
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 3 THEN 1 ELSE 0 END) AS C_3
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 4 THEN 1 ELSE 0 END) AS C_4
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 5 THEN 1 ELSE 0 END) AS C_5
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 6 THEN 1 ELSE 0 END) AS C_6
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 7 THEN 1 ELSE 0 END) AS C_7
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 8 THEN 1 ELSE 0 END) AS C_8
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 9 THEN 1 ELSE 0 END) AS C_9
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 10 THEN 1 ELSE 0 END) AS C_10
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 11 THEN 1 ELSE 0 END) AS C_11
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 12 THEN 1 ELSE 0 END) AS C_12
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 13 THEN 1 ELSE 0 END) AS C_13
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 14 THEN 1 ELSE 0 END) AS C_14
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 15 THEN 1 ELSE 0 END) AS C_15
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 16 THEN 1 ELSE 0 END) AS C_16
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 17 THEN 1 ELSE 0 END) AS C_17
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 18 THEN 1 ELSE 0 END) AS C_18
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 19 THEN 1 ELSE 0 END) AS C_19
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 20 THEN 1 ELSE 0 END) AS C_20
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 21 THEN 1 ELSE 0 END) AS C_21
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 22 THEN 1 ELSE 0 END) AS C_22
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 23 THEN 1 ELSE 0 END) AS C_23
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 24 THEN 1 ELSE 0 END) AS C_24
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 25 THEN 1 ELSE 0 END) AS C_25
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 26 THEN 1 ELSE 0 END) AS C_26
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 27 THEN 1 ELSE 0 END) AS C_27
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 28 THEN 1 ELSE 0 END) AS C_28
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 29 THEN 1 ELSE 0 END) AS C_29
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 30 THEN 1 ELSE 0 END) AS C_30
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 31 THEN 1 ELSE 0 END) AS C_31
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 32 THEN 1 ELSE 0 END) AS C_32
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 33 THEN 1 ELSE 0 END) AS C_33
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 34 THEN 1 ELSE 0 END) AS C_34
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 35 THEN 1 ELSE 0 END) AS C_35
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 36 THEN 1 ELSE 0 END) AS C_36
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 37 THEN 1 ELSE 0 END) AS C_37
  , SUM(CASE WHEN GWS.ID_COMPANY_TYPE = 38 THEN 1 ELSE 0 END) AS C_38
  
  
  , count(GWS.ID_COMPANY_TYPE) AS ITOGO 

    
  FROM GET_WIKI_SUBSTANCES_ALL GWS
  RIGHT JOIN WIKI_ANSWER WAN
   ON WAN.ID_ANSWER = GWS.ID_ANSWER
  WHERE (WAN.ACTIVE = I_ACTIVE OR I_ACTIVE IS NULL)
 --   AND (WCL.ID_CLASSIFICATOR = I_CLASSIFICATOR OR I_CLASSIFICATOR IS NULL)
 GROUP BY ROLLUP(WAN.ID_ANSWER)

 )
  SELECT 
   (CASE
     WHEN WAN.ID_ANSWER IS NULL
     THEN 'Всего'
     ELSE WCL.CLASSIFICATOR_NAME
    END) AS CLASSIFICATOR_NAME --Шаблон ответа_Классификатор     
 , WSC.SUBSECTION_NAME --Шаблон ответа_Подраздел
 , TRIM(replace(WSB.SUBSTANCE_NAME,CHR(34),'')) AS SUBSTANCE_NAME --Шаблон ответа_Суть вопроса
 , WAN.INFORMATION_SOURCE --Ссылка на шаблон wiki
 , (CASE
     WHEN WAN.ACTIVE = 1
     THEN 'Активен'
     WHEN WAN.ACTIVE = 0 
     THEN 'Не Активен'
     ELSE ''
    END) AS ACTIVE
 , WSS.* 
  FROM WIKI_STATISTIC_SUM WSS
  LEFT JOIN WIKI_ANSWER WAN
   ON WAN.ID_ANSWER = WSS.ID_ANSWER
  LEFT JOIN WIKI_D_CLASSIFICATOR WCL
   ON WCL.ID_CLASSIFICATOR = WAN.FID_CLASSIFICATOR
  LEFT JOIN WIKI_D_SUBSECTION WSC
   ON WSC.ID_SUBSECTION = WAN.FID_SUBSECTION
  LEFT JOIN WIKI_D_SUBSTANCE WSB
   ON WSB.ID_SUBSTANCE = WAN.FID_SUBSTANCE
  ORDER BY WAN.ID_ANSWER ASC NULLS LAST
;
      
      
    TYPE t_wiki_statistic_company IS TABLE OF cur_wiki_statistic_company%rowtype;

FUNCTION fnc_wiki_statistic_company
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_REGION NUMBER
      , I_ACTIVE NUMBER
      , I_COMPANY_TYPE NUMBER DEFAULT NULL
      , I_TYPE NUMBER DEFAULT NULL
      , I_CLASSIFICATOR NUMBER DEFAULT NULL
      
) RETURN t_wiki_statistic_company pipelined;

      

END PKG_WIKI_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_WIKI_REPORTS AS


    --                                                            --
    --  Отчетность для статистики по используемым шаблонам        --
    --  Заявка ZHKKH-726                                          --
    --
-------------------------------------------------------
--  Детализированный отчет по используемым шаблонам
-------------------------------------------------------

  FUNCTION fnc_wiki_answers_log
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_COMPANY_TYPE NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL
      , I_CHANNEL VARCHAR2 := NULL
      

) RETURN t_wiki_answers_log pipelined AS
    INIT_TIME TIMESTAMP;
    FINISH_TIME TIMESTAMP;
  BEGIN
  --Так сделано, потому что в конце апреля была проблема с логированием
   INIT_TIME := (CASE WHEN I_INIT_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       THEN TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       ELSE I_INIT_TIME
                 END);
   FINISH_TIME := (CASE WHEN I_FINISH_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi') THEN NULL ELSE I_FINISH_TIME END);
   FOR L IN cur_wiki_answers_log(INIT_TIME, FINISH_TIME, I_REGION, I_COMPANY_TYPE,I_ADMIN_TYPE, I_CHANNEL)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_wiki_answers_log;
  
  
  
-------------------------------------------------------
--    Статистика шаблонов в разрезе каналов
-------------------------------------------------------

  FUNCTION fnc_wiki_statistic_channel
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_TYPE NUMBER
      , I_TYPE NUMBER
      , I_REGION NUMBER
      , I_CLASSIFICATOR NUMBER
      , I_ACTIVE NUMBER
      , I_ORDER NUMBER
      , I_CHANNEL VARCHAR2 DEFAULT NULL
      
) RETURN t_wiki_statistic_channel pipelined AS
    INIT_TIME TIMESTAMP;
    FINISH_TIME TIMESTAMP;
  BEGIN
  --Так сделано, потому что в конце апреля была проблема с логированием
   INIT_TIME := (CASE WHEN I_INIT_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       THEN TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       ELSE I_INIT_TIME
                 END); 
   FINISH_TIME := (CASE WHEN I_FINISH_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi') THEN NULL ELSE I_FINISH_TIME END); 
   FOR L IN cur_wiki_statistic_channel(INIT_TIME,
                                       FINISH_TIME,
                                       I_COMPANY_TYPE,
                                       I_TYPE,
                                       I_REGION,
                                       I_CLASSIFICATOR,
                                       I_ACTIVE-1,
                                       I_ORDER,
                                       I_CHANNEL                               

                                       )
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_wiki_statistic_channel;
  
  

-------------------------------------------------------
--    Статистика шаблонов в разрезе полномочий
-------------------------------------------------------

FUNCTION fnc_wiki_statistic_company
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CHANNEL VARCHAR2
      , I_REGION NUMBER
      , I_ACTIVE NUMBER
      , I_COMPANY_TYPE NUMBER DEFAULT NULL
      , I_TYPE NUMBER DEFAULT NULL
      , I_CLASSIFICATOR NUMBER DEFAULT NULL
      
) RETURN t_wiki_statistic_company pipelined AS
    INIT_TIME TIMESTAMP;
    FINISH_TIME TIMESTAMP;
  BEGIN
  --Так сделано, потому что в конце апреля была проблема с логированием
   INIT_TIME := (CASE WHEN I_INIT_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       THEN TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi')
                       ELSE I_INIT_TIME
                 END); 
   FINISH_TIME := (CASE WHEN I_FINISH_TIME < TO_TIMESTAMP('01.05.2016 00:00','dd.mm.yyyy hh24:mi') THEN NULL ELSE I_FINISH_TIME END); 
   FOR L IN cur_wiki_statistic_company(INIT_TIME,
                                       FINISH_TIME,
                                       I_CHANNEL,
                                       I_REGION,                            
                                       I_ACTIVE-1                                                                

                                       )
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_wiki_statistic_company;
  

END PKG_WIKI_REPORTS;
/
