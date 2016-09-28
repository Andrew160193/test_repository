CREATE OR REPLACE PACKAGE          PKG_MAIL_REPORTS AS

--------------------------------------------------------------
--     СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ MAILREADER    --
--------------------------------------------------------------

CURSOR cur_tickets_statistic (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL)
, PERIODS AS (
        SELECT
                CAST(GREATEST(PERIOD_START_TIME, CAST(I_INIT_TIME AS TIMESTAMP)) AS TIMESTAMP) AS START_PERIOD
                , CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD
                , TO_CHAR(GREATEST(PERIOD_START_TIME, CAST(I_INIT_TIME AS TIMESTAMP)),'dd.mm.yyyy') AS VIEW_PERIOD
                --          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
                --          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE (
                COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME (
                        NVL2(LOWER(I_GROUP), TRUNC(CAST(I_INIT_TIME AS TIMESTAMP)), CAST(I_INIT_TIME AS TIMESTAMP))
                        , CAST(I_FINISH_TIME AS TIMESTAMP)
                        , NVL(LOWER(I_GROUP), 'year')
                )
        )
)
, COMPANY_TYPE_FOR_FILTER AS (--ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
        SELECT
                ID_COMPANY_TYPE
                , NAME AS FULL_NAME
                , COALESCE(SHORT_NAME, NAME) AS NAME
                , CASE
                        WHEN NAME = 'Гражданин' THEN 'По гражданам'
                        WHEN NAME = 'Не определено' THEN 'Не определено'
                        ELSE 'По организациям'
                END AS CLIENT_TYPE
        FROM TICKETS_D_COMPANY_TYPES
)
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
        FROM PERIODS
                , ALL_TYPES_FOR_FORMAT TTP
--  ORDER BY START_PERIOD,ORD,(case when CLASS_TYPE = 'Гражданин' then 1 else 2 end), ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
)

, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
) 
, ALL_TICKETS_TYPES AS (--КлассификаторЫ
        SELECT
                TCK.ID_TICKET AS ID_TICKET
                , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
                , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
                , MAX((CASE WHEN CTPF.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
                , MAX(ADT.NAME) AS ADMIN_TYPE
                , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
                , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
        FROM  TICKETS TCK
                JOIN MAIL_MESSAGES MSG
                    ON MSG.FID_TICKET = TCK.ID_TICKET
                JOIN TICKETS_HAS_TYPES TTP
                    ON TTP.FID_TICKET = TCK.ID_TICKET
                LEFT JOIN ALL_TYPES TDT  --MUST JOIN
                    ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
                LEFT JOIN TICKETS_HAS_CMP_TPS CTP
                    ON CTP.FID_TICKET = TCK.ID_TICKET
                LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
                    ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
                LEFT JOIN TICKETS_D_ADM_TYPES ADT
                    ON ADT.ID_TYPE = TCK.FID_ADM_TYPE
                LEFT JOIN BLOCK_MAILS BML
                 ON BML.FID_MESSAGE = MSG.ID_MESSAGE
        WHERE
                nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
                AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
                AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017 
        GROUP BY TCK.ID_TICKET
)
, ALL_CALLS_PREV AS (
        SELECT 
                CL.SESSION_ID
                , MAX(CL.ID_CALL) AS ID_CALL
        FROM CORE_CALLS CL
        WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
        GROUP BY CL.SESSION_ID
)  
, ALL_CALLS AS (
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
                SELECT 
                        tab.*
                        , ROW_NUMBER()OVER(PARTITION BY tab.SESSION_ID ORDER BY tab.CALL_CREATED DESC)   AS RN
                FROM TABLE(PKG_GENERAL_REPORTS.FNC_DATA_INC_CALL(I_INIT_TIME, I_FINISH_TIME)) tab
                WHERE cast(I_FINISH_TIME as timestamp) > TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME
                UNION ALL
                SELECT 
                        tab.*
                        , 1 AS RN
                FROM TABLE_DATA_INC_CALL TAB
                WHERE (cast(I_FINISH_TIME as timestamp) <= TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
                AND (tab.CALL_CREATED >= I_INIT_TIME AND tab.CALL_CREATED < I_FINISH_TIME)
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
, ALL_TICKETS AS ( --E-mail
        SELECT
                TTP.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1
                , TTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2
                , TTP.CLASS_TYPE AS CLASS_TYPE
                , TTP.ADMIN_TYPE AS ADMIN_TYPE 
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
        --E-mail (Из них заявок на 2-ю линию, шт. )
        SELECT
                TTP.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1
                , TTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2
                , TTP.CLASS_TYPE AS CLASS_TYPE
                , TTP.ADMIN_TYPE AS ADMIN_TYPE 
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
                END) AS TYPE_NAME_LEVEL_1 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
                , (CASE
                        WHEN CL.FID_RESULT IN (5,6,7,8)
                        THEN 'Посторонний звонок'
                        WHEN CL.FID_RESULT = 4
                        THEN 'Тестовое обращение'
                END) AS TYPE_NAME_LEVEL_2 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)  
                , (CASE
                        WHEN CL.FID_RESULT IN (5,6,7,8)
                        THEN '-'
                        WHEN CL.FID_RESULT = 4
                        THEN 'Гражданин'
                END) AS CLASS_TYPE   
                , '' AS ADMIN_TYPE
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
        WHERE  
                CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
                AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
                AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
                AND CL.FID_RESULT IN (4,5,6,7,8,10,11)
                AND ACL.ANS_CALL = 1
        UNION ALL
        --Голос
        SELECT
                CLT.TYPE_NAME_LEVEL_1
                , CLT.TYPE_NAME_LEVEL_2
                , (CASE
                        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
                        ELSE 'НЕ гражданин'
                END) AS CLASS_TYPE
                , CLT.ADMIN_TYPE
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
                    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
                JOIN TICKETS_D_COMPANY_TYPES CPT
                    ON CPT.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
                LEFT JOIN CALLS_TYPE CLT
                    ON CLT.SESSION_ID = CL.SESSION_ID
                LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
                    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
        WHERE  
                CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
                AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
                AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
                AND CL.FID_RESULT in (1,2,3,9)
                AND ACL.ANS_CALL = 1
        UNION ALL
        --Голос (Оформлено заявок на 2-ю линию, шт.)
        SELECT
                CLT.TYPE_NAME_LEVEL_1
                , CLT.TYPE_NAME_LEVEL_2
                , (CASE
                        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
                        ELSE 'НЕ гражданин'
                END) AS CLASS_TYPE
                , CLT.ADMIN_TYPE
                , 'INCOMING_LINE_3' AS LINE
                , PR.START_PERIOD AS PERIOD
                , ACL.ANS_CALL_SECOND
        FROM PERIODS PR
                JOIN CORE_CALLS CL 
                    ON CL.CREATED_AT >= PR.START_PERIOD AND CL.CREATED_AT < PR.STOP_PERIOD
                JOIN INC_CALL_CONTACT_DATA CCD
                    ON CCD.FID_CALL = CL.ID_CALL --AND CCD.IS_PRIMARY = 1
                JOIN TICKETS_D_COMPANY_TYPES CPT
                    ON CPT.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
                LEFT JOIN CALLS_TYPE CLT
                    ON CLT.SESSION_ID = CL.SESSION_ID
                LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
                    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
                LEFT JOIN ALL_CALLS ACL
                    ON ACL.ID_CALL = CL.ID_CALL
        WHERE  
                CL.CREATED_AT BETWEEN I_INIT_TIME AND I_FINISH_TIME
                AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
                AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
                AND LOWER(CL.DIRECTION) = 'in'
                AND CCD.FID_MESSAGE_MAIL IS NOT NULL
)
, SUM_TICKETS AS (  
        SELECT
                TYPE_NAME_LEVEL_1
                , TYPE_NAME_LEVEL_2
                , CLASS_TYPE
                , PERIOD
                , SUM(
                        CASE
                                WHEN LINE = 'INCOMING'
                                THEN 1
                                ELSE 0
                        END
                ) AS INCOMING_FIRST
                , SUM(
                        CASE
                                WHEN LINE = 'INCOMING' AND ANS_CALL_SECOND = 1
                                THEN 1
                                ELSE 0
                        END
                ) AS INCOMING_SECOND
                , SUM(
                        CASE
                                WHEN LINE = 'INCOMING_LINE_3'
                                THEN 1
                                ELSE 0
                        END
                ) AS INCOMING_LINE_3_FIRST
                , SUM(
                        CASE
                                WHEN LINE = 'INCOMING_LINE_3' AND ANS_CALL_SECOND = 1
                                THEN 1
                                ELSE 0
                        END
                ) AS INCOMING_LINE_3_SECOND
                , SUM(
                        CASE
                                WHEN LINE = 'MAILREADER'
                                THEN 1
                                ELSE 0
                        END
                ) AS MAILREADER
                , SUM(
                        CASE
                                WHEN LINE = 'MAILREADER_LINE_3'
                                THEN 1
                                ELSE 0
                        END
                ) AS MAILREADER_LINE_3
        FROM  ALL_TICKETS
        WHERE (ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
        GROUP BY 
                TYPE_NAME_LEVEL_1
                , TYPE_NAME_LEVEL_2
                , CLASS_TYPE, PERIOD
)
, SUM_TICKETS_2 AS (--ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
        SELECT
                DECODE(GROUPING(FT.START_PERIOD)
                ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
                , FT.TYPE_NAME_LEVEL_2
                , FT.CLASS_TYPE
                , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
                , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
                , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
                , MAX(FT.ORD) AS ORD
                , SUM(NVL(ST.INCOMING_FIRST,0)) AS INCOMING_FIRST --Входящая линия --1-Я ЛИНИЯ
                , SUM(NVL(ST.INCOMING_SECOND,0)) AS INCOMING_SECOND --Входящая линия --2-Я ЛИНИЯ
                , SUM(NVL(ST.INCOMING_FIRST,0)+NVL(ST.INCOMING_SECOND,0)) AS INCOMING --Входящая линия
                -- , SUM(NVL(ST.INCOMING_LINE_3,0)) AS INCOMING_LINE_3 --Входящая линия
                
                , SUM(DECODE(FT.TYPE_NAME_LEVEL_1,
                'Тестовое обращение',0,
                NVL(ST.INCOMING_LINE_3_FIRST,0))) AS INCOMING_LINE_3_FIRST --Входящая линия --1-Я ЛИНИЯ
                
                , SUM(DECODE(FT.TYPE_NAME_LEVEL_1,
                'Тестовое обращение',0,
                NVL(ST.INCOMING_LINE_3_SECOND,0))) AS INCOMING_LINE_3_SECOND --Входящая линия   
                
                , SUM(DECODE(FT.TYPE_NAME_LEVEL_1,
                'Тестовое обращение',0,
                NVL(ST.INCOMING_LINE_3_FIRST,0) + NVL(ST.INCOMING_LINE_3_SECOND,0))) AS INCOMING_LINE_3                     
                
                , SUM(NVL(ST.MAILREADER,0)) AS MAILREADER  --MailReader
                , SUM(NVL(ST.MAILREADER_LINE_3,0)) AS MAILREADER_LINE_3  --MailReader
                , SUM(NVL(ST.MAILREADER,0)+NVL(ST.INCOMING_FIRST,0)+ NVL(ST.INCOMING_SECOND,0)) AS ITOGO --Итого
        FROM SUM_TICKETS ST
                RIGHT JOIN FORMAT FT 
                    ON FT.TYPE_NAME_LEVEL_1 = ST.TYPE_NAME_LEVEL_1
                    AND FT.TYPE_NAME_LEVEL_2 = ST.TYPE_NAME_LEVEL_2
                    AND FT.CLASS_TYPE = ST.CLASS_TYPE
                    AND FT.START_PERIOD = ST.PERIOD
        GROUP BY ROLLUP(FT.START_PERIOD,FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)
        ORDER BY GROUPING(FT.START_PERIOD),FT.START_PERIOD,ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
)
SELECT
        TYPE_NAME_LEVEL_1 --Классификация по теме
        , TYPE_NAME_LEVEL_2 --Классификация по теме 2 LEVEL  
        , CLASS_TYPE --Классификатор для полномочия
        , NVL2(I_GROUP,PERIOD,'') AS PERIOD
        , INCOMING_FIRST --Голос 1-Я
        , INCOMING_SECOND --Голос 2-Я
        , INCOMING --Голос, ВСЕГО
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0',
        INCOMING_LINE_3_FIRST) AS INCOMING_LINE_3_FIRST
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0,00%',
        REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3_FIRST/DECODE(INCOMING_FIRST,0,1,INCOMING_FIRST),0)*100,'990D99')),'.',',')||'%') AS INCOMING_FIRST_PROCENT
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0',
        INCOMING_LINE_3_SECOND) AS INCOMING_LINE_3_SECOND
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0,00%',
        REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3_SECOND/DECODE(INCOMING_SECOND,0,1,INCOMING_SECOND),0)*100,'990D99')),'.',',')||'%') AS INCOMING_SECOND_PROCENT
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0',
        INCOMING_LINE_3) AS INCOMING_LINE_3
        , DECODE(TYPE_NAME_LEVEL_1,
        'Тестовое обращение','0,00%',
        REPLACE(TRIM(TO_CHAR(NVL(INCOMING_LINE_3/DECODE(INCOMING,0,1,INCOMING),0)*100,'990D99')),'.',',')||'%') AS INCOMING_PROCENT
        , DECODE(TYPE_NAME_LEVEL_1,
        'Посторонний звонок','-',
        MAILREADER) AS MAILREADER --E-mail
        , DECODE(TYPE_NAME_LEVEL_1,
        'Посторонний звонок','-',
        MAILREADER_LINE_3) AS MAILREADER_LINE_3 --E-mail
        , REPLACE(TRIM(TO_CHAR(NVL(MAILREADER_LINE_3/DECODE(MAILREADER,0,1,MAILREADER),0)*100,'990D99')),'.',',')||'%' AS MAILREADER_PROCENT
        , ITOGO --Итого
FROM
SUM_TICKETS_2
WHERE 
        (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) 
        OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
;

  TYPE t_tickets_statistic IS TABLE OF cur_tickets_statistic%rowtype;

  FUNCTION fnc_tickets_statistic
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
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
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
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
 ,  CALLS_TYPE AS ( --Первый выбранный тип при ответе на вопросы --ZHKKH-917
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
  SELECT * FROM PERIODS, ALL_TYPES_FOR_FORMAT TTP

             --  ORDER BY START_PERIOD,ORD,(case when CLASS_TYPE = 'Гражданин' then 1 else 2 end), ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  )

, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
)  
  
,  ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
  , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
  , MAX((CASE WHEN CTPF.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
  , MAX(ADT.NAME) AS ADMIN_TYPE
  , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
  , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM
  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN ALL_TYPES TDT  --MUST JOIN
   ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  LEFT JOIN TICKETS_D_ADM_TYPES ADT
   ON ADT.ID_TYPE = TCK.FID_ADM_TYPE 
  LEFT JOIN BLOCK_MAILS BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE 
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
    AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017   
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
  (
  --E-MAIL
  SELECT
    NVL(TRG.CODE,0) AS CODE_REGION
  , TTP.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1
  , TTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2
  , TTP.CLASS_TYPE AS CLASS_TYPE
  , TTP.ADMIN_TYPE AS ADMIN_TYPE 
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
   LEFT JOIN TICKETS_D_REGIONS TRG
    ON TRG.ID_REGION = TCK.FID_COMPANY_REGION  
  WHERE
           (TTP.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
       AND (TTP.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
       AND TCK.IS_ACTIVE = 1
  UNION ALL
  --ГОЛОС
   SELECT 
    NVL(TRG.CODE,0) AS CODE_REGION
  , (CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
     END) AS TYPE_NAME_LEVEL_1 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
  , (CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
     END) AS TYPE_NAME_LEVEL_2 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)  
  , (CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN '-'
      WHEN CL.FID_RESULT = 4
       THEN 'Гражданин'
     END) AS CLASS_TYPE   
  , '' AS ADMIN_TYPE
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
   LEFT JOIN TICKETS_D_REGIONS TRG
    ON TRG.ID_REGION = CCD.FID_COMPANY_REGION 
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
    NVL(TRG.CODE,0) AS CODE_REGION
  , CLT.TYPE_NAME_LEVEL_1
  , CLT.TYPE_NAME_LEVEL_2
  , (CASE
      WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
      ELSE 'НЕ гражданин'
     END) AS CLASS_TYPE
  , CLT.ADMIN_TYPE
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
   JOIN TICKETS_D_COMPANY_TYPES CPT
    ON CPT.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE 
   LEFT JOIN CALLS_TYPE CLT
    ON CLT.SESSION_ID = CL.SESSION_ID 
   LEFT JOIN TICKETS_D_REGIONS TRG
    ON TRG.ID_REGION = CCD.FID_COMPANY_REGION 
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
  WHERE  (FID_SOURCE = I_CHANNEL OR I_CHANNEL IS NULL)
     AND (ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
  )
, SUM_TICKETS AS --ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
 (  SELECT
    DECODE(GROUPING(FT.START_PERIOD)
                ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
  , FT.TYPE_NAME_LEVEL_2
  , FT.CLASS_TYPE
  , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
  , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
  , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
  , MAX(FT.ORD) AS ORD
  , COUNT(CASE WHEN ATC.CODE_REGION = 0 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_0
  , COUNT(CASE WHEN ATC.CODE_REGION = 1 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_1
  , COUNT(CASE WHEN ATC.CODE_REGION = 2 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_2
  , COUNT(CASE WHEN ATC.CODE_REGION = 3 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_3
  , COUNT(CASE WHEN ATC.CODE_REGION = 4 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_4
  , COUNT(CASE WHEN ATC.CODE_REGION = 5 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_5
  , COUNT(CASE WHEN ATC.CODE_REGION = 6 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_6
  , COUNT(CASE WHEN ATC.CODE_REGION = 7 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_7
  , COUNT(CASE WHEN ATC.CODE_REGION = 8 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_8
  , COUNT(CASE WHEN ATC.CODE_REGION = 9 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_9
  , COUNT(CASE WHEN ATC.CODE_REGION = 10 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_10
  , COUNT(CASE WHEN ATC.CODE_REGION = 11 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_11
  , COUNT(CASE WHEN ATC.CODE_REGION = 12 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_12
  , COUNT(CASE WHEN ATC.CODE_REGION = 13 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_13
  , COUNT(CASE WHEN ATC.CODE_REGION = 14 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_14
  , COUNT(CASE WHEN ATC.CODE_REGION = 15 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_15
  , COUNT(CASE WHEN ATC.CODE_REGION = 16 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_16
  , COUNT(CASE WHEN ATC.CODE_REGION = 17 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_17
  , COUNT(CASE WHEN ATC.CODE_REGION = 18 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_18
  , COUNT(CASE WHEN ATC.CODE_REGION = 19 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_19
  , COUNT(CASE WHEN ATC.CODE_REGION = 20 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_20
  , COUNT(CASE WHEN ATC.CODE_REGION = 21 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_21
  , COUNT(CASE WHEN ATC.CODE_REGION = 22 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_22
  , COUNT(CASE WHEN ATC.CODE_REGION = 23 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_23
  , COUNT(CASE WHEN ATC.CODE_REGION = 24 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_24
  , COUNT(CASE WHEN ATC.CODE_REGION = 25 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_25
  , COUNT(CASE WHEN ATC.CODE_REGION = 26 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_26
  , COUNT(CASE WHEN ATC.CODE_REGION = 27 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_27
  , COUNT(CASE WHEN ATC.CODE_REGION = 28 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_28
  , COUNT(CASE WHEN ATC.CODE_REGION = 29 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_29
  , COUNT(CASE WHEN ATC.CODE_REGION = 30 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_30
  , COUNT(CASE WHEN ATC.CODE_REGION = 31 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_31
  , COUNT(CASE WHEN ATC.CODE_REGION = 32 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_32
  , COUNT(CASE WHEN ATC.CODE_REGION = 33 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_33
  , COUNT(CASE WHEN ATC.CODE_REGION = 34 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_34
  , COUNT(CASE WHEN ATC.CODE_REGION = 35 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_35
  , COUNT(CASE WHEN ATC.CODE_REGION = 36 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_36
  , COUNT(CASE WHEN ATC.CODE_REGION = 37 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_37
  , COUNT(CASE WHEN ATC.CODE_REGION = 38 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_38
  , COUNT(CASE WHEN ATC.CODE_REGION = 39 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_39
  , COUNT(CASE WHEN ATC.CODE_REGION = 40 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_40
  , COUNT(CASE WHEN ATC.CODE_REGION = 41 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_41
  , COUNT(CASE WHEN ATC.CODE_REGION = 42 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_42
  , COUNT(CASE WHEN ATC.CODE_REGION = 43 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_43
  , COUNT(CASE WHEN ATC.CODE_REGION = 44 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_44
  , COUNT(CASE WHEN ATC.CODE_REGION = 45 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_45
  , COUNT(CASE WHEN ATC.CODE_REGION = 46 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_46
  , COUNT(CASE WHEN ATC.CODE_REGION = 47 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_47
  , COUNT(CASE WHEN ATC.CODE_REGION = 48 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_48
  , COUNT(CASE WHEN ATC.CODE_REGION = 49 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_49
  , COUNT(CASE WHEN ATC.CODE_REGION = 50 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_50
  , COUNT(CASE WHEN ATC.CODE_REGION = 51 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_51
  , COUNT(CASE WHEN ATC.CODE_REGION = 52 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_52
  , COUNT(CASE WHEN ATC.CODE_REGION = 53 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_53
  , COUNT(CASE WHEN ATC.CODE_REGION = 54 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_54
  , COUNT(CASE WHEN ATC.CODE_REGION = 55 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_55
  , COUNT(CASE WHEN ATC.CODE_REGION = 56 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_56
  , COUNT(CASE WHEN ATC.CODE_REGION = 57 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_57
  , COUNT(CASE WHEN ATC.CODE_REGION = 58 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_58
  , COUNT(CASE WHEN ATC.CODE_REGION = 59 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_59
  , COUNT(CASE WHEN ATC.CODE_REGION = 60 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_60
  , COUNT(CASE WHEN ATC.CODE_REGION = 61 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_61
  , COUNT(CASE WHEN ATC.CODE_REGION = 62 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_62
  , COUNT(CASE WHEN ATC.CODE_REGION = 63 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_63
  , COUNT(CASE WHEN ATC.CODE_REGION = 64 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_64
  , COUNT(CASE WHEN ATC.CODE_REGION = 65 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_65
  , COUNT(CASE WHEN ATC.CODE_REGION = 66 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_66
  , COUNT(CASE WHEN ATC.CODE_REGION = 67 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_67
  , COUNT(CASE WHEN ATC.CODE_REGION = 68 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_68
  , COUNT(CASE WHEN ATC.CODE_REGION = 69 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_69
  , COUNT(CASE WHEN ATC.CODE_REGION = 70 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_70
  , COUNT(CASE WHEN ATC.CODE_REGION = 71 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_71
  , COUNT(CASE WHEN ATC.CODE_REGION = 72 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_72
  , COUNT(CASE WHEN ATC.CODE_REGION = 73 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_73
  , COUNT(CASE WHEN ATC.CODE_REGION = 74 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_74
  , COUNT(CASE WHEN ATC.CODE_REGION = 75 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_75
  , COUNT(CASE WHEN ATC.CODE_REGION = 76 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_76
  , COUNT(CASE WHEN ATC.CODE_REGION = 77 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_77
  , COUNT(CASE WHEN ATC.CODE_REGION = 78 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_78
  , COUNT(CASE WHEN ATC.CODE_REGION = 79 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_79
  , COUNT(CASE WHEN ATC.CODE_REGION = 83 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_83
  , COUNT(CASE WHEN ATC.CODE_REGION = 86 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_86
  , COUNT(CASE WHEN ATC.CODE_REGION = 87 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_87
  , COUNT(CASE WHEN ATC.CODE_REGION = 89 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_89
  , COUNT(CASE WHEN ATC.CODE_REGION = 91 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_91
  , COUNT(CASE WHEN ATC.CODE_REGION = 92 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_92
  , COUNT(CASE WHEN ATC.CODE_REGION = 99 THEN ATC.TYPE_NAME_LEVEL_1 ELSE NULL END ) AS COUNT_CITY_99
  
  
  , COUNT(ATC.TYPE_NAME_LEVEL_1) AS COUNT_ALL
 
 
 
  FROM ALL_TICKETS ATC
  RIGHT JOIN FORMAT FT ON FT.TYPE_NAME_LEVEL_1 = ATC.TYPE_NAME_LEVEL_1
                      AND FT.TYPE_NAME_LEVEL_2 = ATC.TYPE_NAME_LEVEL_2
                      AND FT.CLASS_TYPE = ATC.CLASS_TYPE
                      AND FT.START_PERIOD = ATC.PERIOD

  GROUP BY ROLLUP(FT.START_PERIOD,FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)

  ORDER BY GROUPING(FT.START_PERIOD),FT.START_PERIOD,ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  )
--  SELECT * FROM SUM_TICKETS;

  SELECT
    *
  FROM SUM_TICKETS
  WHERE (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
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
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_tickets_statistic_regions pipelined;
  


---------------------------------------------------------------
--  5.14 Статистика по полномочиям (TICKETS_D_COMPANY_TYPES) --
---------------------------------------------------------------
CURSOR cur_tickets_statistic_COMPANY (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_REGION NUMBER --ФИЛЬТР ПО РЕГИОНАМ
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL
  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL)
, PERIODS AS (
        SELECT
                CAST(GREATEST(PERIOD_START_TIME, CAST(I_INIT_TIME AS TIMESTAMP)) AS TIMESTAMP) AS START_PERIOD,
                CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
                TO_CHAR(GREATEST(PERIOD_START_TIME, CAST(I_INIT_TIME AS TIMESTAMP)),'dd.mm.yyyy') AS VIEW_PERIOD
        --          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
        --          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
                COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                        NVL2(LOWER(I_GROUP), CAST(TRUNC(CAST(I_INIT_TIME AS TIMESTAMP)) AS TIMESTAMP), CAST(I_INIT_TIME AS TIMESTAMP))
                        , CAST(I_FINISH_TIME AS TIMESTAMP), NVL(LOWER(I_GROUP), 'year')
                )
        )
)
, COMPANY_TYPE_FOR_FILTER AS (--ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
        SELECT
                ID_COMPANY_TYPE
                , NAME AS FULL_NAME
                , COALESCE(SHORT_NAME, NAME) AS NAME
                , CASE
                        WHEN NAME = 'Гражданин' THEN 'По гражданам'
                        WHEN NAME = 'Не определено' THEN 'Не определено'
                        ELSE 'По организациям'
                END AS CLIENT_TYPE
        FROM TICKETS_D_COMPANY_TYPES
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
        SELECT distinct   --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
                PR.*,
                CTP.ID_COMPANY_TYPE,
                CTP.COMPANY_NAME,
                CTP.ord_for_company,
                TTP.*
        FROM PERIODS PR
                , (SELECT 
                        ID_COMPANY_TYPE,
                        COALESCE(SHORT_NAME, NAME) AS COMPANY_NAME,
                        ID_COMPANY_TYPE as ord_for_company
                FROM TICKETS_D_COMPANY_TYPES CTP
                UNION
                SELECT 
                        0 AS ID_COMPANY_TYPE,
                        'Не указан' AS NAME,
                        1000 as ord_for_company
                FROM DUAL   
                ) CTP
        , ALL_TYPES_FOR_FORMAT TTP
        --  ORDER BY PR.START_PERIOD,CTP.ord,TTP.ID_TYPE
)
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
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
)
, ALL_TICKETS_TYPES AS (--КлассификаторЫ
        SELECT
                TCK.ID_TICKET AS ID_TICKET
                , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
                , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
                , MAX((CASE WHEN CTPF.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
                , MAX(ADT.NAME) AS ADMIN_TYPE
                , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
                , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
        FROM  TICKETS TCK
                JOIN MAIL_MESSAGES MSG
                    ON MSG.FID_TICKET = TCK.ID_TICKET
                JOIN TICKETS_HAS_TYPES TTP
                    ON TTP.FID_TICKET = TCK.ID_TICKET
                LEFT JOIN ALL_TYPES TDT  --MUST JOIN
                    ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
                LEFT JOIN TICKETS_HAS_CMP_TPS CTP
                    ON CTP.FID_TICKET = TCK.ID_TICKET
                LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
                    ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
                LEFT JOIN TICKETS_D_ADM_TYPES ADT
                    ON ADT.ID_TYPE = TCK.FID_ADM_TYPE 
                LEFT JOIN BLOCK_MAILS BML
                 ON BML.FID_MESSAGE = MSG.ID_MESSAGE    
        WHERE
                (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >=I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) <I_FINISH_TIME)
            AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017    
        GROUP BY TCK.ID_TICKET
)
, ALL_CALLS_PREV AS (
        SELECT 
                CL.SESSION_ID
                , MAX(CL.ID_CALL) AS ID_CALL
        FROM CORE_CALLS CL
        WHERE CL.CREATED_AT >= I_INIT_TIME AND CL.CREATED_AT < I_FINISH_TIME
        GROUP BY CL.SESSION_ID
)
, ALL_CALLS AS (
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
                        WHERE cast(I_FINISH_TIME as timestamp)> TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME
                        UNION ALL
                        SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
                        WHERE (cast(I_FINISH_TIME as timestamp) <= TRUNC(SYSTIMESTAMP) AND I_INIT_TIME <= I_FINISH_TIME)
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
, ALL_TICKETS AS (
  --E-mail
  SELECT
    TTP.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1
  , TTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2
  , TTP.CLASS_TYPE AS CLASS_TYPE
  , TTP.ADMIN_TYPE AS ADMIN_TYPE 
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
       END) AS TYPE_NAME_LEVEL_1 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
    , (CASE
        WHEN CL.FID_RESULT IN (5,6,7,8)
         THEN 'Посторонний звонок'
        WHEN CL.FID_RESULT = 4
         THEN 'Тестовое обращение'
       END) AS TYPE_NAME_LEVEL_2 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)  
    , (CASE
        WHEN CL.FID_RESULT IN (5,6,7,8)
         THEN '-'
        WHEN CL.FID_RESULT = 4
         THEN 'Гражданин'
       END) AS CLASS_TYPE   
   , '' AS ADMIN_TYPE
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
   LEFT JOIN CALLS_TYPE CLT
    ON CLT.SESSION_ID = CL.SESSION_ID 
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
     CLT.TYPE_NAME_LEVEL_1
   , CLT.TYPE_NAME_LEVEL_2
   , (CASE
        WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
        ELSE 'НЕ гражданин'
       END) AS CLASS_TYPE
   , CLT.ADMIN_TYPE
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
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_COMPANY_TYPES CPT
    ON CPT.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   LEFT JOIN CALLS_TYPE CLT
    ON CLT.SESSION_ID = CL.SESSION_ID
--   LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
--    ON CTPF.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE
   WHERE  CL.CREATED_AT >=I_INIT_TIME AND CL.CREATED_AT <I_FINISH_TIME
     AND (CCD.FID_COMPANY_REGION =I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
   --   AND (CTPF.CLIENT_TYPE = I_CLIENT_TYPE OR I_CLIENT_TYPE IS NULL)
   --   AND (CTPF.ID_COMPANY_TYPE = I_COMPANY_TYPE OR I_COMPANY_TYPE IS NULL)
      AND CL.FID_RESULT in (1,2,3,9)
      AND ACL.ANS_CALL = 1

  )    

  , SUM_TICKETS AS 
  ( 
   SELECT
     FID_COMPANY_TYPE
   , TYPE_NAME_LEVEL_1
   , TYPE_NAME_LEVEL_2
   , CLASS_TYPE
   , PERIOD
   , SUM(CASE
       WHEN LINE = 'INCOMING'
       THEN 1
       ELSE 0
     END) AS INCOMING_FIRST
   , SUM(CASE
       WHEN LINE = 'MAILREADER'
       THEN 1
       ELSE 0
     END) AS MAILREADER

    FROM  ALL_TICKETS
    
    WHERE (ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
     GROUP BY TYPE_NAME_LEVEL_1,TYPE_NAME_LEVEL_2,CLASS_TYPE, PERIOD, FID_COMPANY_TYPE

  
  )
  
, SUM_TICKETS_2 AS (--ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
SELECT
    DECODE(GROUPING(FT.COMPANY_NAME)
                ,0,FT.COMPANY_NAME,'Всего') AS COMPANY_NAME --Полномочие
 
  , FT.TYPE_NAME_LEVEL_1
  , FT.TYPE_NAME_LEVEL_2
  , FT.CLASS_TYPE
  , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
  , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
  , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
  , MAX(FT.ORD) AS ORD
  , MAX(ord_for_company) AS ord_for_company
  , SUM(NVL(ST.INCOMING_FIRST,0)) AS INCOMING_FIRST --Входящая линия                
  , SUM(NVL(ST.MAILREADER,0)) AS MAILREADER  --MailReader
  , SUM(NVL(ST.MAILREADER,0)+NVL(ST.INCOMING_FIRST,0)) AS ITOGO --Итого
  FROM SUM_TICKETS ST
  RIGHT JOIN FORMAT FT ON 
             FT.ID_COMPANY_TYPE = ST.FID_COMPANY_TYPE
         AND FT.TYPE_NAME_LEVEL_1 = ST.TYPE_NAME_LEVEL_1
         AND FT.TYPE_NAME_LEVEL_2 = ST.TYPE_NAME_LEVEL_2
         AND FT.CLASS_TYPE = ST.CLASS_TYPE
         AND FT.START_PERIOD = ST.PERIOD

  GROUP BY ROLLUP(FT.COMPANY_NAME, FT.START_PERIOD,FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)

  ORDER BY  GROUPING(FT.START_PERIOD),FT.START_PERIOD,ord_for_company,ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  )
  
  SELECT
    COMPANY_NAME
  , TYPE_NAME_LEVEL_1 --Классификация по теме
  , TYPE_NAME_LEVEL_2 --Классификация по теме 2 LEVEL  
  , CLASS_TYPE --Классификатор для полномочия
  , NVL2(I_GROUP, PERIOD,'') AS PERIOD
  , INCOMING_FIRST --Голос 1-Я (МЫ ТУТ СКЛЕИВАЕМ 2 ЛИНИИ В ОДНУ)
  , DECODE(TYPE_NAME_LEVEL_1,
                     'Посторонний звонок','-',
                     MAILREADER) AS MAILREADER --E-mail
  , ITOGO --Итого

  FROM
  SUM_TICKETS_2
  WHERE-- (LAST_TYPE is not null OR COMPANY_NAME = 'Всего') --Убираем промежуточные суммы
  (PERIOD is not null AND COMPANY_NAME is not null AND TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) OR COMPANY_NAME = 'Всего' --Убираем промежуточные суммы
  ;

  TYPE t_tickets_statistic_COMPANY IS TABLE OF cur_tickets_statistic_COMPANY%rowtype;


  FUNCTION fnc_tickets_statistic_COMPANY
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_COMPANY_REGION NUMBER  --ФИЛЬТР ПО РЕГИОНАМ
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_tickets_statistic_COMPANY pipelined;
  

   ---------------------------------------------------------------
-- 5.10 Статистика по классификациям в разрезе полномочий       --
---------------------------------------------------------------
CURSOR cur_statistic_ON_COMPANY_TRN (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2
      , I_CHANNEL VARCHAR2 --КАНАЛ
      , I_GROUP VARCHAR2 DEFAULT NULL
  )
IS
    WITH
   GIS_ZHKH AS (SELECT * FROM DUAL)
, PERIODS AS(
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
          TO_CHAR(GREATEST(PERIOD_START_TIME, I_INIT_TIME),'dd.mm.yyyy hh24:mi') || ' - ' ||
          TO_CHAR(PERIOD_FINISH_TIME,'dd.mm.yyyy hh24:mi') AS VIEW_PERIOD
        FROM TABLE(
              COMMON_V2.PKG_DATETIME_UTILS.FNC_GET_PERIODS_OF_TIME(
                I_INIT_TIME,
                I_FINISH_TIME,
                NVL(LOWER(I_GROUP), 'year'))))
 ,  CALLS_TYPE AS ( --Первый выбранный тип при ответе на вопросы --ZHKKH-917
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

      SELECT distinct   --ВСЕ ЭТO МОДИФИКАЦИ ДЛЯ КОРРЕКТНОЙ СОРТИРОВКИ СТАТУСОВ ПРИ ВЫВОДЕ
        PR.*,
        TTP.*
      FROM PERIODS PR, ALL_TYPES_FOR_FORMAT TTP
    
                  -- ORDER BY PR.START_PERIOD,TTP.ID_TYPE
  )  
  
, COMPANY_TYPE_FOR_FILTER AS   (--ТАБЛИЦА TICKETS_D_COMPANY_TYPES ПРЕОБРАЗОВАННАЯ ПОД ФИЛЬТРЫ
SELECT
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
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
)

, ALL_TICKETS_TYPES AS   (--КлассификаторЫ
SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
  , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
  , MAX((CASE WHEN CTPF.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
  , MAX(ADT.NAME) AS ADMIN_TYPE
  , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
  , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN ALL_TYPES TDT  --MUST JOIN
   ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN COMPANY_TYPE_FOR_FILTER CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  LEFT JOIN TICKETS_D_ADM_TYPES ADT
   ON ADT.ID_TYPE = TCK.FID_ADM_TYPE 
  LEFT JOIN BLOCK_MAILS BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE  
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >=I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) <I_FINISH_TIME)
    AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017    
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
              WHERE I_FINISH_TIME > cast(TRUNC(SYSTIMESTAMP) as timestamp) AND I_INIT_TIME <= I_FINISH_TIME
              UNION ALL
              SELECT tab.*, 1 AS RN FROM TABLE_DATA_INC_CALL TAB
              WHERE (I_FINISH_TIME <= cast(TRUNC(SYSTIMESTAMP) as timestamp) AND I_INIT_TIME <= I_FINISH_TIME)
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
, ALL_TICKETS AS (
  --E-mail
  SELECT
    cast(TTP.TYPE_NAME_LEVEL_1 as varchar2(255)) AS TYPE_NAME_LEVEL_1
  , cast(TTP.TYPE_NAME_LEVEL_2 as varchar2(255)) AS TYPE_NAME_LEVEL_2
  , cast(TTP.CLASS_TYPE as varchar2(255)) AS CLASS_TYPE
  , cast(TTP.ADMIN_TYPE as varchar2(255)) AS ADMIN_TYPE 
  , cast('E_MAIL' as varchar2(255)) AS LINE
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
    WHERE/* (TCK.FID_COMPANY_REGION = I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
      AND*/ TCK.IS_ACTIVE = 1

  UNION ALL
  -----------------------------------
--   Голос
  -----------------------------------
   SELECT
    cast((CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
       else ' '
     END) as varchar2(255)) AS TYPE_NAME_LEVEL_1 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)
   , cast((CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN 'Посторонний звонок'
      WHEN CL.FID_RESULT = 4
       THEN 'Тестовое обращение'
     END)as varchar2(255)) AS TYPE_NAME_LEVEL_2 -- Посторонний звонок (РАЗДЕЛЕНО НА 2 КАТЕГОРИИ)  
   , cast((CASE
      WHEN CL.FID_RESULT IN (5,6,7,8)
       THEN '-'
      WHEN CL.FID_RESULT = 4
       THEN 'Гражданин'
     END)as varchar2(255)) AS CLASS_TYPE   
   , '' AS ADMIN_TYPE
   , 'Голос' AS LINE
   , nvl(CCD.FID_COMPANY_TYPE,0) as FID_COMPANY_TYPE
   , PR.START_PERIOD AS PERIOD --ACL.PERIOD AS PERIOD
   , ACL.ANS_CALL_SECOND
   FROM
   ALL_CALLS ACL
   JOIN CORE_CALLS CL
    ON ACL.ID_CALL = CL.ID_CALL
   JOIN PERIODS PR
    ON CL.CREATED_AT >= cast(PR.START_PERIOD as timestamp) AND CL.CREATED_AT <  cast(PR.STOP_PERIOD  as timestamp)
   LEFT JOIN INC_CALL_CONTACT_DATA CCD
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   WHERE  CL.CREATED_AT >=I_INIT_TIME AND CL.CREATED_AT <I_FINISH_TIME
    -- AND (CCD.FID_COMPANY_REGION =I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
      AND CL.FID_RESULT IN (4,5,6,7,8,10,11)
      AND ACL.ANS_CALL = 1

  UNION ALL
  --Голос
   SELECT
    cast(CLT.TYPE_NAME_LEVEL_1 as varchar2(255)) as TYPE_NAME_LEVEL_1
   , cast(CLT.TYPE_NAME_LEVEL_2 as varchar2(255)) as TYPE_NAME_LEVEL_2
   , (CASE
      WHEN CPT.NAME = 'Гражданин' THEN 'Гражданин'
      ELSE 'НЕ гражданин'
     END) AS CLASS_TYPE
   , cast(CLT.ADMIN_TYPE as varchar2(255)) as ADMIN_TYPE
   , 'Голос' AS LINE
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
    ON CCD.FID_CALL = CL.ID_CALL AND CCD.IS_PRIMARY = 1
   JOIN TICKETS_D_TYPES TTP
    ON TTP.ID_TYPE = CCD.FID_TYPE
   JOIN TICKETS_D_COMPANY_TYPES CPT
    ON CPT.ID_COMPANY_TYPE = CCD.FID_COMPANY_TYPE 
   LEFT JOIN CALLS_TYPE CLT
    ON CLT.SESSION_ID = CL.SESSION_ID 
   WHERE  CL.CREATED_AT >=I_INIT_TIME AND CL.CREATED_AT <I_FINISH_TIME
   --  AND (CCD.FID_COMPANY_REGION =I_COMPANY_REGION OR I_COMPANY_REGION IS NULL)
      AND CL.FID_RESULT in (1,2,3,9)
      AND ACL.ANS_CALL = 1

  )  
  , SUM_TICKETS AS ( 
   SELECT   
      DECODE(GROUPING(FT.START_PERIOD)
                  ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
    , FT.TYPE_NAME_LEVEL_2
    , FT.CLASS_TYPE
    , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
    , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
    , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
    , MAX(FT.ORD) AS ORD
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 0 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_0 --НЕ ЗАДАНО
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 1 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_1
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 2 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_2
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 3 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_3
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 4 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_4
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 5  THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_5
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 6  THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_6
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 7  THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_7
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 8  THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_8
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 9  THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_9
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 10 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_10
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 11 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_11
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 12 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_12
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 13 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_13
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 14 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_14
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 15 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_15
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 16 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_16
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 17 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_17  
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 18 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_18
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 19 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_19
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 20 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_20
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 21 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_21
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 22 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_22
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 23 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_23
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 24 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_24
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 25 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_25
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 26 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_26
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 27 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_27
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 28 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_28
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 29 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_29
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 30 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_30
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 31 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_31
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 32 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_32
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 33 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_33
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 34 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_34
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 35 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_35
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 36 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_36
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 37 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_37
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 38 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_38
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 39 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_39
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 40 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_40
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 41 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_41
    , COUNT(CASE WHEN FID_COMPANY_TYPE = 42 THEN TCT.TYPE_NAME_LEVEL_1 ELSE NULL END) AS COUNT_COMPANY_42
    
    , COUNT(TCT.TYPE_NAME_LEVEL_1) AS COUNT_ALL

    FROM ALL_TICKETS TCT
    RIGHT JOIN FORMAT FT ON
                          FT.TYPE_NAME_LEVEL_1 = TCT.TYPE_NAME_LEVEL_1
                      AND FT.TYPE_NAME_LEVEL_2 = TCT.TYPE_NAME_LEVEL_2
                      AND FT.CLASS_TYPE = TCT.CLASS_TYPE
                      AND FT.START_PERIOD = TCT.PERIOD
    WHERE (LINE = I_CHANNEL OR I_CHANNEL IS NULL)  
        AND (ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL 
        OR (I_ADMIN_TYPE = 'Не задан' AND ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
    
    GROUP BY ROLLUP(FT.START_PERIOD,FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)
    ORDER BY GROUPING(FT.START_PERIOD),FT.START_PERIOD,ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2  
)
select * from SUM_TICKETS
WHERE (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
;

  TYPE t_statistic_ON_COMPANY_TRN IS TABLE OF cur_statistic_ON_COMPANY_TRN%rowtype;


  FUNCTION fnc_statistic_ON_COMPANY_TRN
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2
      , I_CHANNEL VARCHAR2 --КАНАЛ
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_statistic_ON_COMPANY_TRN pipelined;



  ----------------------------------------------------------------
  --              ОТЧЕТ ПО РАСЧЕТУ ОСС                          --
  ----------------------------------------------------------------

 CURSOR cur_calculation_occ (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_LOCATION VARCHAR2 := NULL
  )
IS
  WITH
  -----------------------------------------
  --Получаем информацию по вызовам
  -----------------------------------------
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
      ),  
  CALLS AS--Подумать, если ли тут дубли?????
(
SELECT
         TAB.OPR_LOGIN AS LOGIN
        , COUNT(DISTINCT
                CASE
                  WHEN TAB.CALL_RESULT_NUM = 1
                    AND TAB.CONNECT_RESULT_NUM = 2 -- ЧТОБЫ НЕ ПОПАДАЛИ БЛОКИРОВАННЫЕ
                  THEN TAB.SESSION_ID
                END) AS ANS_CALL --Отвеченные операторами
        , SUM(CASE
                WHEN TAB.CALL_RESULT_NUM = 1
                THEN TAB.SERVISE_CALL_DUR
              END) AS ALL_TIME--WORK_TIME -- Суммарное время обработки вызова

FROM DATA_INC_CALL_2 TAB

WHERE
  --CALLER NOT IN ('4957392201','957392201')
                  --По заявке ZHKKH-490:
        --С первого декабря по другому учитываются номера, в которых меньше 10-ти цифр
        --До первого ноября нужно вообще не отсекать тестовые звонки доработка 02.02.2016
          (
          (TAB.CALL_CREATED>=to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and TAB.CALLER NOT IN ('4957392201','957392201'))
       OR ((TAB.CALL_CREATED<  to_timestamp('01.12.2015 00:00:00','dd.mm.yyyy hh24:mi:ss') and
            TAB.CALL_CREATED>= to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss')) and substr(TAB.CALLER, -10) NOT IN ('4957392201'))
       OR (TAB.CALL_CREATED<  to_timestamp('01.11.2015 00:00:00','dd.mm.yyyy hh24:mi:ss'))
          )
  AND TAB.OPR_LOGIN IS NOT NULL
GROUP BY TAB.OPR_LOGIN
)
 --------------------------------------------
 --Теперь считаем время обработки письма
 --------------------------------------------
,
ALL_CHANGE AS --ВСЕ ИЗМЕНЕНИЯ ПИСЬМА
(SELECT
   CLG.ID_CHANGE_LOG AS ID_CHANGE_LOG
 , CLG.FID_MESSAGE AS FID_MESSAGE
 , US.LOGIN AS LOGIN
 , CLG.ACTION_TIME AS ACTION_TIME
 , ACT.CODE AS CODE
  FROM MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
  JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
   ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = CLG.FID_USER

  WHERE
      (CLG.ACTION_TIME >= I_INIT_TIME AND CLG.ACTION_TIME < I_FINISH_TIME +1)
  AND ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND US.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol', 'v.v.iliykhin_gis_zhkh_Vol','t.aitkaliev') -- ДЛЯ ЗАЯВКИ ZHKKH-473
)
,
INTERVALS AS (
 SELECT
   FID_MESSAGE
 , LOGIN
 , ACTION_TIME
 , CODE
 , LAG (ACTION_TIME,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_ORDER_DATE
 , LAG(CODE,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_CODE
 FROM ALL_CHANGE
 ORDER BY
 FID_MESSAGE, ACTION_TIME
 )
, CALCULATION_MAILS AS ( --Расчет времени по письмам--ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
 SELECT
 --FID_MESSAGE
   LOGIN
 , count(distinct FID_MESSAGE) as MESSAGES_COUNT
 , ceil(SUM((NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)))) as ALL_TIME
 FROM INTERVALS
 WHERE CODE IN ('assign') AND PREV_CODE = 'open'
 AND (PREV_ORDER_DATE >= I_INIT_TIME AND PREV_ORDER_DATE < I_FINISH_TIME)
 GROUP BY/* FID_MESSAGE,*/ LOGIN)
 --------------------------------------------
 --Теперь считаем время обработки обращений--
 --------------------------------------------
, ACTIONS_LOGS AS
(SELECT
   ACL.ID_ACTION,
   ACL.LOGGABLE_ID as LOG_ID --НОМЕР ТИКЕТА
 , LAG(ACL.ID_ACTION,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS PREV_ID_ACTION
 , LAG(ACL.LOGGABLE_ID,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS PREV_LOG_ID
 , LAG(ACL.CREATED_AT,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS START_TIME
 , LAG(ACT.CODE,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS START_CODE
 , ACL.CREATED_AT AS FINISH_TIME
 , ACT.CODE AS FINISH_CODE
 , ACL.FID_USER AS FID_USER
 FROM
   USER_ACTIONS_LOG ACL
   JOIN USER_ACTION_TYPES ACT
    ON ACT.ID_TYPE = ACL.FID_TYPE
 WHERE
       LOGGABLE_TYPE = 'TICKETS'
   AND ACT.CODE IN ('ticket-assigned','ticket-new-answer-sent','ticket-no-answer','ticket-created','ticket-blocked','ticket-unblocked')
   AND (ACL.CREATED_AT >= I_INIT_TIME) AND (ACL.CREATED_AT < I_FINISH_TIME + 1)

)
, BIND_MESSAGES AS
(SELECT
   RELATIONABLE_TYPE
 , RELATIONABLE_ID
 , FID_ACTION
 , CREATED_AT
  FROM
 USER_ACTION_RELATIONS ACR
 WHERE
   ACR.RELATIONABLE_TYPE = 'MAIL_MESSAGES'
)
, ACTIONS_LOGS_2 AS
(SELECT DISTINCT--Двойные события
   ACL.ID_ACTION AS ID_ACTION
 , ACL.LOG_ID AS LOG_ID --НОМЕР ТИКЕТА
 , ACL.START_TIME AS START_TIME
 , ACL.START_CODE AS START_CODE
 , ACL.FINISH_TIME AS FINISH_TIME
 , ACL.FINISH_CODE AS FINISH_CODE
 , US.LOGIN AS LOGIN
 , ACL.PREV_ID_ACTION AS PREV_ID_ACTION
 , BMS.RELATIONABLE_ID AS ID_MAIL --ПРИВЯЗАННОЕ ПИСЬМО
 FROM
  ACTIONS_LOGS ACL
 LEFT JOIN USER_ACTION_RELATIONS ACR
  ON ACR.FID_ACTION = ACL.ID_ACTION
 LEFT JOIN BIND_MESSAGES BMS
  ON BMS.FID_ACTION = ACL.PREV_ID_ACTION
 LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = ACL.FID_USER
 WHERE
        (ACL.START_TIME >= I_INIT_TIME) AND (ACL.START_TIME < I_FINISH_TIME)
   AND    ACL.LOG_ID = ACL.PREV_LOG_ID
   AND ((ACL.START_CODE = 'ticket-assigned' and ACL.FINISH_CODE IN ('ticket-new-answer-sent','ticket-no-answer'))
     OR (ACL.START_CODE IN ('ticket-created') and ACL.FINISH_CODE = 'ticket-unblocked')
     OR (ACL.START_CODE IN ('ticket-blocked') and ACL.FINISH_CODE = 'ticket-unblocked')
       )
)
, CALCULATION_TICKETS AS --ТУТ Я ПРОСТО БЕРУ ОПЕРАТОРА И ВРЕМЯ (ПОСКОЛЬКУ ТУТ УЧИТЫВАЮТСЯ НЕ ВСЕ ПИСЬМА)
 (
 SELECT
   ACL.LOGIN AS LOGIN
 , ceil(SUM((NAUCRM.intervaltosec(ACL.FINISH_TIME - ACL.START_TIME)))) as ALL_TIME
 FROM
 ACTIONS_LOGS_2 ACL
 GROUP BY ACL.LOGIN
 )
 --------------------------------------------
 --Расчет табельного времени
 --------------------------------------------
, WORK_HOURS as
(
  SELECT
    SUM (CASE
          WHEN SC.STATUS = 'available'-- IN ('ringing' , 'speaking'  , 'wrapup'  , 'normal')--  , 'custom1' , 'custom2' , 'custom3')
           THEN SC.DURATION
           ELSE 0
         END
        ) AS ALL_TIME--OPERATOR_WORK_HOURS
  , US.LOGIN AS LOGIN     --Логин оператора
  FROM NAUCRM.STATUS_CHANGES SC
  JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.LOGIN = sc.LOGIN
  WHERE
        SC.ENTERED >= I_INIT_TIME
    AND SC.ENTERED <  I_FINISH_TIME
    AND( LOWER(US.LOGIN) LIKE '%_gis_zhkh%'
     or
    LOWER(US.LOGIN) LIKE '%_emvol')
    AND LOWER(US.LOGIN) NOT LIKE 'test_gis_zhkh_%'
    AND US.LOGIN NOT LIKE 'dev_gis_zhkh%'
    AND US.LOGIN != 'z.o.gis_gis_zhkh_edu_Vol'
  GROUP BY US.LOGIN
)

 --------------------------------------------
 --Обьединение и вывод результатов
 --------------------------------------------
, FINAL_DATA_PREV AS
 (
 SELECT
   COALESCE(CL.LOGIN,WH.LOGIN,ML.LOGIN,TCK.LOGIN) AS LOGIN
 , NVL(CL.ANS_CALL,0) AS CALLS_COUNT
 , NVL(CL.ALL_TIME,0) AS CALLS_TIME
 , NVL(ML.MESSAGES_COUNT,0) AS MESSAGES_COUNT
 , NVL(TCK.ALL_TIME,0) + NVL(ML.ALL_TIME,0)    AS MESSAGES_TIME
 , NVL(WH.ALL_TIME,0) AS WORK_HOURS
 FROM
           CALLS CL
 FULL JOIN WORK_HOURS WH
  ON WH.LOGIN = CL.LOGIN
 FULL JOIN CALCULATION_MAILS ML
  ON ML.LOGIN = WH.LOGIN
 FULL JOIN CALCULATION_TICKETS TCK
  ON TCK.LOGIN = ML.LOGIN
 WHERE NVL(CL.ANS_CALL,0) !=0 OR  NVL(CL.ALL_TIME,0) !=0 OR NVL(ML.MESSAGES_COUNT,0) !=0
  OR NVL(ML.ALL_TIME,0) + NVL(TCK.ALL_TIME,0) !=0 OR NVL(WH.ALL_TIME,0) !=0
 ORDER BY LOGIN
 )
, FINAL_DATA AS (
 SELECT
    FD.LOGIN
  , US.SURNAME ||' '|| US.NAME ||' '|| SUBSTR(US.PATRONYMIC, 1, INSTR(US.PATRONYMIC,'_',1,1)-1) AS FIO 
  , SUM(FD.CALLS_COUNT) AS CALLS_COUNT
  , SUM(FD.CALLS_TIME) AS CALLS_TIME
  , SUM(FD.MESSAGES_COUNT) AS MESSAGES_COUNT
  , SUM(FD.MESSAGES_TIME) AS MESSAGES_TIME
  , SUM(FD.WORK_HOURS) AS WORK_HOURS
  FROM FINAL_DATA_PREV FD
  JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.LOGIN = FD.LOGIN
  WHERE FD.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol',
                      'v.v.iliykhin_gis_zhkh_Vol',
                      'o.i.ruskhanova_gis_zhkh_Vol',
                      's.v.srybnaia_gis_zhkh_Vol',
                      'a.horolskiy',
                      'v.v.iliykhin_gis_zhkh_Vol',
                      't.aitkaliev',
                      'y.dudkin') --не нужно учитывать эти логины
   AND (US.FID_LOCATION = I_LOCATION OR I_LOCATION IS NULL)                    
  GROUP BY FD.LOGIN, US.SURNAME, US.NAME, US.PATRONYMIC
  ORDER BY FD.LOGIN
 )
, SUM_FINAL_DATA AS
 (
 SELECT
 'Итого:' AS LOGIN
 , SUM(CALLS_COUNT) AS CALLS_COUNT
 , SUM(CALLS_TIME) AS CALLS_TIME
 , SUM(MESSAGES_COUNT) AS MESSAGES_COUNT
 , SUM(MESSAGES_TIME) AS MESSAGES_TIME
 , SUM(WORK_HOURS) AS WORK_HOURS
  FROM FINAL_DATA

 )
SELECT
   LOGIN
 , FIO  
 , CALLS_COUNT --Количество принятых  звонков
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(CALLS_TIME/DECODE(CALLS_COUNT,0,1,CALLS_COUNT))) AS CALLS_AVG_TIME --Среднее время обработки звонков, АHT
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(CALLS_TIME) AS CALLS_TIME --Общее время обработки звонков, HT
 , MESSAGES_COUNT --Количество обработанных писем
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(MESSAGES_TIME/DECODE(MESSAGES_COUNT,0,1,MESSAGES_COUNT))) AS MESSAGES_AVG_TIME --Среднее время обработки писем, АHT E-mail
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(MESSAGES_TIME) AS MESSAGES_TIME --Общее время работы с письмами, HT, мин.
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(WORK_HOURS) AS WORK_HOURS --Общее время готовности оператора к работе, AT
 , REPLACE(TRIM(TO_CHAR(NVL(MESSAGES_TIME/DECODE(WORK_HOURS,0,1,WORK_HOURS)*100,0),'9999990D99')),'.',',')||'%' AS ОСС -- ОСС,%
 FROM FINAL_DATA
UNION ALL
SELECT
   LOGIN
 , '' AS FIO
 , CALLS_COUNT --Количество принятых  звонков
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(CALLS_TIME/DECODE(CALLS_COUNT,0,1,CALLS_COUNT))) AS CALLS_AVG_TIME --Среднее время обработки звонков, АHT
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(CALLS_TIME) AS CALLS_TIME --Общее время обработки звонков, HT
 , MESSAGES_COUNT --Количество обработанных писем
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(MESSAGES_TIME/DECODE(MESSAGES_COUNT,0,1,MESSAGES_COUNT))) AS MESSAGES_AVG_TIME --Среднее время обработки писем, АHT E-mail
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(MESSAGES_TIME) AS MESSAGES_TIME --Общее время работы с письмами, HT, мин.
 , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(WORK_HOURS) AS WORK_HOURS --Общее время готовности оператора к работе, AT
 , REPLACE(TRIM(TO_CHAR(NVL(MESSAGES_TIME/DECODE(WORK_HOURS,0,1,WORK_HOURS)*100,0),'9999990D99')),'.',',')||'%' AS ОСС -- ОСС,%
 FROM SUM_FINAL_DATA
;

  TYPE t_calculation_occ IS TABLE OF cur_calculation_occ%rowtype;

  FUNCTION fnc_calculation_occ
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_LOCATION VARCHAR2 := NULL

  ) RETURN t_calculation_occ pipelined;


-----------------------------------------------------------
--              ЛОГ ПИСЕМ (ДЛЯ ВЫГРУЗКИ В ЕИС)           --
-----------------------------------------------------------

CURSOR cur_mail_log_for_eis (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS NUMBER -- СТАТУС ПИСЬМА
      , I_LOGIN VARCHAR2 -- ОПЕРАТОР
      , I_DIRECTION VARCHAR2 -- НАПРАВЛЕНИЕ
  )
IS
WITH 
GIS_ZHKH AS (SELECT * FROM DUAL),
ALL_CHANGE AS --ВСЕ ИЗМЕНЕНИЯ ПИСЬМА
(SELECT
   CLG.ID_CHANGE_LOG AS ID_CHANGE_LOG
 , CLG.FID_MESSAGE AS FID_MESSAGE
 , CLG.ACTION_TIME AS ACTION_TIME
 , ACT.CODE AS CODE
  FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
  LEFT JOIN MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
   ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
  JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
   ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
  JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
   ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
  JOIN MAIL_D_MSG_STATUSES MST --СТАТУС ПИСЬМА
   ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = CLG.FID_USER
  WHERE
      (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
       AND ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND (I_STATUS = to_char(MST.ID_MSG_STATUS) /*OR DECODE(I_STATUS,1000,' 1 4 ','') LIKE '% '|| MST.ID_MSG_STATUS ||' %'*/
       OR I_STATUS IS NULL) -- СТАТУС ПИСЬМА
  AND (MTP.DIRECTION = I_DIRECTION or I_DIRECTION is null) -- НАПРАВЛЕНИЕ
  AND US.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol', 'v.v.iliykhin_gis_zhkh_Vol','t.aitkaliev') -- ДЛЯ ЗАЯВКИ ZHKKH-473
)
,
INTERVALS AS (
 SELECT
   FID_MESSAGE
 , ACTION_TIME
 , CODE
 , LAG (ACTION_TIME,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_ORDER_DATE
 , LAG(CODE,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_CODE
 FROM ALL_CHANGE
 ORDER BY
 FID_MESSAGE, ACTION_TIME
 )
, CALCULATION AS (
 SELECT
   FID_MESSAGE
 , ceil(SUM((NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)))) as ALL_TIME
 FROM INTERVALS
 WHERE CODE IN ('assign') AND PREV_CODE = 'open'
 GROUP BY FID_MESSAGE)
, FINAL_TIME AS --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
  (
 SELECT
   FID_MESSAGE
 , common.strutils.FNC_INTERVALTOCHAR(NUMTODSINTERVAL(all_time, 'SECOND')) as processing_time

  from CALCULATION
  )
, SENDER_ADDRESSES AS --ВЫЧИСЛЯЕТ АДРЕС ОТПРАВИТЕЛЯ
  (SELECT
   ADR.FID_MESSAGE AS FID_MESSAGE
 , ADR.MAIL_ADDRESS AS MAIL_ADDRESS
  FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
  JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
   ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
  JOIN MAIL_ADDRESSES ADR --АДРЕСАТЫ
   ON ADR.FID_MESSAGE = MSG.ID_MESSAGE
JOIN MAIL_D_MSG_STATUSES MST --СТАТУС ПИСЬМА
 ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS

WHERE
      (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)

  AND (I_STATUS = to_char(MST.ID_MSG_STATUS) /*OR DECODE(I_STATUS,1000,' 1 4 ','') LIKE '% '|| MST.ID_MSG_STATUS ||' %'*/
       OR I_STATUS IS NULL) -- СТАТУС ПИСЬМА

  AND (MTP.DIRECTION = I_DIRECTION or I_DIRECTION is null) -- НАПРАВЛЕНИЕ
      AND ADR.FID_ADDRESS_TYPE = 1 -- ЗНАЧИТ ТОЛЬКО ОТПРАВИТЕЛЬ
  )
, LOGIN_OPERATORS AS
  (SELECT
     CLG.FID_MESSAGE AS FID_MESSAGE
   , MAX(US.LOGIN) KEEP (DENSE_RANK LAST ORDER BY CLG.ID_CHANGE_LOG) AS OPERATOR_LOGIN
    FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
    LEFT JOIN MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
     ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
    JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
     ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
    JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
     ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
    JOIN MAIL_D_MSG_STATUSES MST --СТАТУС ПИСЬМА
     ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
    LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
     ON US.ID_USER = MSG.FID_USER
    WHERE
       (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
   AND  ACT.CODE in ('assign','unbind') -- МЫ ВЫБИРАЕМ ТОЛЬКО "ПРИВЯЗАЛ"
   AND (I_STATUS = to_char(MST.ID_MSG_STATUS) /*OR DECODE(I_STATUS,1000,' 1 4 ','') LIKE '% '|| MST.ID_MSG_STATUS ||' %'*/
       OR I_STATUS IS NULL) -- СТАТУС ПИСЬМА
   AND (MTP.DIRECTION = I_DIRECTION or I_DIRECTION is null) -- НАПРАВЛЕНИЕ
  GROUP BY CLG.FID_MESSAGE
  )
  
  , ALL_TICKETS_TYPES AS (--КлассификаторЫ
        SELECT
            TCK.ID_TICKET AS ID_TICKET
          , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
          , MAX(TDT_LEV_1.NAME) AS CLASSIFIER_NEW_LEV_1
          , MAX(TDT_LEV_2.NAME) AS CLASSIFIER_NEW_LEV_2
          , MAX(ADT.NAME) AS ADMIN_TYPE
        
        FROM TICKETS TCK
        JOIN MAIL_MESSAGES MSG
         ON MSG.FID_TICKET = TCK.ID_TICKET
        JOIN TICKETS_HAS_TYPES TTP
         ON TTP.FID_TICKET = TCK.ID_TICKET
        LEFT JOIN TICKETS_D_TYPES TDT
         ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
        LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
         ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
        LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
         ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1 
        LEFT JOIN TICKETS_D_ADM_TYPES ADT
         ON ADT.ID_TYPE = TCK.FID_ADM_TYPE
         
        WHERE (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
        GROUP BY TCK.ID_TICKET
  )
  
, ALL_TICKETS_TYPES_2 AS ( --КлассификаторЫ ИЗ INC_CALL_CONTACT_DATA
  SELECT 
    MSG.ID_MESSAGE
  , TTP.NAME AS NAME_TYPE
  FROM MAIL_MESSAGES MSG
  JOIN INC_CALL_CONTACT_DATA INC
   ON INC.FID_MESSAGE_MAIL = MSG.ID_MESSAGE
  JOIN TICKETS_D_TYPES TTP 
   ON TTP.ID_TYPE = INC.FID_TYPE  AND TTP.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  WHERE
        (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
  ) 
 , UNBIND_MESSAGES AS (
      SELECT 
         MSG.ID_MESSAGE
       , MAX(CLG.FID_TICKET) KEEP (DENSE_RANK LAST ORDER BY CLG.ACTION_TIME) AS FID_TICKET
        
      FROM MAIL_CHANGE_LOG CLG
      JOIN MAIL_MESSAGES MSG
       ON MSG.ID_MESSAGE = CLG.FID_MESSAGE
      JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
       ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
      WHERE
            (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
        AND  ACT.CODE = 'unbind'
        
      GROUP BY MSG.ID_MESSAGE
   ) 
  
, MESSAGES AS
  (SELECT
      MSG.ID_MESSAGE                                   AS ID_MESSAGE
    , TO_CHAR(MSG.CREATED_AT,'dd.mm.yyyy hh24:mi')     AS RECEIVING_TIME
    , ADR.MAIL_ADDRESS                                 AS MAIL_ADDRESS
    , MSG.SUBJECT                                      AS SUBJECT
    , RTP.NAME                                         AS REQUESTER_NAME
    , REGEXP_REPLACE( SUBSTR(TRIM(TRIM(chr(13) FROM trim(chr(10) from REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(MSG.BODY,'<br>',chr(10)),'<style>.*</style>','',1, 0, 'nm'),'(\<(/?[^>]+)>)','')))),1,20000) , '&*;',' ')  AS MESSAGE_TEXT
    , MTP.NAME                                         AS TYPE_LETTER
    , LOP.OPERATOR_LOGIN                               AS OPERATOR_LOGIN
    , FT.PROCESSING_TIME                               AS PROCESSING_TIME --не пишется у исходящих писем
    , MST.NAME                                         AS STATUS_NAME
    , TO_CHAR(MSG.RECEIVING_TIME,'dd.mm.yyyy hh24:mi') AS SUPPORT_TIME
    , NVL(MSG.FID_TICKET,NBM.FID_TICKET)               AS ID_TICKET
    , (CASE
       WHEN MTP.CODE = 'in'
       THEN TO_CHAR(MSG.PROCESSING_TIME,'dd.mm.yyyy hh24:mi')
       ELSE ''
       END)                                            AS TICKET_TIME --Дата обработки письма (не пишется у исходящих писем)
    , COALESCE(TTP.CLASSIFIER_NEW_LEV_1, TTP.LAST_TYPE, TTP_2.NAME_TYPE) AS CLASSIFIER_NEW_LEV_1
    , TTP.CLASSIFIER_NEW_LEV_2
    , TTP.ADMIN_TYPE
       
  FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
  JOIN MAIL_D_MSG_STATUSES MST --СТАТУСЫ ПИСЕМ
   ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
  JOIN MAIL_D_REQUESTER_TYPES RTP --ЗАЯВИТЕЛЬ
   ON RTP.ID_REQUESTER_TYPE = MSG.FID_REQUESTER_TYPE
  JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
   ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
  LEFT JOIN SENDER_ADDRESSES ADR --АДРЕСАТЫ
   ON ADR.FID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN LOGIN_OPERATORS LOP
   ON LOP.FID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN FINAL_TIME FT --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
   ON FT.FID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN ALL_TICKETS_TYPES TTP
   ON TTP.ID_TICKET = MSG.FID_TICKET
  LEFT JOIN ALL_TICKETS_TYPES_2 TTP_2
   ON TTP_2.ID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN UNBIND_MESSAGES NBM
   ON NBM.ID_MESSAGE = MSG.ID_MESSAGE
  WHERE
      (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
  AND (I_STATUS = to_char(MST.ID_MSG_STATUS) /*OR DECODE(I_STATUS,1000,' 1 4 ','') LIKE '% '|| MST.ID_MSG_STATUS ||' %'*/
       OR I_STATUS IS NULL) -- СТАТУС ПИСЬМА
  AND (I_LOGIN = LOP.OPERATOR_LOGIN OR I_LOGIN IS NULL)
  AND (MTP.DIRECTION = I_DIRECTION or I_DIRECTION is null) -- НАПРАВЛЕНИЕ
  AND (ADR.MAIL_ADDRESS != 'postmaster@newcontact.su')

  )
SELECT
    ID_MESSAGE --ID
  , RECEIVING_TIME --Дата и время поступления
  , MAIL_ADDRESS --E-mail
  , SUBJECT --Тема
  , REQUESTER_NAME --Заявитель
  , MESSAGE_TEXT --Текст письма
  , TYPE_LETTER --Тип письма
  , OPERATOR_LOGIN -- Оператор
  , PROCESSING_TIME --Время обработки
  , STATUS_NAME --Статус письма
  , SUPPORT_TIME --Дата и время поступления на support
  , ID_TICKET --Привязано к обращению
  , TICKET_TIME -- Дата обработки письма (Когда привязали к обращению)
  , CLASSIFIER_NEW_LEV_1-- Классификатор 1 - УРОВЕНЬ
  , CLASSIFIER_NEW_LEV_2-- Классификатор 2 - УРОВЕНЬ
  , ADMIN_TYPE -- АДМИНИСТРАТИВНЫЙ ТИП
  
FROM MESSAGES
 ORDER BY ID_MESSAGE;

TYPE t_mail_log_for_eis IS TABLE OF cur_mail_log_for_eis%rowtype;

FUNCTION fnc_mail_log_for_eis
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_STATUS NUMBER -- СТАТУС ПИСЬМА
    , I_LOGIN VARCHAR2 -- ОПЕРАТОР
    , I_DIRECTION VARCHAR2 -- НАПРАВЛЕНИЕ
) RETURN t_mail_log_for_eis pipelined;


-----------------------------------------------------------------
--              ЛОГ ОБРАЩЕНИЙ (ДЛЯ ВЫГРУЗКИ В ЕИС)             --
-----------------------------------------------------------------

CURSOR cur_ticket_log_for_eis (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS VARCHAR2 --СТАТУС ОБРАЩЕНИЯ
      , I_METKA VARCHAR2 -- МЕТКИ
      , I_ADMIN_TYPE NUMBER := NULL --Административный тип
  )
IS
WITH 
GIS_ZHKH AS (SELECT * FROM DUAL),
FIRST_MESSAGES AS --МЫ ДОЛЖНЫ БРАТЬ ТО ПИСЬМО, КОТОРОЕ ИНИЦИИРОВАЛО ОБРАЩЕНИЕ
 (SELECT
   TCK.ID_TICKET
 , MIN(MSG.ID_MESSAGE) AS ID_MESSAGE
 FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
 JOIN TICKETS TCK --ОБРАЩЕНИЯ
   ON TCK.ID_TICKET = MSG.FID_TICKET
 WHERE (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
      --МНОЖЕСТВЕННЫЙ ВЫБОР
--      AND (I_STATUS like '% '|| TCK.FID_STATUS ||' %' OR nvl(I_STATUS,'1') = '1')--Статус обращения
      --ДЛЯ ЕИС
        AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
      AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
  GROUP BY TCK.ID_TICKET
 )

, ALL_CHANGE AS --ВСЕ ИЗМЕНЕНИЯ ПИСЬМА
(SELECT
   CLG.ID_CHANGE_LOG AS ID_CHANGE_LOG
 , CLG.FID_MESSAGE AS FID_MESSAGE
 , CLG.FID_TICKET AS FID_TICKET
 , CLG.ACTION_TIME AS ACTION_TIME
 , ACT.CODE AS CODE
FROM TICKETS TCK --ОБРАЩЕНИЯ
JOIN MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
 ON MSG.FID_TICKET = TCK.ID_TICKET
JOIN MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
 ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
 ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = CLG.FID_USER
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
   AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
   AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
   AND  ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND  US.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol', 'v.v.iliykhin_gis_zhkh_Vol','t.aitkaliev') -- ДЛЯ ЗАЯВКИ ZHKKH-473
)
, INTERVALS AS (
 SELECT
   FID_MESSAGE
 , FID_TICKET
 , ACTION_TIME
 , CODE
 , LAG (ACTION_TIME,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_ORDER_DATE
 , LAG (CODE,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_CODE
 FROM ALL_CHANGE
 ORDER BY
 FID_MESSAGE, ACTION_TIME
 )
, CALCULATION AS (
  SELECT
   FID_TICKET
 , ceil(SUM((NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)))) as ALL_TIME
 FROM INTERVALS
 WHERE CODE IN ('assign') AND PREV_CODE = 'open' AND FID_TICKET IS NOT NULL
 GROUP BY FID_TICKET)
, FINAL_TIME AS --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
  (
 SELECT
   FID_TICKET
 , common.strutils.FNC_INTERVALTOCHAR(NUMTODSINTERVAL(all_time, 'SECOND')) as processing_time

  FROM CALCULATION
  )
, SENDER_ADDRESSES AS --ВЫЧИСЛЯЕТ АДРЕС ОТПРАВИТЕЛЯ
  (SELECT
   ADR.FID_MESSAGE AS FID_MESSAGE
 , ADR.MAIL_ADDRESS AS MAIL_ADDRESS
  FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
  JOIN TICKETS TCK --ОБРАЩЕНИЯ
   ON TCK.ID_TICKET = MSG.FID_TICKET
  JOIN MAIL_ADDRESSES ADR --АДРЕСАТЫ
   ON ADR.FID_MESSAGE = MSG.ID_MESSAGE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
    AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
    AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
    AND ADR.FID_ADDRESS_TYPE = 1
  )
, TICKETS_METKS AS --МЕТКИ ДЛЯ ОБРАЩЕНИЙ
  (SELECT
     TCK.ID_TICKET AS ID_TICKET
  ,  LISTAGG(TDT.NAME,', ') WITHIN GROUP(ORDER BY TTG.FID_TAG) AS METKA
  FROM
  TICKETS TCK
  JOIN TICKETS_HAS_TAGS TTG
   ON TTG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TAGS TDT
   ON TDT.ID_TAG = TTG.FID_TAG
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
    AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
    AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
  GROUP BY TCK.ID_TICKET
  )
, ALL_TICKETS_TYPES AS (--КлассификаторЫ
  SELECT
    TCK.ID_TICKET AS ID_TICKET
  , LISTAGG(TDT.NAME,', ') WITHIN GROUP(ORDER BY TTP.ID_HAS) AS CLASSIFIER
  , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_1
  , MAX(TDT_LEV_2.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_2
  
  FROM
  TICKETS TCK
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
   ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
   ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1 
   
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
   AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
   AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
  GROUP BY TCK.ID_TICKET
  )

, ALL_TICKETS_TASKS AS--достает список телефонов контакта
    (
    SELECT FID_TICKET,
           MAX(DECODE(seq,1,TASK_CODE,NULL))  AS TASK1
        ,  MAX(DECODE(seq,2,TASK_CODE,NULL))  AS TASK2
        ,  MAX(DECODE(seq,3,TASK_CODE,NULL))  AS TASK3
        ,  MAX(DECODE(seq,4,TASK_CODE,NULL))  AS TASK4
        ,  MAX(DECODE(seq,5,TASK_CODE,NULL))  AS TASK5
        ,  MAX(DECODE(seq,6,TASK_CODE,NULL))  AS TASK6
        ,  MAX(DECODE(seq,7,TASK_CODE,NULL))  AS TASK7
        ,  MAX(DECODE(seq,8,TASK_CODE,NULL))  AS TASK8
        ,  MAX(DECODE(seq,9,TASK_CODE,NULL))  AS TASK9
        ,  MAX(DECODE(seq,10,TASK_CODE,NULL)) AS TASK10
    FROM
      (SELECT
          FID_TICKET
        , TASK_CODE
        , ROW_NUMBER()
       OVER
          (PARTITION BY
                FID_TICKET
           ORDER BY ID_TASK DESC NULLS LAST) seq
             FROM TICKETS_TASKS TSK
             JOIN TICKETS TCK
              ON TCK.ID_TICKET = TSK.FID_TICKET
             WHERE
                 (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
             AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения       
             AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
       )
       WHERE seq <= 10
       GROUP BY FID_TICKET
    )
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
)  
, ALL_TICKETS_MESSAGE AS --ВСЕ ПРИВЯЗАННЫЕ ПИСЬМА
  (SELECT
  TCK.ID_TICKET AS ID_TICKET
  ,  rtrim ( xmlcast ( xmlagg ( xmlelement ( "a", MSG.ID_MESSAGE ||', ' ) ORDER BY MSG.ID_MESSAGE ASC ) AS CLOB ), ', ' ) AS MESSAGES
  --, LISTAGG(MSG.ID_MESSAGE,', ') WITHIN GROUP(ORDER BY MSG.ID_MESSAGE) AS MESSAGES
  FROM
  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN BLOCK_MAILS BML
    ON BML.FID_MESSAGE = MSG.ID_MESSAGE 
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
   AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
   AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
   AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017
  GROUP BY TCK.ID_TICKET
  )
, ALL_COMPANY_TYPES AS
 (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , LISTAGG(TCTP.NAME, '; ') WITHIN GROUP (ORDER BY CTP.ID_HAS) AS COMPANY_TYPES
  FROM  TICKETS TCK
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_COMPANY_TYPES TCTP
   ON TCTP.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
    AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения       
    AND (TCK.FID_ADM_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND TCK.FID_ADM_TYPE IS NULL))--ZHKKH-917 --Административный тип
  GROUP BY TCK.ID_TICKET
 )
, ALL_NEW_TYPES AS ( --Пригодится если появится TICKETS.FID_TYPE
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
 
, ALL_TICKETS AS
  (SELECT
      TCK.ID_TICKET                                    AS ID_TICKET --№ обращения
    , TO_CHAR(TCK.CREATED_AT,'dd.mm.yyyy hh24:mi')     AS CREATED_AT --Дата и время создания
    , ADR.MAIL_ADDRESS                                 AS MAIL_ADDRESS --E-mail
    , MSG.SUBJECT                                      AS SUBJECT --Тема
    , RTP.NAME                                         AS REQUESTER_NAME --Заявитель
    , REGEXP_REPLACE( SUBSTR(TRIM(TRIM(chr(13) FROM trim(chr(10) from REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(MSG.BODY,'<br>',chr(10)),'<style>.*</style>','',1, 0, 'nm'),'(\<(/?[^>]+)>)','')))),1,20000) , '&*;',' ')  AS MESSAGE_TEXT --Текст письма
    , TCK.PRIORITY                                     AS PRIORITY --Приоритет
    , NVL(TTP.CLASSIFIER,TTP.CLASSIFIER_NEW_LEV_1)     AS CLASSIFIER --Классификатор-- 1 LEVEL --ZHKKH-917
    , TTP.CLASSIFIER_NEW_LEV_2                AS CLASSIFIER_LEVEL_2 --Классификатор-- 2 LEVEL--ZHKKH-917
    , ADT.NAME                             AS ADMIN_TYPE --Административный тип  --ZHKKH-917
    , TMT.METKA                                        AS METKA --Метка
    , TTS.TASK1                                        AS TASK1 --Номер заявки в Jira
    , TTS.TASK2                                        AS TASK2 --Номер заявки в Jira
    , TTS.TASK3                                        AS TASK3 --Номер заявки в Jira
    , TTS.TASK4                                        AS TASK4 --Номер заявки в Jira
    , TTS.TASK5                                        AS TASK5 --Номер заявки в Jira
    , TTS.TASK6                                        AS TASK6 --Номер заявки в Jira
    , TTS.TASK7                                        AS TASK7 --Номер заявки в Jira
    , TTS.TASK8                                        AS TASK8 --Номер заявки в Jira
    , TTS.TASK9                                        AS TASK9 --Номер заявки в Jira
    , TTS.TASK10                                       AS TASK10 --Номер заявки в Jira
    , COALESCE(TCK.UPDATED_BY, US.LOGIN)               AS OPERATOR_LOGIN --Оператор
    , FT.PROCESSING_TIME                               AS PROCESSING_TIME --Время обработки
    , TSR.NAME                                         AS SOURSE_NAME --Канал обращения
    , TST.NAME                                         AS STATUS_NAME --Статус Обращения
    , TCK.COMMENTS                                     AS COMMENTS --Комментарий оператора
    , TCM.MESSAGES                                     AS MESSAGES --Привязанные письма
    , COALESCE(TRG.NAME,'Регион не указан')            AS REGION_NAME --REGION
    , TO_CHAR(TCK.UPDATED_AT,'dd.mm.yyyy hh24:mi')     AS UPDATED_AT --Дата последнего изменени
    , CTP.COMPANY_TYPES                                AS COMPANY_TYPES -- ВСЕ ПОЛНОМОЧИЯ
    , TCK.COMPANY_OGRN                                 AS COMPANY_OGRN -- OGRN
    , TCK.COMPANY_NAME                                 AS COMPANY_NAME -- НАЗВАНИЕ КАМПАНИИ
    , TO_CHAR(nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT),'dd.mm.yyyy hh24:mi')  AS REGISTERED_AT --Время резервирования номера обращения

  FROM FIRST_MESSAGES FMS
  JOIN MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
   ON MSG.ID_MESSAGE = FMS.ID_MESSAGE
  JOIN TICKETS TCK --ОБРАЩЕНИЯ
   ON TCK.ID_TICKET = FMS.ID_TICKET
  JOIN TICKETS_D_SOURCE TSR --КаналЫ обращения
   ON TSR.ID_SOURCE = TCK.FID_SOURCE
  JOIN ALL_TICKETS_MESSAGE TCM --ВСЕ ПРИВЯЗАННЫЕ ПИСЬМА
   ON TCM.ID_TICKET = TCK.ID_TICKET
  JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ
   ON TTP.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_STATUSES TST --СТАТУСЫ ОБРАЩЕНИЯ
   ON TST.ID_STATUS = TCK.FID_STATUS
  LEFT JOIN MAIL_D_REQUESTER_TYPES RTP --ЗАЯВИТЕЛЬ
   ON RTP.ID_REQUESTER_TYPE = MSG.FID_REQUESTER_TYPE
  LEFT JOIN SENDER_ADDRESSES ADR --АДРЕСАТЫ
   ON ADR.FID_MESSAGE = FMS.ID_MESSAGE
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = MSG.FID_USER
  LEFT JOIN FINAL_TIME FT --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА--потом уберем left
   ON FT.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_METKS TMT --МЕТКИ
   ON TMT.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN ALL_TICKETS_TASKS TTS --НОМЕРА ЗАЯВОК В JIRA
   ON TTS.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_REGIONS TRG
   ON TRG.ID_REGION = TCK.FID_COMPANY_REGION
  LEFT JOIN ALL_COMPANY_TYPES CTP
   ON CTP.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_ADM_TYPES ADT--ZHKKH-917 
   ON ADT.ID_TYPE = TCK.FID_ADM_TYPE
  WHERE
    (' '||TMT.METKA||',' LIKE '% '|| I_METKA ||',%' OR I_METKA IS NULL)
    AND (ADT.ID_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 4 AND ADT.ID_TYPE IS NULL))--ZHKKH-917--Административный тип
    AND TCK.IS_ACTIVE = 1
    ORDER BY nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT)
  )
  SELECT
    ID_TICKET --№ обращения
  , CREATED_AT --Дата и время создания
  , MAIL_ADDRESS --E-mail
  , SUBJECT --Тема – тема письма
  , REQUESTER_NAME --Заявитель
  , MESSAGE_TEXT --Текст письма
  , PRIORITY --Приоритет
  , CLASSIFIER --Классификатор--
  , CLASSIFIER_LEVEL_2 --Классификатор-- 2 LEVEL
  , ADMIN_TYPE --Административный тип 
  , METKA --Метка
  , TASK1 --Номер заявки в Jira
  , TASK2 --Номер заявки в Jira
  , TASK3 --Номер заявки в Jira
  , TASK4 --Номер заявки в Jira
  , TASK5 --Номер заявки в Jira
  , TASK6 --Номер заявки в Jira
  , TASK7 --Номер заявки в Jira
  , TASK8 --Номер заявки в Jira
  , TASK9 --Номер заявки в Jira
  , TASK10 --Номер заявки в Jira
  , OPERATOR_LOGIN --Оператор--ПОКА ОПЕРАТОР БЕРЕТСЯ ИЗ ПИСЕМ, НО НАДО ИЗ ОБРАЩЕНИЯ
  , PROCESSING_TIME --Время обработки
  , SOURSE_NAME --Канал обращения
  , STATUS_NAME --Статус Обращения
  , COMMENTS --Комментарий оператора
  , MESSAGES --Привязанные письма
  , REGION_NAME --REGION
  , UPDATED_AT --Дата последнего изменени
  , COMPANY_TYPES -- ВСЕ ПОЛНОМОЧИЯ
  , COMPANY_OGRN -- OGRN
  , COMPANY_NAME -- НАЗВАНИЕ КАМПАНИИ
  , REGISTERED_AT --Время резервирования номера обращения
 --,'' as sdfsds
  FROM ALL_TICKETS
;

  TYPE t_ticket_log_for_eis IS TABLE OF cur_ticket_log_for_eis%rowtype;

  FUNCTION fnc_ticket_log_for_eis
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS VARCHAR2 --СТАТУС ОБРАЩЕНИЯ
      , I_METKA VARCHAR2 -- МЕТКИ
      , I_ADMIN_TYPE NUMBER := NULL --Административный тип
  ) RETURN t_ticket_log_for_eis pipelined;


-----------------------------------------------------------
--         ОТЧЕТ ПО СТАТУСАМ ОБРАЩЕНИЙ                   --
-----------------------------------------------------------

CURSOR cur_tickets_statuses (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

  )
IS
WITH 
gis_zhkh AS (
        SELECT * 
        FROM dual
)
, all_types AS ( --ВСЕ ТИПЫ В СПРАВОЧНИКЕ
        SELECT 
                tdt_lev_2.id_type AS id_type_level_2 --ID ТИПА ВТОРОГО УРОВНЯ  
                , tdt_lev_1
                .NAME AS type_name_level_1 --ТИП ПЕРВОГО УРОВНЯ
                , tdt_lev_2.NAME AS type_name_level_2 --ТИП ВТОРОГО УРОВНЯ
        FROM tickets_d_types tdt_lev_2
                JOIN tickets_d_types tdt_lev_1
                    ON tdt_lev_1.id_type = tdt_lev_2.id_parent AND tdt_lev_2.is_active = 1
)
, all_types_for_format AS ( --ВСЕ ТИПЫ В СПРАВОЧНИКЕ
        SELECT 
                tdt_lev_1.id_type AS id_type_level_1 --ID ТИПА ПЕРВОГО УРОВНЯ
                , tdt_lev_2.id_type AS id_type_level_2 --ID ТИПА ВТОРОГО УРОВНЯ  
                , tdt_lev_1.NAME AS type_name_level_1 --ТИП ПЕРВОГО УРОВНЯ
                , tdt_lev_2.NAME AS type_name_level_2 --ТИП ВТОРОГО УРОВНЯ
                , act.NAME AS class_type --(ГРАЖДАНИН ИЛИ НЕ ГРАЖДАНИН)
                , (CASE WHEN tdt_lev_2.NAME = 'Тестовое обращение' THEN 2 ELSE 1 END) AS ord -- ДЛЯ СОРТИРОВКИ
        FROM 
                tickets_d_types tdt_lev_2
                JOIN tickets_d_types tdt_lev_1
                    ON tdt_lev_1.id_type = tdt_lev_2.id_parent
                JOIN tickets_tps_has_acs_tps hat
                    ON hat.fid_ticket_type = tdt_lev_2.id_type
                JOIN tickets_d_tps_acs_tps act
                    ON act.id_type = hat.fid_access_type
                    AND tdt_lev_1.is_active = 1
        --ORDER BY (case when act.code = 'not_citizen' then 1 else 2 end),TDT_LEV_1.ID_TYPE, TDT_LEV_2.ID_TYPE
)
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
)
, all_tickets_types AS (--КлассификаторЫ
        SELECT
                tck.id_ticket AS id_ticket
                , MAX(tdt.type_name_level_1) KEEP (DENSE_RANK LAST ORDER BY ttp.id_has) AS type_name_level_1
                , MAX(tdt.type_name_level_2) KEEP (DENSE_RANK LAST ORDER BY ttp.id_has) AS type_name_level_2
                , MAX((CASE WHEN dctp.NAME = 'Гражданин' THEN 'Гражданин' ELSE 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY ctp.id_has) AS class_type
        FROM
                tickets tck
                JOIN mail_messages msg
                    ON msg.fid_ticket = tck.id_ticket
                JOIN tickets_has_types ttp
                    ON ttp.fid_ticket = tck.id_ticket
                JOIN all_types tdt
                    ON tdt.id_type_level_2 = ttp.fid_type
                LEFT JOIN tickets_has_cmp_tps ctp
                    ON ctp.fid_ticket = tck.id_ticket 
                LEFT JOIN tickets_d_company_types dctp 
                    ON dctp.id_company_type = ctp.fid_company_type 
                LEFT JOIN BLOCK_MAILS BML
                 ON BML.FID_MESSAGE = MSG.ID_MESSAGE    
        WHERE
                (nvl(tck.registered_at, tck.updated_at) >= i_init_time AND nvl(tck.registered_at, tck.updated_at) < i_finish_time)
            AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017 
        GROUP BY tck.id_ticket
)
, transfered_tickets as (
        select 
                  tck.id_ticket
                , max(tck.id_ticket) as is_transfered
        from tickets tck
        join TICKETS_STATUS_CHANGES tsc on  tck.id_ticket=tsc.FID_TICKET
        LEFT JOIN tickets_d_adm_types adt
                    ON adt.id_type = tck.fid_adm_type
         WHERE 
                (nvl(tck.registered_at, tck.updated_at) >= i_init_time AND nvl(tck.registered_at, tck.updated_at) < i_finish_time)
                AND tsc.FID_STATUS = 3
                AND tck.is_active = 1
                AND (adt.NAME = i_admin_type OR i_admin_type IS NULL OR (i_admin_type = 'Не задан' AND adt.NAME IS NULL))--ZHKKH-917--Административный тип
        group by tck.id_ticket
)
, IS_TICKETS_TASKS AS (
     SELECT 
       TCK.ID_TICKET
     , MAX(TCK.ID_TICKET) AS IS_TICKETS_TASKS
    FROM TICKETS TCK
    JOIN TICKETS_TASKS TTS
     ON TTS.FID_TICKET = TCK.ID_TICKET
    WHERE (NVL(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND NVL(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME) 
     GROUP BY TCK.ID_TICKET
  
 )

, all_tickets AS (
        SELECT
                ttp.type_name_level_1 AS type_name_level_1
                , ttp.type_name_level_2 AS type_name_level_2
                , ttp.class_type AS class_type  
                , tst.code AS code_status
                , NVL(TTS.IS_TICKETS_TASKS, tt.is_transfered) AS is_transfered
        FROM
                tickets tck                
                JOIN tickets_d_statuses tst
                    ON tst.id_status = tck.fid_status
                JOIN tickets_d_source tsr
                    ON tsr.id_source = tck.fid_source
                LEFT JOIN all_tickets_types ttp --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
                    ON ttp.id_ticket = tck.id_ticket    
                LEFT join transfered_tickets tt 
                    on tt.id_ticket= tck.id_ticket
                LEFT JOIN tickets_d_adm_types adt
                    ON adt.id_type = tck.fid_adm_type                    
                LEFT JOIN IS_TICKETS_TASKS TTS
                 ON TTS.id_ticket = tck.id_ticket
        WHERE 
                (nvl(tck.registered_at, tck.updated_at) >= i_init_time AND nvl(tck.registered_at, tck.updated_at) < i_finish_time)
                AND tck.is_active = 1
                AND (adt.NAME = i_admin_type OR i_admin_type IS NULL OR (i_admin_type = 'Не задан' AND adt.NAME IS NULL))--ZHKKH-917--Административный тип
)
, sum_tickets AS (--ДОБАВИЛ СУММИРОВАНИЕ ПО ВЕРТИКАЛИ И ГОРИЗОНТАЛИ
        SELECT
                decode(GROUPING(ttp.type_name_level_1)
                ,0,ttp.type_name_level_1,'Всего') AS type_name_level_1 --Классификация по теме
                , ttp.type_name_level_2
                , ttp.class_type
                , MAX(ttp.id_type_level_1) AS id_type_level_1
                , MAX(ttp.id_type_level_2) AS id_type_level_2
                , MAX(ttp.ord) AS ord
                , sum(CASE WHEN code_status = 'new' THEN 1 ELSE 0 END) AS st_new --Новое
                , sum(0) AS in_work --В работе (Такие записи не добавляются)
                , sum(CASE WHEN code_status = 'transferred-3rd-line' THEN 1 ELSE 0 END) AS st_transferred_3rd_line  --Переведен на специалиста 3й линии
                , sum(CASE WHEN code_status = 'waiting-requester-answer' THEN 1 ELSE 0 END) AS st_waiting_requester_answer --Ожидается ответ заявителя
                , sum(CASE WHEN code_status = 'need-call-to-requester' THEN 1 ELSE 0 END) AS st_need_call_to_requester --Требуется исходящий звонок заявителю
                , sum(CASE WHEN code_status = 'pre-resolved' THEN 1 ELSE 0 END) AS st_pre_resolved --Предварительное решение
                , sum(CASE WHEN code_status = 'resolved' and is_transfered IS NULL THEN 1 ELSE 0 END) AS st_resolved_line1 -- Решено на первой линии
                , sum(CASE WHEN code_status = 'resolved' and is_transfered IS NOT NULL THEN 1 ELSE 0 END) AS st_resolved_line2 -- Решено на второй линии
                , sum(CASE WHEN code_status = 'resolved' THEN 1 ELSE 0 END) AS st_resolved --Решено
                , sum(CASE WHEN code_status = 'closed-on-request' THEN 1 ELSE 0 END) AS st_closed_on_request --Закрыто по запросу
                , sum(CASE WHEN code_status = 'closed' THEN 1 ELSE 0 END) AS st_closed --Закрыто
                , sum(CASE WHEN code_status = 'solved-by-fias' THEN 1 ELSE 0 END) AS st_solved_by_fias --Решено ФИАС
                , count(code_status) AS itogo --Итого
        FROM 
                all_tickets tct
                RIGHT JOIN all_types_for_format ttp ON ttp.type_name_level_1 = tct.type_name_level_1
                    AND ttp.type_name_level_2 = tct.type_name_level_2
                    AND ttp.class_type = tct.class_type
        GROUP BY ROLLUP(ttp.type_name_level_1, ttp.type_name_level_2, ttp.class_type)
        ORDER BY GROUPING(ttp.type_name_level_1),ord, ttp.class_type, id_type_level_1,id_type_level_2
)
SELECT
        type_name_level_1 --Классификация по теме
        , type_name_level_2
        , class_type
        , st_new -- Новое
        , in_work -- В РАБОТЕ
        , st_transferred_3rd_line  -- Переведен на специалиста 3й линии
        , st_waiting_requester_answer -- Ожидается ответ заявителя
        , st_need_call_to_requester -- Требуется исходящий звонок заявителю
        , st_pre_resolved -- Предварительное решение
        , st_resolved_line1 -- Решено на первой линии
        , st_resolved_line2 -- Решено на второй линии
        , st_resolved -- Решено
        -- , ST_CLOSED_ON_REQUEST --Закрыто по запросу
        , (st_closed_on_request + st_closed) AS st_closed  -- Закрыто --НАДО СУММИРОВАТЬ
        , st_solved_by_fias -- Решено ФИАС
        , itogo -- Итого
FROM
        sum_tickets
WHERE 
        (type_name_level_1 IS NOT NULL AND type_name_level_2 IS NOT NULL AND class_type IS NOT NULL) 
        OR type_name_level_1 = 'Всего' --Убираем промежуточные суммы
;

  TYPE t_tickets_statuses IS TABLE OF cur_tickets_statuses%rowtype;

  FUNCTION fnc_tickets_statuses
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

  ) RETURN t_tickets_statuses pipelined;


-----------------------------------------------------------
--         ОТЧЕТ ДЛЯ ОТДЕЛА ПЛАНИРОВАНИЯ                 --
-----------------------------------------------------------

CURSOR cur_report_planning_dep (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

  )
IS
WITH
GIS_ZHKH AS (SELECT * FROM DUAL),
 PERIODS AS --РАЗБИВКА НА ПЕРИОДЫ
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
      )

 , EXCLUDED_LOGINS AS --ЛОГИНЫ, ДЕЙСВИЯ КОТОРЫХ МЫ НЕ УЧИТЫВАЕМ
(
 SELECT 'i.a.strapko_gis_zhkh_Vol' as LOGIN FROM DUAL
  UNION ALL
 SELECT 'v.v.iliykhin_gis_zhkh_Vol' as LOGIN FROM DUAL
  UNION ALL
 SELECT 'o.i.ruskhanova_gis_zhkh_Vol' as LOGIN FROM DUAL
  UNION ALL
 SELECT 's.v.srybnaia_gis_zhkh_Vol' as LOGIN FROM DUAL
  UNION ALL
 SELECT 'a.horolskiy' as LOGIN FROM DUAL
  UNION ALL
 SELECT 'v.v.iliykhin_gis_zhkh_Vol' as LOGIN FROM DUAL
  UNION ALL
 SELECT 't.aitkaliev' as LOGIN FROM DUAL
   UNION ALL
 SELECT 'y.dudkin' as LOGIN FROM DUAL
)

 --------------------------------------------
 --Теперь считаем время обработки письма
 --------------------------------------------
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM MAIL_MESSAGES MSG
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
      GROUP BY MAD.FID_MESSAGE
) 
 
, ALL_CHANGE AS --ВСЕ ИЗМЕНЕНИЯ ПИСЬМА
(SELECT
   CLG.ID_CHANGE_LOG AS ID_CHANGE_LOG
 , CLG.FID_MESSAGE AS FID_MESSAGE
 , US.LOGIN AS LOGIN
 , CLG.ACTION_TIME AS ACTION_TIME
 , ACT.CODE AS CODE
 , MSG.CREATED_AT AS CREATED_AT
  FROM MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
  JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
   ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = CLG.FID_USER
  JOIN MAIL_MESSAGES MSG
   ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN BLOCK_MAILS BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE 

  WHERE
      --(CLG.ACTION_TIME >= I_INIT_TIME AND CLG.ACTION_TIME < I_FINISH_TIME +1)
        (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
  AND ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND US.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol', 'v.v.iliykhin_gis_zhkh_Vol','t.aitkaliev') -- ДЛЯ ЗАЯВКИ ZHKKH-473
  AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017 
)
,
INTERVALS AS (
 SELECT
   FID_MESSAGE
 , LOGIN
 , ACTION_TIME
 , CODE
 , LAG (ACTION_TIME,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_ORDER_DATE
 , LAG(CODE,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_CODE
 , CREATED_AT
 FROM ALL_CHANGE
 ORDER BY
 FID_MESSAGE, ACTION_TIME
 )
, CALCULATION_MAILS_PREV AS ( --Расчет времени по письмам--ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
 SELECT
   FID_MESSAGE
 , (NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)) as ALL_TIME
 FROM INTERVALS ITR
 WHERE ITR.CODE = 'assign' AND ITR.PREV_CODE = 'open'
  AND ITR.LOGIN NOT IN ( SELECT LOGIN FROM  EXCLUDED_LOGINS )
)
, CALCULATION_MAILS AS (
SELECT
    PR.START_PERIOD
  , COUNT(DISTINCT MSG.ID_MESSAGE) AS MESSAGES_COUNT
  , ceil(SUM(CM.ALL_TIME)) AS ALL_TIME
   
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
  LEFT JOIN CALCULATION_MAILS_PREV CM
   ON CM.FID_MESSAGE = MSG.ID_MESSAGE
  LEFT JOIN BLOCK_MAILS BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE  
   
  WHERE MTP.DIRECTION = 'in'
   AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017
    GROUP BY PR.START_PERIOD
)

 -----------------------------------------------------
 --   ВРЕМЯ ОБРАБОТКИ ОБРАЩЕНИЙ ИЗ ЛОГА ОБРАЩЕНИЙ  (но с особенностями) --
 -----------------------------------------------------
 , ALL_CHANGE_TICKET AS --ВСЕ ИЗМЕНЕНИЯ ПИСЕМ ПРИВЯЗАННЫХ К ОБРАЩЕНИЮ
(SELECT
   CLG.ID_CHANGE_LOG AS ID_CHANGE_LOG
 , CLG.FID_MESSAGE AS FID_MESSAGE
 , US.LOGIN AS LOGIN
 , CLG.ACTION_TIME AS ACTION_TIME
 , ACT.CODE AS CODE
 --, nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) AS CREATED_AT
 --, TCK.ID_TICKET AS ID_TICKET
  FROM MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
  JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
   ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = CLG.FID_USER
  JOIN MAIL_MESSAGES MSG
   ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
  --JOIN TICKETS TCK
  -- ON TCK.ID_TICKET = MSG.FID_TICKET

  WHERE
        (CLG.ACTION_TIME >= I_INIT_TIME AND CLG.ACTION_TIME < I_FINISH_TIME)
      -- Именно здесь мы берем не врямя создания письма / обращения, а время совершения действия
      -- Это сделано для того, чтобы мы не привязывали время целой кучи привязанных писем
      --
      --  (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  AND ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND US.LOGIN NOT IN ( SELECT LOGIN FROM  EXCLUDED_LOGINS )
)
,
INTERVALS_TICKET AS (
 SELECT
   FID_MESSAGE
 --, ID_TICKET
 , LOGIN
 , ACTION_TIME
 , CODE
 , LAG (ACTION_TIME,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_ORDER_DATE
 , LAG(CODE,1) over (ORDER BY FID_MESSAGE, ACTION_TIME) AS PREV_CODE
 --, CREATED_AT
 FROM ALL_CHANGE_TICKET
 ORDER BY
 FID_MESSAGE, ACTION_TIME
 )
, CALCULATION_TICKETS AS ( --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЕМ
 SELECT
   --FID_MESSAGE
 -- LOGIN
   PR.START_PERIOD
 --, count(distinct ID_TICKET) as TICKETS_COUNT
 , ceil(SUM((NAUCRM.intervaltosec(ACTION_TIME - PREV_ORDER_DATE)))) as ALL_TIME
 FROM
     PERIODS PR
    JOIN INTERVALS_TICKET ITR
      -- Именно здесь мы берем не время создания письма / обращения, а время совершения действия
      -- Это сделано для того, чтобы мы не привязывали время целой кучи привязанных писем
      --
     ON ITR.PREV_ORDER_DATE >= PR.START_PERIOD AND ITR.PREV_ORDER_DATE < PR.STOP_PERIOD
   --  ITR.CREATED_AT >= PR.START_PERIOD AND ITR.CREATED_AT < PR.STOP_PERIOD
 WHERE ITR.CODE = 'assign' AND ITR.PREV_CODE = 'open'

 --AND (ITR.PREV_ORDER_DATE >= I_INIT_TIME AND ITR.PREV_ORDER_DATE < I_FINISH_TIME)
 GROUP BY PR.START_PERIOD)

 ----------------------------------------------------------
 -- СЧИТАЕМ КОЛИЧЕСТВО ОБРАЩЕНИЙ ОТДЕЛЬНО (ЧТОБЫ СХОДИЛОСЬ С КЛАСИФИКАЦИЕЙ ПО ОБРАЩЕНИЯМ)
 ----------------------------------------------------------
, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
 -- , MAX(CTPF.CLIENT_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLIENT_TYPE
 -- , MAX(CTPF.ID_COMPANY_TYPE) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS ID_COMPANY_TYPE
  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_COMPANY_TYPES CTPF
   ON CTPF.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
, VIEW_TICKETS_COUNT AS (

  SELECT
    COUNT(TTP.LAST_TYPE) AS TICKETS_COUNT
  , PR.START_PERIOD AS START_PERIOD
    FROM
     PERIODS PR
    JOIN TICKETS TCK ON nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= PR.START_PERIOD AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < PR.STOP_PERIOD
    JOIN TICKETS_D_SOURCE TSR
     ON TSR.ID_SOURCE = TCK.FID_SOURCE
    LEFT JOIN ALL_TICKETS_TYPES TTP --КлассификаторЫ--, LEFT ПОТОМУ ЧТО БАЗА ГРЯЗНАЯ
     ON TTP.ID_TICKET = TCK.ID_TICKET
    WHERE TCK.IS_ACTIVE = 1 
    GROUP BY PR.START_PERIOD
)
 --------------------------------------------
 --Теперь считаем время обработки обращений ИЗ ЖУРНАЛА СОБЫТИЙ--
 --------------------------------------------
, ACTIONS_LOGS AS
(SELECT
   ACL.ID_ACTION,
   ACL.LOGGABLE_ID as LOG_ID --НОМЕР ТИКЕТА
 , LAG(ACL.ID_ACTION,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS PREV_ID_ACTION
 , LAG(ACL.LOGGABLE_ID,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS PREV_LOG_ID
 , LAG(ACL.CREATED_AT,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS START_TIME
 , LAG(ACT.CODE,1) over (ORDER BY ACL.LOGGABLE_ID, ACL.CREATED_AT) AS START_CODE
 , ACL.CREATED_AT AS FINISH_TIME
 , ACT.CODE AS FINISH_CODE
 , ACL.FID_USER AS FID_USER
 , ACL.LOGGABLE_ID AS FID_TICKET
 , nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) AS CREATED_AT
 FROM
   USER_ACTIONS_LOG ACL
   JOIN USER_ACTION_TYPES ACT
    ON ACT.ID_TYPE = ACL.FID_TYPE
   JOIN TICKETS TCK
    ON TCK.ID_TICKET = ACL.LOGGABLE_ID AND ACL.LOGGABLE_TYPE = 'TICKETS'
 WHERE
       ACL.LOGGABLE_TYPE = 'TICKETS'
   AND ACT.CODE IN ('ticket-assigned','ticket-new-answer-sent','ticket-no-answer','ticket-created','ticket-blocked','ticket-unblocked')
  -- AND (ACL.CREATED_AT >= I_INIT_TIME) AND (ACL.CREATED_AT < I_FINISH_TIME + 1)--
  AND (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME) AND (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)

)

, ACTIONS_LOGS_2 AS
(SELECT DISTINCT--Двойные события
   ACL.ID_ACTION AS ID_ACTION
 , ACL.LOG_ID AS LOG_ID --НОМЕР ТИКЕТА
 , ACL.START_TIME AS START_TIME
 , ACL.START_CODE AS START_CODE
 , ACL.FINISH_TIME AS FINISH_TIME
 , ACL.FINISH_CODE AS FINISH_CODE
 , US.LOGIN AS LOGIN
 , ACL.PREV_ID_ACTION AS PREV_ID_ACTION
 , ACL.FID_TICKET
 , ACL.CREATED_AT

 FROM
  ACTIONS_LOGS ACL
 LEFT JOIN USER_ACTION_RELATIONS ACR
  ON ACR.FID_ACTION = ACL.ID_ACTION
 LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = ACL.FID_USER
 WHERE
       /* (ACL.START_TIME >= I_INIT_TIME) AND (ACL.START_TIME < I_FINISH_TIME)  --
   AND */   ACL.LOG_ID = ACL.PREV_LOG_ID
   AND ((ACL.START_CODE = 'ticket-assigned' and ACL.FINISH_CODE IN ('ticket-new-answer-sent','ticket-no-answer'))
     OR (ACL.START_CODE IN ('ticket-created') and ACL.FINISH_CODE = 'ticket-unblocked')
     OR (ACL.START_CODE IN ('ticket-blocked') and ACL.FINISH_CODE = 'ticket-unblocked')
       )
)
, CALCULATION_TICKETS_LOG AS
 (
 SELECT
   --ACL.LOGIN AS LOGIN
   PR.START_PERIOD
 , COUNT(DISTINCT FID_TICKET) AS TICKETS_COUNT
 , ceil(SUM((NAUCRM.intervaltosec(ACL.FINISH_TIME - ACL.START_TIME)))) as ALL_TIME

 FROM
     PERIODS PR
    JOIN ACTIONS_LOGS_2 ACL
     ON ACL.CREATED_AT >= PR.START_PERIOD AND ACL.CREATED_AT < PR.STOP_PERIOD --
    --ON ACL.START_TIME >= PR.START_PERIOD AND ACL.START_TIME < PR.STOP_PERIOD --

 WHERE ACL.LOGIN NOT IN ( SELECT LOGIN FROM  EXCLUDED_LOGINS )
 GROUP BY PR.START_PERIOD  --ACL.LOGIN
 )
, FINAL_DATA AS
 (
 SELECT
  --distinct -- есть глюк выходят одинаковые интервалы, если начинать не с нуля часов
 --COALESCE(PR.START_PERIOD, CM.START_PERIOD, CT.START_PERIOD, CTL.START_PERIOD) as START_PERIOD_2
 --  PR.START_PERIOD
   DECODE(GROUPING(PR.START_PERIOD)
                ,0,TO_CHAR(PR.START_PERIOD,'dd.mm.yyyy hh24:mi:ss'),'Всего') AS START_PERIOD
                ,TO_CHAR(PR.START_PERIOD,'dd.mm.yyyy hh24:mi:ss') as qqq

 , SUM(NVL(CM.MESSAGES_COUNT,0)) AS MESSAGES_COUNT
 , SUM(NVL(CM.ALL_TIME,0)) AS MESSAGES_TIME
 , SUM(NVL(VTC.TICKETS_COUNT,0)) AS TICKETS_COUNT
 , SUM(NVL(CT.ALL_TIME,0) + NVL(CTL.ALL_TIME,0)) AS TICKETS_TIME

   --
 FROM
          PERIODS PR
 FULL JOIN CALCULATION_MAILS CM
  ON CM.START_PERIOD = PR.START_PERIOD
 FULL JOIN CALCULATION_TICKETS CT
  ON CT.START_PERIOD = PR.START_PERIOD
 FULL JOIN CALCULATION_TICKETS_LOG CTL
  ON CTL.START_PERIOD = PR.START_PERIOD
 FULL JOIN VIEW_TICKETS_COUNT VTC
  ON VTC.START_PERIOD = PR.START_PERIOD


  GROUP BY ROLLUP(PR.START_PERIOD)
  ORDER BY PR.START_PERIOD
 )

    SELECT

      nvl2(I_GROUP,START_PERIOD,'') as START_PERIOD  -- Дата
    , MESSAGES_COUNT -- Кол-во поступивших писем
    , MESSAGES_TIME
    , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(MESSAGES_TIME/DECODE(MESSAGES_COUNT,0,1,MESSAGES_COUNT))) AS MAILS_AHT --AHT писем
    , TICKETS_COUNT --Кол-во созданных обращений
    , TICKETS_TIME
    , PKG_GENERAL_REPORTS.FNC_TO_TIME_FORMAT(ROUND(TICKETS_TIME/DECODE(TICKETS_COUNT,0,1,TICKETS_COUNT))) AS  TICKETS_AHT --AHT обращений
    FROM FINAL_DATA

    WHERE --'Всего' != START_PERIOD --пока не попросят, не раскомменчивать следующую строку
     nvl2(I_GROUP,'Все строки','Всего') != START_PERIOD
  ;

  TYPE t_report_planning_dep IS TABLE OF cur_report_planning_dep%rowtype;

  FUNCTION fnc_report_planning_dep
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_report_planning_dep pipelined;


---------------------------------------------------------
--         ОТЧЕТ ПО SLA ДЛЯ КАНАЛА E-MAIL
---------------------------------------------------------
--Заявка ZHKKH-823

CURSOR cur_report_SLA (
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
 -- select * from PERIODS; --hour  day  minute
, MAIL_TYPES AS (
      SELECT 1 AS ID_TYPE, 'Новое на support' AS NAME_TYPE, 'TYPE_1' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 2 AS ID_TYPE, 'Новое по веб-форме на Портале, всего' AS NAME_TYPE, 'TYPE_2' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 3 AS ID_TYPE, 'Новое по веб-форме на Портале, МКС' AS NAME_TYPE, 'TYPE_3' AS CODE, 1 AS TST FROM DUAL
   UNION ALL
      SELECT 4 AS ID_TYPE, 'Новое по веб-форме на Портале, Оплата на портале ГИС ЖКХ' AS NAME_TYPE, 'TYPE_4' AS CODE, 3 AS TST FROM DUAL
   UNION ALL
      SELECT 5 AS ID_TYPE, 'Новое по веб-форме на Портале, другой приоритет' AS NAME_TYPE, 'TYPE_5' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 6 AS ID_TYPE, 'Ответ заявителя на support, всего' AS NAME_TYPE, 'TYPE_6' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 7 AS ID_TYPE, 'Ответ заявителя на support, МКС' AS NAME_TYPE, 'TYPE_7' AS CODE, 1 AS TST FROM DUAL
   UNION ALL
      SELECT 8 AS ID_TYPE, 'Ответ заявителя на support, Оплата на портале ГИС ЖКХ' AS NAME_TYPE, 'TYPE_8' AS CODE, 3 AS TST FROM DUAL
   UNION ALL
      SELECT 9 AS ID_TYPE, 'Ответ заявителя на support, другой приоритет' AS NAME_TYPE, 'TYPE_9' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 10 AS ID_TYPE, 'Уведомление из JIRA' AS NAME_TYPE, 'TYPE_10' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 11 AS ID_TYPE, 'Новое со входящей линии, всего' AS NAME_TYPE, 'TYPE_11' AS CODE, 24 AS TST FROM DUAL
   UNION ALL
      SELECT 12 AS ID_TYPE, 'Новое со входящей линии, МКС' AS NAME_TYPE, 'TYPE_12' AS CODE, 1 AS TST FROM DUAL
   UNION ALL
      SELECT 13 AS ID_TYPE, 'Новое со входящей линии, Оплата на портале ГИС ЖКХ' AS NAME_TYPE, 'TYPE_13' AS CODE, 3 AS TST FROM DUAL
   UNION ALL
      SELECT 14 AS ID_TYPE, 'Новое со входящей линии, другой приоритет' AS NAME_TYPE, 'TYPE_14' AS CODE, 24 AS TST FROM DUAL
      
)
, FORMAT AS (
   SELECT * FROM
   PERIODS PR, MAIL_TYPES MTP
   ORDER BY PR.START_PERIOD, MTP.ID_TYPE
 )

, MAIL_PROCESSING_TIME AS  (--определяет время привязки к обращению
      SELECT
         CLG.FID_MESSAGE AS ID_MESSAGE
       , MIN(CLG.ACTION_TIME) AS PROCESSING_TIME
       --, NAUCRM.intervaltosec(MIN(CLG.ACTION_TIME) - MSG.CREATED_AT)/60 AS QWE
      
      
      FROM MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
      LEFT JOIN MAIL_CHANGE_LOG CLG --ЛОГ ИЗМЕНЕНИЙ
       ON CLG.FID_MESSAGE = MSG.ID_MESSAGE
      JOIN MAIL_D_ACTION_TYPES ACT --ТИПЫ ИЗМЕНЕНИЙ
       ON ACT.ID_ACTION_TYPE = CLG.FID_ACTION_TYPE
      JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
       ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE
      JOIN MAIL_D_MSG_STATUSES MST --СТАТУС ПИСЬМА
       ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS

      WHERE
          (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
      AND ACT.CODE = 'assign' -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
      AND MTP.DIRECTION = 'in' -- НАПРАВЛЕНИЕ только входящие
      GROUP BY CLG.FID_MESSAGE

)


, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_1
  , MAX(TDT_LEV_2.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_2 
  , MAX(TTP.CREATED_AT) AS LAST_TYPE_CREATED
  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE  AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
   ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
   ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1  

  WHERE
        (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
  GROUP BY TCK.ID_TICKET
  )
--SELECT * from  ALL_TICKETS_TYPES;
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
      GROUP BY MAD.FID_MESSAGE
)
, PREPARE_MESSAGES AS (
    --TYPE_ IN (1,2,6,10,11)
      SELECT
        MSG.ID_MESSAGE
      , MSG.FID_TICKET
      , MSG.CREATED_AT
      , MPT.PROCESSING_TIME
      , PR.START_PERIOD
      , (CASE
          --TYPE_1:
          WHEN MTP.CODE = 'support_new'
          THEN 'TYPE_1'
          WHEN MTP.CODE = 'web_form_new'
          THEN 'TYPE_2'
          WHEN MTP.CODE = 'support_answer'
          THEN 'TYPE_6'
          WHEN MTP.CODE = 'expert_notification'          
          THEN 'TYPE_10'
          WHEN MTP.CODE = 'in_script_new'                    
          THEN 'TYPE_11'         
          
          ELSE 'ДРУГОЕ'     
         END) AS TYPE_CODE
      FROM PERIODS PR
      JOIN MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
       ON (MSG.CREATED_AT >= PR.START_PERIOD AND MSG.CREATED_AT < PR.STOP_PERIOD)
      JOIN MAIL_D_MSG_STATUSES MST --СТАТУСЫ ПИСЕМ
       ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
      JOIN MAIL_D_REQUESTER_TYPES RTP --ЗАЯВИТЕЛЬ
       ON RTP.ID_REQUESTER_TYPE = MSG.FID_REQUESTER_TYPE
      JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
       ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE 
      LEFT JOIN MAIL_PROCESSING_TIME MPT
       ON MPT.ID_MESSAGE = MSG.ID_MESSAGE
      LEFT JOIN BLOCK_MAILS BML
        ON BML.FID_MESSAGE = MSG.ID_MESSAGE 
      WHERE
          (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
      AND MTP.DIRECTION = 'in' -- НАПРАВЛЕНИЕ только входящие
      AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017

        UNION ALL
         --TYPE_ NOT IN (1,2,6,10,11)
      SELECT
        MSG.ID_MESSAGE
      , MSG.FID_TICKET
      , MSG.CREATED_AT
      , MPT.PROCESSING_TIME
      , PR.START_PERIOD
      , (CASE
          WHEN MTP.CODE = 'web_form_new' AND RTP.CODE = 'mks'
          THEN 'TYPE_3'
          WHEN MTP.CODE = 'web_form_new' AND RTP.CODE != 'mks' 
           AND COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ')
          THEN 'TYPE_4'
          WHEN MTP.CODE = 'web_form_new' AND RTP.CODE != 'mks' 
           AND (COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) NOT IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ') OR COALESCE(TTP_2.NAME,TTP.LAST_TYPE) IS NULL) 
          THEN 'TYPE_5'
          WHEN MTP.CODE = 'support_answer' AND RTP.CODE = 'mks'
          THEN 'TYPE_7'
          WHEN MTP.CODE = 'support_answer' AND RTP.CODE != 'mks' 
           AND COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ')
          THEN 'TYPE_8'
          WHEN MTP.CODE = 'support_answer' AND RTP.CODE != 'mks' 
           AND (COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) NOT IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ') OR COALESCE(TTP_2.NAME,TTP.LAST_TYPE) IS NULL)
          THEN 'TYPE_9'
          WHEN MTP.CODE = 'in_script_new' AND RTP.CODE = 'mks'
          THEN 'TYPE_12'
          WHEN MTP.CODE = 'in_script_new' AND RTP.CODE != 'mks' 
           AND COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ')
          THEN 'TYPE_13'
          WHEN MTP.CODE = 'in_script_new' AND RTP.CODE != 'mks' 
           AND (COALESCE(TDT_LEV_1.NAME,TTP_2.NAME,CLASSIFIER_NEW_LEV_1,TTP.LAST_TYPE) NOT IN ('Оплата услуг ЖКУ банковской картой на сайте ГИС ЖКХ','Оплата на портале ГИС ЖКХ') OR COALESCE(TTP_2.NAME,TTP.LAST_TYPE) IS NULL)
          THEN 'TYPE_14'          
        
          
          ELSE 'ДРУГОЕ'     
         END) AS TYPE_CODE
      FROM PERIODS PR
      JOIN MAIL_MESSAGES MSG --ЛОГ ПИСЕМ
       ON (MSG.CREATED_AT >= PR.START_PERIOD AND MSG.CREATED_AT < PR.STOP_PERIOD)
      JOIN MAIL_D_MSG_STATUSES MST --СТАТУСЫ ПИСЕМ
       ON MST.ID_MSG_STATUS = MSG.FID_MSG_STATUS
      JOIN MAIL_D_REQUESTER_TYPES RTP --ЗАЯВИТЕЛЬ
       ON RTP.ID_REQUESTER_TYPE = MSG.FID_REQUESTER_TYPE
      JOIN MAIL_D_MSG_TYPES MTP --ТИПЫ ПИСЕМ
       ON MTP.ID_MSG_TYPE = MSG.FID_MSG_TYPE 
      LEFT JOIN MAIL_PROCESSING_TIME MPT
       ON MPT.ID_MESSAGE = MSG.ID_MESSAGE
      LEFT JOIN ALL_TICKETS_TYPES TTP
       ON TTP.ID_TICKET = MSG.FID_TICKET
      LEFT JOIN INC_CALL_CONTACT_DATA INC
       ON INC.FID_MESSAGE_MAIL = MSG.ID_MESSAGE
      LEFT JOIN TICKETS_D_TYPES TTP_2
       ON TTP_2.ID_TYPE = INC.FID_TYPE AND TTP_2.ID_TYPE BETWEEN 1 AND 13
      LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
       ON TDT_LEV_2.ID_TYPE = INC.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
      LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
       ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1
      LEFT JOIN BLOCK_MAILS BML
        ON BML.FID_MESSAGE = MSG.ID_MESSAGE  
       
      WHERE
          (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
      AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017 
      AND MTP.DIRECTION = 'in' -- НАПРАВЛЕНИЕ только входящие   

  )
, STATISTIC AS (
  SELECT
    FT.NAME_TYPE AS NAME_TYPE
  , MAX(FT.ID_TYPE) AS ID_TYPE
-- , FR.NAME_TYPE
  , FT.START_PERIOD
  , COUNT(PRM.TYPE_CODE) AS COUNT_RECEIVED
  , COUNT((CASE WHEN PRM.FID_TICKET IS NOT NULL AND PRM.PROCESSING_TIME IS NOT NULL THEN PRM.TYPE_CODE ELSE NULL END)) AS COUNT_PROCESSED
  , MAX(FT.TST) AS TST
  , SUM( CASE
          WHEN  PRM.FID_TICKET IS NOT NULL AND PRM.PROCESSING_TIME IS NOT NULL
            AND ABS (NAUCRM.intervaltosec(PRM.PROCESSING_TIME - PRM.CREATED_AT) )/ 3600 <= FT.TST            
          THEN 1
          ELSE 0
         END) AS COUNT_PROCESSED_IN_TST
  , SUM( ABS (NAUCRM.intervaltosec(PRM.PROCESSING_TIME - PRM.CREATED_AT))) / 3600 AS TIME_PROCESSED
  FROM PREPARE_MESSAGES PRM
  RIGHT JOIN FORMAT FT
   ON FT.START_PERIOD = PRM.START_PERIOD AND FT.CODE = PRM.TYPE_CODE
  GROUP BY FT.START_PERIOD , FT.NAME_TYPE
  ORDER BY FT.START_PERIOD, ID_TYPE
  )
, STATISTIC_SUM AS (
  SELECT

    SUM(COUNT_RECEIVED) AS COUNT_RECEIVED
  , SUM(COUNT_PROCESSED) AS COUNT_PROCESSED
  , SUM(COUNT_PROCESSED_IN_TST) AS COUNT_PROCESSED_IN_TST
  , SUM(TIME_PROCESSED) AS TIME_PROCESSED
  FROM STATISTIC
  WHERE ID_TYPE IN (1,2,6,10,11)
   
  )  
  SELECT
    NAME_TYPE --Тип письма
  , (CASE 
      WHEN I_GROUP IS NULL THEN ''
      ELSE  TO_CHAR(START_PERIOD,'dd.mm.yyyy hh24:mi:ss')
     END) AS START_PERIOD
  , COUNT_RECEIVED --Поступило писем
  , COUNT_PROCESSED --Обработано писем из поступивших
  ,  REPLACE(TRIM(TO_CHAR(NVL(COUNT_PROCESSED/DECODE(COUNT_RECEIVED,0,1,COUNT_RECEIVED),0)*100,'990D99')),'.',',')||'%' AS PROCENT_PROCESSED -- Доля обработанных от поступивших
  , TO_CHAR(TST) AS TST --TST, ч.
  ,  (CASE
       WHEN COUNT_RECEIVED = 0 THEN '100,00%'
       ELSE REPLACE(TRIM(TO_CHAR(NVL(COUNT_PROCESSED_IN_TST/DECODE(COUNT_RECEIVED,0,1,COUNT_RECEIVED),0)*100,'990D99')),'.',',')||'%' 
      END) AS SLA --SLA, %
  ,  REPLACE(TRIM(TO_CHAR(NVL(TIME_PROCESSED/DECODE(COUNT_PROCESSED,0,1,COUNT_PROCESSED),0),'990D99')),'.',',') AS AVG_TIME_PROCESSED --Среднее время реакции, ч.
  FROM STATISTIC
 
  WHERE NAME_TYPE IS NOT NULL  
  
  UNION ALL
  
    SELECT
    'Всего' AS NAME_TYPE --Тип письма
  , '' AS START_PERIOD
  , COUNT_RECEIVED --Поступило писем
  , COUNT_PROCESSED --Обработано писем из поступивших
  ,  REPLACE(TRIM(TO_CHAR(NVL(COUNT_PROCESSED/DECODE(COUNT_RECEIVED,0,1,COUNT_RECEIVED),0)*100,'990D99')),'.',',')||'%' AS PROCENT_PROCESSED -- Доля обработанных от поступивших
  , '-' AS TST --TST, ч.
  ,  (CASE
       WHEN COUNT_RECEIVED = 0 THEN '100,00%'
       ELSE REPLACE(TRIM(TO_CHAR(NVL(COUNT_PROCESSED_IN_TST/DECODE(COUNT_RECEIVED,0,1,COUNT_RECEIVED),0)*100,'990D99')),'.',',')||'%' 
      END) AS SLA --SLA, %
  ,  REPLACE(TRIM(TO_CHAR(NVL(TIME_PROCESSED/DECODE(COUNT_PROCESSED,0,1,COUNT_PROCESSED),0),'990D99')),'.',',') AS AVG_TIME_PROCESSED --Среднее время реакции, ч.
  FROM STATISTIC_SUM
  ;
  
    TYPE t_report_SLA IS TABLE OF cur_report_SLA%rowtype;

  FUNCTION fnc_report_SLA
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_report_SLA pipelined;


---------------------------------------------------------
--         ОТЧЕТ Сроки обработки обращений
---------------------------------------------------------

CURSOR cur_TICKETS_proc_time (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
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

     --ORDER BY (case when act.code = 'not_citizen' then 1 else 2 end),TDT_LEV_1.ID_TYPE, TDT_LEV_2.ID_TYPE
 ) 
, FORMAT AS (
  SELECT * FROM PERIODS ,ALL_TYPES_FOR_FORMAT

               --ORDER BY START_PERIOD,ID_TYPE
  )   
, BLOCK_MAILS AS ( --Так ограничиваем письма с определенным адресом ZHKKH-1017
      SELECT 
       MAD.FID_MESSAGE,
       MAX('BLOCK_MAIL') AS MAIL_ADDRESS
      FROM  TICKETS TCK
      JOIN MAIL_MESSAGES MSG
       ON MSG.FID_TICKET = TCK.ID_TICKET
      JOIN MAIL_ADDRESSES MAD
       ON MAD.FID_MESSAGE = MSG.ID_MESSAGE
      WHERE MAD.MAIL_ADDRESS = 'postmaster@newcontact.su' --ZHKKH-1017
        AND (    nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME 
             AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME
             )
      GROUP BY MAD.FID_MESSAGE
) 
, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
  , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
  , MAX((CASE WHEN DCTP.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
  , MAX(ADT.NAME) AS ADMIN_TYPE

  FROM  TICKETS TCK
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE
    LEFT JOIN ALL_TYPES TDT  --MUST JOIN
   ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
  LEFT JOIN TICKETS_D_ADM_TYPES ADT
   ON ADT.ID_TYPE = TCK.FID_ADM_TYPE
  LEFT JOIN TICKETS_HAS_CMP_TPS CTP
   ON CTP.FID_TICKET = TCK.ID_TICKET 
  LEFT JOIN TICKETS_D_COMPANY_TYPES DCTP
   ON DCTP.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE 
  LEFT JOIN BLOCK_MAILS BML
   ON BML.FID_MESSAGE = MSG.ID_MESSAGE 


  WHERE -- тут от даты резервирования номера
        (nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) >= I_INIT_TIME AND nvl(TCK.CREATED_AT, TCK.REGISTERED_AT) < I_FINISH_TIME)
    AND (BML.MAIL_ADDRESS != 'BLOCK_MAIL' OR BML.MAIL_ADDRESS IS NULL) --ZHKKH-1017  
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
   ON TDT.ID_TYPE = TTP.FID_TYPE


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
,  TICKET_RESOLVED_TIME AS  (--определяет время проставления статуса "Решено"
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
  , TTP.TYPE_NAME_LEVEL_1 AS TYPE_NAME_LEVEL_1
  , TTP.TYPE_NAME_LEVEL_2 AS TYPE_NAME_LEVEL_2
  , TTP.CLASS_TYPE AS CLASS_TYPE
  , TTP.ADMIN_TYPE AS ADMIN_TYPE
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
     ON TTP.ID_TICKET = TCK.ID_TICKET
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
      AND (ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип

  )
  
, ALL_TICKETS AS (
   
   SELECT 
     ID_TICKET
   , TYPE_NAME_LEVEL_1
   , TYPE_NAME_LEVEL_2
   , CLASS_TYPE
   , ADMIN_TYPE
   , PERIOD
   , CREATED_TIME
   , RESOLVED_TIME
   , SECOND_LINE
   FROM ALL_TICKETS_PREP
   WHERE (RESOLVED_TIME >= I_INIT_TIME AND RESOLVED_TIME < I_FINISH_TIME)
      AND (RESOLVED_TIME >= CREATED_TIME) 
      --нужно чтобы время создания и время резервирования входили в выбранный промежуток времени  
 
 )  
  
, PREPARE_STATISTIC AS (
    SELECT 
      DECODE(GROUPING(FT.START_PERIOD)
                  ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
    , FT.TYPE_NAME_LEVEL_2
    , FT.CLASS_TYPE
    , TO_CHAR(FT.START_PERIOD,'dd.mm.yyyy') AS PERIOD
    , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
    , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
    , MAX(FT.ORD) AS ORD
    , SUM(CASE WHEN ATK.SECOND_LINE = 0 THEN 1 ELSE 0 END) AS RESOLVED_CC_COUNT -- Решено КЦ
    , SUM(CASE WHEN ATK.SECOND_LINE = 0 THEN NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME) ELSE 0 END) AS RESOLVED_CC_TIME
    , SUM(CASE WHEN ATK.SECOND_LINE = 1 THEN 1 ELSE 0 END) AS RESOLVED_SECOND_LINE_COUNT -- Решено с участием 2-й линии
    , SUM(CASE WHEN ATK.SECOND_LINE = 1 THEN NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME) ELSE 0 END) AS RESOLVED_SECOND_LINE_TIME
    , COUNT(ATK.ID_TICKET) AS RESOLVED_ALL_COUNT -- Решено ВСЕГО
    , NVL(SUM(NAUCRM.INTERVALTOSEC(ATK.RESOLVED_TIME - ATK.CREATED_TIME)),0) AS RESOLVED_ALL_TIME
    FROM ALL_TICKETS ATK
    RIGHT JOIN FORMAT FT ON FT.TYPE_NAME_LEVEL_1 = ATK.TYPE_NAME_LEVEL_1
                      AND FT.TYPE_NAME_LEVEL_2 = ATK.TYPE_NAME_LEVEL_2
                      AND FT.CLASS_TYPE = ATK.CLASS_TYPE
                      AND FT.START_PERIOD = ATK.PERIOD
        
    GROUP BY ROLLUP(FT.START_PERIOD,FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)
    ORDER BY GROUPING(FT.START_PERIOD),FT.START_PERIOD,ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  
  )
  SELECT 
    TYPE_NAME_LEVEL_1 --Классификация по теме
  , TYPE_NAME_LEVEL_2 --Классификация по теме 2 LEVEL  
  , CLASS_TYPE --Классификатор для полномочия
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_CC_TIME/DECODE(RESOLVED_CC_COUNT,0,1,RESOLVED_CC_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_CC_AVG --Решено КЦ, часов
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_SECOND_LINE_TIME/DECODE(RESOLVED_SECOND_LINE_COUNT,0,1,RESOLVED_SECOND_LINE_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_SECOND_LINE_AVG -- Решено с участием 2-й линии, часов
--  , REPLACE(TRIM(TO_CHAR(NVL(RESOLVED_ALL_TIME/DECODE(RESOLVED_ALL_COUNT,0,1,RESOLVED_ALL_COUNT),0)/3600,'990D99')),'.',',') AS RESOLVED_ALL_AVG --Общий итог

  , ceil(NVL(RESOLVED_CC_TIME/DECODE(RESOLVED_CC_COUNT,0,1,RESOLVED_CC_COUNT),0)/3600) AS RESOLVED_CC_AVG --Решено КЦ, часов
  , ceil(NVL(RESOLVED_SECOND_LINE_TIME/DECODE(RESOLVED_SECOND_LINE_COUNT,0,1,RESOLVED_SECOND_LINE_COUNT),0)/3600) AS RESOLVED_SECOND_LINE_AVG -- Решено с участием 2-й линии, часов
  , ceil(NVL(RESOLVED_ALL_TIME/DECODE(RESOLVED_ALL_COUNT,0,1,RESOLVED_ALL_COUNT),0)/3600) AS RESOLVED_ALL_AVG --Общий итог
  FROM PREPARE_STATISTIC
  WHERE (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
  
  ;

    TYPE t_TICKETS_proc_time IS TABLE OF cur_TICKETS_proc_time%rowtype;

  FUNCTION fnc_TICKETS_proc_time
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

  ) RETURN t_TICKETS_proc_time pipelined;



  ---------------------------------------------------------------------
  --     Отчет по заявкам для МКС - 1-й уровень                      --      
  ---------------------------------------------------------------------


CURSOR cur_report_for_mks (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  )
IS
  WITH
 ALL_TICKETS_TYPES AS (--КлассификаторЫ
      SELECT
        TCK.ID_TICKET AS ID_TICKET
      , LISTAGG(TDT.NAME,', ') WITHIN GROUP(ORDER BY TTP.ID_HAS) AS CLASSIFIER
      , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_1
      , MAX(TDT_LEV_2.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_2
      
      FROM
      TICKETS TCK
      JOIN TICKETS_HAS_TYPES TTP
       ON TTP.FID_TICKET = TCK.ID_TICKET
      LEFT JOIN TICKETS_D_TYPES TDT
       ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
      LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
       ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
      LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
       ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1 
       
      WHERE
           TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME
      GROUP BY TCK.ID_TICKET
  )
--,  TICKET_PROCESSING_TIME AS  (--определяет время РЕГИСТРАЦИИ
--      SELECT
--         TCK.ID_TICKET AS FID_TICKET
--       , MIN(ACL.CREATED_AT) AS REGISTER_TIME
--            
--      FROM TICKETS TCK 
--      JOIN TICKETS_D_SOURCE TSR
--       ON TSR.ID_SOURCE = TCK.FID_SOURCE
--      JOIN TICKETS_D_STATUSES TST
--       ON TST.ID_STATUS = TCK.FID_STATUS
--      JOIN USER_ACTIONS_LOG ACL
--       ON ACL.LOGGABLE_ID = TCK.ID_TICKET AND LOGGABLE_TYPE = 'TICKETS'
--      JOIN USER_ACTION_TYPES ACT
--       ON ACT.ID_TYPE = ACL.FID_TYPE
--
--
--      WHERE -- тут от даты резервирования номера
--          (TCK.CREATED_AT >= :I_INIT_TIME AND TCK.CREATED_AT < :I_FINISH_TIME)
--       AND TCK.IS_ACTIVE = 1
--       AND TST.CODE = 'resolved'
--       AND ACT.CODE = 'ticket-register'
--      GROUP BY TCK.ID_TICKET
--
--)    
,  TICKET_RESOLVED_TIME AS  (--определяет время проставления статуса "Решено"
      SELECT
         TCK.ID_TICKET AS FID_TICKET
     --  , MIN(CASE WHEN TST.CODE = 'resolved' THEN TSC.CREATED_AT END ) AS MIN_RESOLVED_TIME
       , MIN(CASE WHEN TST.CODE = 'transferred-3rd-line' THEN TSC.CREATED_AT END ) AS MIN_TRANSFERRED_TIME
       , MAX(CASE WHEN TST.CODE = 'resolved' THEN TSC.CREATED_AT END ) AS MAX_RESOLVED_TIME
     --  , MAX(CASE WHEN TST.CODE = 'transferred-3rd-line' THEN TSC.CREATED_AT END ) AS MAX_TRANSFERRED_TIME
            
      FROM TICKETS TCK 
      JOIN TICKETS_STATUS_CHANGES TSC
       ON TSC.FID_TICKET = TCK.ID_TICKET
      JOIN TICKETS_D_STATUSES TST
       ON TST.ID_STATUS = TSC.FID_STATUS  
      
      WHERE -- тут от даты резервирования номера
          (TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME)
       AND TCK.IS_ACTIVE = 1
       AND TST.CODE IN ('resolved','transferred-3rd-line')
      GROUP BY TCK.ID_TICKET

)
, PREV_PROCESSING_TIME AS (
      SELECT 
         TCK.ID_TICKET 
--   , (CASE
--      WHEN TFM.CODE_MESSAGE = 'web_form_new'
--      THEN TPT.REGISTER_TIME
--      ELSE TCK.CREATED_AT
--     END) AS CREATED_TIME
      ,  /*COALESCE(TPT.REGISTER_TIME,TCK.REGISTERED_AT,TCK.CREATED_AT)*/TCK.CREATED_AT AS CREATED_TIME --УТОЧНИТЬ!!!!!!!!!!
      ,  NVL(LEAST(TRT.MAX_RESOLVED_TIME, NVL2(TRT.MAX_RESOLVED_TIME, TRT.MIN_TRANSFERRED_TIME, NULL)),
             (CASE
              WHEN TST.CODE = 'resolved' THEN TCK.UPDATED_AT
              ELSE TCK.UPDATED_AT - 5
              END)
             ) AS RESOLVED_TIME
       , (CASE WHEN TRT.MAX_RESOLVED_TIME IS NULL THEN 'false' ELSE 'true' END) AS IS_RESOLVED    
       , (CASE WHEN TRT.MIN_TRANSFERRED_TIME IS NULL THEN 'false' ELSE 'true' END) AS IS_TRANSFERRED
      FROM TICKETS TCK
       JOIN TICKETS_D_STATUSES TST
      ON TST.ID_STATUS = TCK.FID_STATUS
--      LEFT JOIN TICKET_PROCESSING_TIME TPT 
--       ON TPT.FID_TICKET = TCK.ID_TICKET 
      LEFT JOIN TICKET_RESOLVED_TIME TRT
       ON TRT.FID_TICKET = TCK.ID_TICKET
      WHERE (TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME) 

 )
, TICKETS_PROCESSING_TIME AS (
     SELECT 
       ID_TICKET
     , REPLACE(TRIM(TO_CHAR(NVL(NAUCRM.INTERVALTOSEC(RESOLVED_TIME - CREATED_TIME),0)/3600,'999999999990D99')),'.',',') AS PROCESSING_TIME ----УТОЧНИТЬ!!!!!!!!!!
     , IS_RESOLVED
     , IS_TRANSFERRED
     
     FROM PREV_PROCESSING_TIME
     WHERE RESOLVED_TIME > CREATED_TIME
     
  )
, CALCULATION_STATUSES AS (  --сколько раз был проставлен статус "ожидание ответа заявителя"
      SELECT
        TCK.ID_TICKET
      , SUM(CASE WHEN TDST.CODE = 'waiting-requester-answer' THEN 1 ELSE 0 END) AS COUNT_STATUS_WAITING 
      , SUM(CASE WHEN TDST.CODE = 'transferred-3rd-line' THEN 1 ELSE 0 END) AS COUNT_STATUS_TRANSFERRED
      , SUM(CASE
             WHEN TRT.MIN_TRANSFERRED_TIME IS NULL AND TDST.CODE = 'waiting-requester-answer' THEN 1
             WHEN TRT.MIN_TRANSFERRED_TIME < TST.CREATED_AT AND TDST.CODE = 'waiting-requester-answer' THEN 1
             ELSE 0
            END) AS COUNT_STATUS_WAITING_SECOND
      FROM TICKETS TCK
      JOIN TICKETS_STATUS_CHANGES TST
       ON TST.FID_TICKET = TCK.ID_TICKET
      JOIN TICKETS_D_STATUSES TDST
       ON TDST.ID_STATUS = TST.FID_STATUS
      LEFT JOIN TICKET_RESOLVED_TIME TRT
       ON TRT.FID_TICKET = TCK.ID_TICKET
              
      WHERE (TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME)
        AND TDST.CODE IN ('waiting-requester-answer','transferred-3rd-line') --статус "ожидание ответа заявителя"
        
      GROUP BY TCK.ID_TICKET  
  )
 -- SELECT * FROM CALCULATION_STATUSES;
 
, MAIN_DATA AS (
      SELECT 
         TCK.ID_TICKET --||'/'||
       , TTS.TASK_CODE
       , TO_CHAR(TCK.CREATED_AT, 'dd.mm.yyyy hh24:mi:ss') as CREATED_AT
       , NVL(TTP.CLASSIFIER_NEW_LEV_1,TTP.CLASSIFIER) AS CLASSIFIER_NEW_LEV_1
       , TTP.CLASSIFIER_NEW_LEV_2
       , ADT.NAME AS ADMIN_TYPE
       , (CASE WHEN IS_RESOLVED = 'true' THEN TPT.PROCESSING_TIME ELSE '' END) AS PROCESSING_TIME --Срок обработки, в часах
       , (CASE WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN ''
               WHEN NVL(TPT.IS_TRANSFERRED, TTS.TASK_CODE) IS NULL THEN 'Да'
               ELSE 'Нет'
          END) AS NO_SECOND_LINE --Решено без участия 2-ой линии
       , (CASE WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN ''
               WHEN NVL(TPT.IS_TRANSFERRED, TTS.TASK_CODE) IS NULL THEN 'Да'
               ELSE 'Нет'
          END) AS DIRECTED_ANSWER  --Направлен ответ пользователю после работы 2-ой линии !!!!!!!!!!!!!!1
       , (CASE
            WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN NULL
            ELSE NVL(CST.COUNT_STATUS_WAITING, 0)
          END) AS COUNT_STATUS_WAITING  --Запрошена доп. информация у пользователя
       , (CASE
           WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN ''
           WHEN NVL(TPT.IS_TRANSFERRED, TTS.TASK_CODE) IS NULL THEN 'Нет'
           ELSE 'Да'
          END) AS SECOND_LINE --Направлено на 2-ю линию
       , (CASE
           WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN ''
           WHEN TTP.CLASSIFIER_NEW_LEV_1 = 'Деньги списаны с банковской карты' THEN 'Да'
           ELSE 'Нет'
          END) AS DIRECTED_TO_MPK  --Направлено на МПК
       , (CASE
           WHEN nvl(TPT.IS_RESOLVED,'false') = 'false' THEN NULL
           ELSE NVL(CST.COUNT_STATUS_WAITING_SECOND, 0)
          END) AS COUNT_STATUS_WAITING_SECOND --Направлена доп. информация на 2-ю линию
       
      FROM TICKETS TCK
      LEFT JOIN TICKETS_TASKS TTS
       ON TTS.FID_TICKET = TCK.ID_TICKET
      LEFT JOIN ALL_TICKETS_TYPES TTP --Выяснить, обязан ли тикет иметь тип
       ON TTP.ID_TICKET = TCK.ID_TICKET
      LEFT JOIN TICKETS_D_ADM_TYPES ADT
       ON ADT.ID_TYPE = TCK.FID_ADM_TYPE 
      LEFT JOIN TICKETS_PROCESSING_TIME TPT
       ON TPT.ID_TICKET = TCK.ID_TICKET
      LEFT JOIN CALCULATION_STATUSES CST
       ON CST.ID_TICKET = TCK.ID_TICKET
       
      WHERE TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME
      
      ORDER BY TCK.CREATED_AT
)
SELECT * FROM MAIN_DATA
;



  TYPE t_report_for_mks IS TABLE OF cur_report_for_mks%rowtype;

  FUNCTION fnc_report_for_mks
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

  ) RETURN t_report_for_mks pipelined;
  

END PKG_MAIL_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY                   PKG_MAIL_REPORTS AS


--------------------------------------------------------------
--     СТАТИСТИКА ПО КЛАССИФИКАЦИЯМ ОБРАЩЕНИЙ MAILREADER    --
--------------------------------------------------------------

    FUNCTION fnc_tickets_statistic
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_CLIENT_TYPE VARCHAR2
      , I_COMPANY_TYPE NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_tickets_statistic(I_INIT_TIME, I_FINISH_TIME,I_CLIENT_TYPE,I_COMPANY_TYPE,I_ADMIN_TYPE, I_GROUP)
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
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic_regions pipelined AS
  BEGIN

  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
   FOR L IN cur_tickets_statistic_regions(I_INIT_TIME, I_FINISH_TIME,I_CHANNEL,I_DST_ID,I_CLIENT_TYPE,I_COMPANY_TYPE,I_ADMIN_TYPE,I_GROUP)
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
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_tickets_statistic_COMPANY pipelined AS
  BEGIN

  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
   FOR L IN cur_tickets_statistic_COMPANY(I_INIT_TIME, I_FINISH_TIME,I_COMPANY_REGION,I_ADMIN_TYPE, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statistic_COMPANY;  


---------------------------------------------------------------
-- Статистика по классификациям в разрезе полномочий (TICKETS_D_COMPANY_TYPES) --
---------------------------------------------------------------

    FUNCTION fnc_statistic_ON_COMPANY_TRN
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2
      , I_CHANNEL VARCHAR2 --КАНАЛ
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_statistic_ON_COMPANY_TRN pipelined AS
  BEGIN

  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
   FOR L IN cur_statistic_ON_COMPANY_TRN(I_INIT_TIME, I_FINISH_TIME,I_ADMIN_TYPE,I_CHANNEL,I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_statistic_ON_COMPANY_TRN; 


  ----------------------------------------------------------------
  --              ОТЧЕТ ПО РАСЧЕТУ ОСС                          --
  ----------------------------------------------------------------

  FUNCTION fnc_calculation_occ
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_LOCATION VARCHAR2 := NULL

) RETURN t_calculation_occ pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
   FOR L IN cur_calculation_occ(I_INIT_TIME, I_FINISH_TIME, I_LOCATION)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_calculation_occ;


----------------------------------------------------------
--          ЛОГ ПИСЕМ (ДЛЯ ВЫГРУЗКИ В ЕИС)              --
----------------------------------------------------------

  FUNCTION fnc_mail_log_for_eis
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_STATUS NUMBER -- СТАТУС ПИСЬМА
    , I_LOGIN VARCHAR2 -- ОПЕРАТОР
    , I_DIRECTION VARCHAR2 -- НАПРАВЛЕНИЕ
) RETURN t_mail_log_for_eis pipelined AS
  BEGIN
    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
   FOR L IN cur_mail_log_for_eis(I_INIT_TIME, I_FINISH_TIME, I_STATUS, I_LOGIN, I_DIRECTION)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_mail_log_for_eis;

-----------------------------------------------------------------
--           ЛОГ ОБРАЩЕНИЙ (ДЛЯ ВЫГРУЗКИ В ЕИС)                --
-----------------------------------------------------------------

    FUNCTION fnc_ticket_log_for_eis
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS VARCHAR2 --СТАТУС ОБРАЩЕНИЯ
      , I_METKA VARCHAR2 -- МЕТКИ
      , I_ADMIN_TYPE NUMBER := NULL --Административный тип
) RETURN t_ticket_log_for_eis pipelined AS
  BEGIN
    EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';
  
   FOR L IN cur_ticket_log_for_eis(I_INIT_TIME, I_FINISH_TIME, I_STATUS, I_METKA, I_ADMIN_TYPE)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_ticket_log_for_eis;



--------------------------------------------------------------
--           ОТЧЕТ ПО СТАТУСАМ ОБРАЩЕНИЙ                    --
--------------------------------------------------------------

    FUNCTION fnc_tickets_statuses
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

) RETURN t_tickets_statuses pipelined AS
  BEGIN
  
      EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_tickets_statuses(I_INIT_TIME, I_FINISH_TIME,I_ADMIN_TYPE)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_tickets_statuses;


-----------------------------------------------------------
--         ОТЧЕТ ДЛЯ ОТДЕЛА ПЛАНИРОВАНИЯ                 --
-----------------------------------------------------------

    FUNCTION fnc_report_planning_dep
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_report_planning_dep pipelined AS
  BEGIN
  
      EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_report_planning_dep(I_INIT_TIME, I_FINISH_TIME, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_report_planning_dep;


--------------------------------------------------------------
--               ОТЧЕТ ПО SLA ДЛЯ КАНАЛА E-MAIL             --
--------------------------------------------------------------

    FUNCTION fnc_report_SLA
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_report_SLA pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_report_SLA(I_INIT_TIME, I_FINISH_TIME, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_report_SLA;
  
  
--------------------------------------------------------------
--               ОТЧЕТ Сроки обработки обращений            --
--------------------------------------------------------------

    FUNCTION fnc_TICKETS_proc_time
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
      , I_GROUP VARCHAR2 DEFAULT NULL

) RETURN t_TICKETS_proc_time pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_TICKETS_proc_time(I_INIT_TIME, I_FINISH_TIME,I_ADMIN_TYPE, I_GROUP)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_TICKETS_proc_time;
  
  
  
--------------------------------------------------------------
--         Отчет по заявкам для МКС - 1-й уровень           --
--------------------------------------------------------------

    FUNCTION fnc_report_for_mks
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP

) RETURN t_report_for_mks pipelined AS
  BEGIN
  EXECUTE IMMEDIATE 'alter session set nls_language = ''russian''';
  EXECUTE IMMEDIATE 'alter session set nls_territory = ''russia''';

   FOR L IN cur_report_for_mks(I_INIT_TIME, I_FINISH_TIME)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_report_for_mks;  
  

END PKG_MAIL_REPORTS;
/
