CREATE OR REPLACE PACKAGE PKG_ACSI_MAIL_REPORTS AS 
--                                                                              --
-- Отчетность для оценки удовлетворенности обработки обращений на канале E-mail --
-- Заявки ZHKKH-714 и ZHKKH-718                                                 --
--
----------------------------------------------------------------------------------
--      Детализированный отчет по оценке удовлетворенности обработки E-mail     --
----------------------------------------------------------------------------------


CURSOR cur_acsi_mail_log (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_EMAIL_LINE NUMBER
  )
IS
WITH 
GIS_ZHKH AS (SELECT * FROM DUAL),
ALL_TICKETS AS ( --Общая выборка оцененных обращений
SELECT 
 TCK.ID_TICKET
FROM QUALITY_RATING QR
JOIN TICKETS TCK
 ON TCK.ID_TICKET = QR.FID_TICKET
WHERE (QR.CREATED_AT >= I_INIT_TIME AND QR.CREATED_AT < I_FINISH_TIME) --Фильтр по дате опроса:
   AND TCK.IS_ACTIVE = 1

),
 ALL_TICKETS_TASKS AS--достает список задач в JIRA контакта
    (
    SELECT 
      ID_TICKET,
      LISTAGG(TASK_CODE,',  ') WITHIN GROUP (order by ID_TASK) AS TICKETS_TASKS

    FROM
      (  
         SELECT DISTINCT
            TCK.ID_TICKET
          , TTS.TASK_CODE 
          , TTS.ID_TASK
          FROM ALL_TICKETS ATCK
          JOIN TICKETS TCK
           ON TCK.ID_TICKET = ATCK.ID_TICKET
          JOIN TICKETS_TASKS TTS
           ON TTS.FID_TICKET = TCK.ID_TICKET

       )
       GROUP BY ID_TICKET
    ),
 TICKETS_INFO AS (
   SELECT 
     ATCK.ID_TICKET
   , MIN(CASE WHEN ACL.FID_TYPE = 3 THEN ACL.CREATED_AT ELSE NULL END) AS REGISTRY_TIME 
   , MIN(CASE WHEN ACL.FID_TYPE = 27 THEN ACL.CREATED_AT ELSE NULL END) AS CLOSED_TIME
   , MAX(CASE WHEN TTS.FID_TICKET IS NULL THEN 1 ELSE 2 END) AS EMAIL_LINE --Линия: 1-я линия поддержки (НК) 2-я линия поддержки (Ланит)
   FROM ALL_TICKETS ATCK
   JOIN USER_ACTIONS_LOG ACL
    ON ACL.LOGGABLE_ID = ATCK.ID_TICKET AND ACL.LOGGABLE_TYPE = 'TICKETS'
   LEFT JOIN (SELECT DISTINCT FID_TICKET
              FROM TICKETS_TASKS TTS
              JOIN ALL_TICKETS ATCK
               ON TTS.FID_TICKET = ATCK.ID_TICKET) TTS
     ON TTS.FID_TICKET = ATCK.ID_TICKET
   GROUP BY ATCK.ID_TICKET
 
  ),
 ALL_REASONS AS (
   SELECT 
    QR.ID_RATING,
    LISTAGG(RES.REASON_TEXT,',  ') WITHIN GROUP (order by HRES.ID_HAS) AS REASONS
  FROM ALL_TICKETS ATCK
  JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = ATCK.ID_TICKET
  JOIN Q_RATING_HAS_REASONS HRES
   ON HRES.FID_RATING = QR.ID_RATING
  JOIN Q_RATING_D_REASONS RES
   ON RES.ID_REASON = HRES.FID_REASON
  GROUP BY QR.ID_RATING
 ),
 
  ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
 -- , MAX(TDT.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS LAST_TYPE
  , MAX(TDT_LEV_1.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_1
  , MAX(TDT_LEV_2.NAME) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS CLASSIFIER_NEW_LEV_2
  , MAX(ADT.NAME) AS ADMIN_TYPE
  
  FROM ALL_TICKETS ATCK
  JOIN TICKETS TCK
   ON TCK.ID_TICKET = ATCK.ID_TICKET
  JOIN MAIL_MESSAGES MSG
   ON MSG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
--  LEFT JOIN TICKETS_D_TYPES TDT
--   ON TDT.ID_TYPE = TTP.FID_TYPE AND TDT.ID_TYPE BETWEEN 1 AND 13 --СТАРЫЙ ТИП
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_2  --MUST JOIN
   ON TDT_LEV_2.ID_TYPE = TTP.FID_TYPE AND TDT_LEV_2.IS_ACTIVE = 1
  LEFT JOIN TICKETS_D_TYPES TDT_LEV_1  --MUST JOIN
   ON TDT_LEV_1.ID_TYPE = TDT_LEV_2.ID_PARENT AND TDT_LEV_1.IS_ACTIVE = 1
  LEFT JOIN TICKETS_D_ADM_TYPES ADT
   ON ADT.ID_TYPE = TCK.FID_ADM_TYPE

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
  
  SELECT
     ATCK.ID_TICKET--№ обращения 
   ,  (CASE 
       WHEN TCF.EMAIL_LINE = 1
       THEN '1-я линия поддержки (НК)'
       WHEN TCF.EMAIL_LINE = 2
       THEN '2-я линия поддержки (Ланит)'
      END) AS EMAIL_LINE
  ,  TTP.CLASSIFIER_NEW_LEV_1 AS LAST_TYPE --Классификатор  
  ,  TTP.CLASSIFIER_NEW_LEV_2 AS LAST_TYPE_LEVEL_2 --Классификатор 2 LEVEL
  ,  TTP.ADMIN_TYPE  --Административный тип
  ,  TTS.TICKETS_TASKS--№ заявок в JIRA
  ,  TO_CHAR(TCF.REGISTRY_TIME,'dd.mm.yyyy hh24:mi:ss') AS REGISTRY_TIME --Дата/время регистрации обращения  --из user_action 3-й статус
  ,  TO_CHAR(TCF.CLOSED_TIME,'dd.mm.yyyy hh24:mi:ss') AS CLOSED_TIME  --Дата/время закрытия обращения  --из user_action 27-й статус
  ,  TO_CHAR(QR.CREATED_AT,'dd.mm.yyyy hh24:mi:ss') AS RATING_TIME--Дата/время оценки удовлетворенности --будет добавлена колонка
  ,  MRK.MARK_TEXT --Оценка
  ,  RSN.REASONS--Причина 
  ,  QR.COMMENTS--Причина_Другое
  FROM ALL_TICKETS ATCK
  JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = ATCK.ID_TICKET
  JOIN Q_RATING_D_MARKS MRK
   ON MRK.ID_MARK = QR.FID_MARK
  JOIN TICKETS_INFO TCF
   ON TCF.ID_TICKET = ATCK.ID_TICKET
  LEFT JOIN ALL_TICKETS_TASKS TTS
   ON TTS.ID_TICKET = ATCK.ID_TICKET
  LEFT JOIN ALL_REASONS RSN
   ON RSN.ID_RATING = QR.ID_RATING
  LEFT JOIN ALL_TICKETS_TYPES TTP
   ON TTP.ID_TICKET = ATCK.ID_TICKET
  WHERE (TCF.EMAIL_LINE = I_EMAIL_LINE OR I_EMAIL_LINE IS NULL)
  ORDER BY QR.CREATED_AT ASC
  ;
  
TYPE t_acsi_mail_log IS TABLE OF cur_acsi_mail_log%rowtype;

FUNCTION fnc_acsi_mail_log
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_EMAIL_LINE NUMBER

) RETURN t_acsi_mail_log pipelined;



----------------------------------------------------------------------------------
--      Сводный отчет по оценке удовлетворенности обработки E-mail              --
----------------------------------------------------------------------------------


CURSOR cur_acsi_mail_general (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_TYPE_TIME_FILTER NUMBER
      , I_GROUP VARCHAR
      , I_REGION NUMBER
  )
IS
    WITH
    GIS_ZHKH AS (SELECT * FROM DUAL),
    PERIODS AS
      (
        SELECT
          CAST(GREATEST(PERIOD_START_TIME, I_INIT_TIME) AS TIMESTAMP) AS START_PERIOD,
          CAST(PERIOD_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
          0 AS ITOG_IND  --Период предназначен для итоговых значений (да = 1, нет = 0)
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
           
           UNION        --ДЛЯ ВЫВОДА РЕЗУЛЬТАТОВ ЗА ВЕСЬ ПЕРИОД        
        SELECT  
          CAST(I_INIT_TIME AS TIMESTAMP) AS START_PERIOD,
          CAST(I_FINISH_TIME AS TIMESTAMP) AS STOP_PERIOD,
          CASE
           WHEN I_GROUP IS NULL OR I_GROUP = 'year'
           THEN 0
           WHEN I_GROUP = 'day' and TRUNC(I_INIT_TIME) = TRUNC(I_FINISH_TIME - interval '1' minute)
           THEN 0
           WHEN I_GROUP = 'hour' and TRUNC(I_INIT_TIME) = TRUNC(I_FINISH_TIME - interval '1' minute)
            AND EXTRACT(HOUR FROM I_INIT_TIME) = EXTRACT(HOUR FROM (I_FINISH_TIME - interval '1' minute))
           THEN 0
           ELSE 1
          END AS ITOG_IND
      
        FROM DUAL
            
      )
, FORMAT AS (
  SELECT * FROM PERIODS,
   (
       SELECT '1-я линия поддержки' AS EMAIL_LINE FROM DUAL
        UNION ALL
       SELECT '2-я линия поддержки' AS EMAIL_LINE FROM DUAL 
    ) EL
  )      

--Множество обращений для обработки
, PREP_ALL_CLOSE_TICKETS AS (
 SELECT 
   TCT.ID_TICKET
 FROM TICKETS TCT
 JOIN USER_ACTIONS_LOG ACL
  ON ACL.LOGGABLE_ID = TCT.ID_TICKET AND ACL.LOGGABLE_TYPE = 'TICKETS'
  WHERE ACL.CREATED_AT >= I_INIT_TIME AND ACL.CREATED_AT < I_FINISH_TIME
    AND ACL.FID_TYPE = 27
    AND TCT.IS_ACTIVE = 1
 
)
, ALL_CLOSE_TICKETS AS (
 SELECT 
   TCT.ID_TICKET,
   MIN(ACL.CREATED_AT) AS CLOSED_TIME
 FROM PREP_ALL_CLOSE_TICKETS TCT
 JOIN USER_ACTIONS_LOG ACL
  ON ACL.LOGGABLE_ID = TCT.ID_TICKET AND ACL.LOGGABLE_TYPE = 'TICKETS'
  WHERE ACL.FID_TYPE = 27
  GROUP BY TCT.ID_TICKET  
)

, ALL_TICKETS AS (
--Два типа фильтров по времени:
--Это выводится независимо от фильтров
SELECT ID_TICKET
FROM ALL_CLOSE_TICKETS
WHERE (CLOSED_TIME >= I_INIT_TIME AND CLOSED_TIME < I_FINISH_TIME)

UNION
--Фильтр Закрытые и оцененные за период
-- выводит данные по количеству закрытых и количеству оцененных обращений независимо друг от друга^
SELECT 
 TCK.ID_TICKET
FROM QUALITY_RATING QR
JOIN TICKETS TCK
 ON TCK.ID_TICKET = QR.FID_TICKET
WHERE I_TYPE_TIME_FILTER = 1 
 AND (QR.CREATED_AT >= I_INIT_TIME AND QR.CREATED_AT < I_FINISH_TIME)
 AND TCK.IS_ACTIVE = 1
--Фильтр По дате закрытия + оцененные из них
--выводит данные по количеству закрытых и количеству оцененных обращений из выбранных закрытых (по примеру воронки)
--или если перевести на мой язык: "брать оцененные обращения из множества закрытых"
--ДЛЯ ЭТОГО НЕ НУЖНО ФОРМИРОВАТЬ ОТДЕЛЬНОЙ ВЫБОРКИ
--ОНА УЖЕ ВЫВОДИТСЯ ИЗ ALL_CLOSE_TICKETS

-- Дату закрытия всегда считать фиксированной
-- Брать нужно первую дату закрытия обращения, если оно переоткрывалось.
),
ALL_REASONS AS (
   SELECT 
     QR.ID_RATING
    --0 - ОЗНАЧАЕТ ОТСУТСВИЕ ВЫБОРА ПРИЧИНЫ, 1 - ПРИЧИНА ВЫБРАНА:
   , MAX(CASE WHEN RES.ID_REASON = 1 THEN 1 ELSE 0 END) AS REASON_1
   , MAX(CASE WHEN RES.ID_REASON = 2 THEN 1 ELSE 0 END) AS REASON_2
   , MAX(CASE WHEN RES.ID_REASON = 3 THEN 1 ELSE 0 END) AS REASON_3
   , MAX(CASE WHEN RES.ID_REASON = 4 THEN 1 ELSE 0 END) AS REASON_4
   , MAX(CASE WHEN RES.ID_REASON = 5 THEN 1 ELSE 0 END) AS REASON_5
   , MAX(CASE WHEN RES.ID_REASON = 6 THEN 1 ELSE 0 END) AS REASON_6
   , MAX(CASE WHEN RES.ID_REASON = 7 THEN 1 ELSE 0 END) AS REASON_7
   , MAX(CASE WHEN RES.ID_REASON = 8 THEN 1 ELSE 0 END) AS REASON_8
   , MAX(CASE WHEN RES.ID_REASON = 9 THEN 1 ELSE 0 END) AS REASON_9
   , MAX(CASE WHEN RES.ID_REASON = 10 THEN 1 ELSE 0 END) AS REASON_10  

  FROM ALL_TICKETS ATCK
  JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = ATCK.ID_TICKET
  JOIN Q_RATING_HAS_REASONS HRES
   ON HRES.FID_RATING = QR.ID_RATING
  JOIN Q_RATING_D_REASONS RES
   ON RES.ID_REASON = HRES.FID_REASON
  GROUP BY QR.ID_RATING
 ) ,
 ALL_RATINGS AS (
   SELECT 
     TCK.ID_TICKET
   , COALESCE(QR.CREATED_AT, CLT.CLOSED_TIME) AS RATING_TIME  
   , QR.ID_RATING
   , (CASE WHEN TTS.FID_TICKET IS NULL THEN '1-я линия поддержки' ELSE '2-я линия поддержки' END) AS EMAIL_LINE --Линия: 1-я линия поддержки (НК) 2-я линия поддержки (Ланит)
   , MRK.MARK_NUM
   , (CASE 
       WHEN ARS.ID_RATING IS NOT NULL THEN 1
       WHEN QR.ID_RATING IS NOT NULL THEN 0
       ELSE NULL
     END) AS CHOOSED_REASONS 
   , ARS.REASON_1
   , ARS.REASON_2
   , ARS.REASON_3
   , ARS.REASON_4
   , ARS.REASON_5
   , ARS.REASON_6
   , ARS.REASON_7
   , ARS.REASON_8
   , ARS.REASON_9
   , ARS.REASON_10
  FROM ALL_TICKETS TCK
  LEFT JOIN TICKETS TC
   ON TC.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN ALL_CLOSE_TICKETS CLT
   ON CLT.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN Q_RATING_D_MARKS MRK
   ON MRK.ID_MARK = QR.FID_MARK
  LEFT JOIN ALL_REASONS ARS
   ON ARS.ID_RATING = QR.ID_RATING
  LEFT JOIN (SELECT DISTINCT FID_TICKET
              FROM TICKETS_TASKS TTS
              JOIN ALL_TICKETS TCK
               ON TTS.FID_TICKET = TCK.ID_TICKET) TTS
   ON TTS.FID_TICKET = TCK.ID_TICKET
   
  WHERE (NVL(TC.FID_COMPANY_REGION,85) = I_REGION OR I_REGION IS NULL) 
 )    

 , RATING_STATISTICA AS (
 SELECT 
   F.EMAIL_LINE
 , START_PERIOD
 , STOP_PERIOD
 , ITOG_IND
 , MAX(CASE WHEN F.EMAIL_LINE = '1-я линия поддержки' THEN 11 ELSE 21 END) AS ORD
 , COUNT(ID_TICKET) AS CLOSED_TICKETS --Закрыто обращений
 , COUNT(ID_RATING) AS PASSED_RATING --Прошли опрос всего (кол-во)
 , SUM(CASE WHEN CHOOSED_REASONS = 1 THEN 1 ELSE 0 END) AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
 , SUM(CASE WHEN CHOOSED_REASONS = 0 THEN 1 ELSE 0 END) AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
 , '-' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений 
 , SUM(CASE WHEN MARK_NUM = 5 THEN 1 ELSE 0 END) AS MARK_5 --Количество оценок 5
 , SUM(CASE WHEN MARK_NUM = 4 THEN 1 ELSE 0 END) AS MARK_4
 , SUM(CASE WHEN MARK_NUM = 3 THEN 1 ELSE 0 END) AS MARK_3
 , SUM(CASE WHEN MARK_NUM = 2 THEN 1 ELSE 0 END) AS MARK_2
 , SUM(CASE WHEN MARK_NUM = 1 THEN 1 ELSE 0 END) AS MARK_1 --Количество оценок 1
 , '-' AS CSAT
 , '-' AS CDSAT
 , SUM(CASE WHEN REASON_1 = 1 THEN 1 ELSE 0 END) AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
 , SUM(CASE WHEN REASON_2 = 1 THEN 1 ELSE 0 END) AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
 , SUM(CASE WHEN REASON_3 = 1 THEN 1 ELSE 0 END) AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
 , SUM(CASE WHEN REASON_4 = 1 THEN 1 ELSE 0 END) AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
 , SUM(CASE WHEN REASON_5 = 1 THEN 1 ELSE 0 END) AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
 , SUM(CASE WHEN REASON_6 = 1 THEN 1 ELSE 0 END) AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
 , SUM(CASE WHEN REASON_7 = 1 THEN 1 ELSE 0 END) AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
 , SUM(CASE WHEN REASON_8 = 1 THEN 1 ELSE 0 END) AS REASON_8 --Причина удовлетворенности «Вопрос решен»
 , SUM(CASE WHEN REASON_9 = 1 THEN 1 ELSE 0 END) AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
 , SUM(CASE WHEN REASON_10 = 1 AND MARK_NUM IN (1,2,3) THEN 1 ELSE 0 END) AS REASON_10_NEG --Причина неудовлетворенности «Другое»
 , SUM(CASE WHEN REASON_10 = 1 AND MARK_NUM IN (4,5) THEN 1 ELSE 0 END) AS REASON_10_POS --Причина удовлетворенности «Другое»
 , SUM(CASE 
        WHEN REASON_1 = 1 OR REASON_2 = 1 OR REASON_3 = 1 OR REASON_4 = 1 OR REASON_5 = 1 OR (REASON_10 = 1 AND MARK_NUM IN (1,2,3))
        THEN 1
        ELSE 0
       END) AS REASON_ALL_NEG
 , SUM(CASE 
        WHEN REASON_6 = 1 OR REASON_7 = 1 OR REASON_8 = 1 OR REASON_9 = 1 OR (REASON_10 = 1 AND MARK_NUM IN (4,5))
        THEN 1
        ELSE 0
       END) AS REASON_ALL_POS       
 
 , SUM(CASE WHEN MARK_NUM IN (1,2,3) THEN 1 ELSE 0 END) AS MARK_1_2_3 
 , SUM(CASE WHEN MARK_NUM IN (4,5) THEN 1 ELSE 0 END) AS MARK_4_5

 FROM ALL_RATINGS TR
 RIGHT JOIN FORMAT F
  ON TR.RATING_TIME >= F.START_PERIOD AND TR.RATING_TIME < F.STOP_PERIOD AND TR.EMAIL_LINE = F.EMAIL_LINE
        
 GROUP BY F.EMAIL_LINE, START_PERIOD,STOP_PERIOD, ITOG_IND 
 )

, RATING_STATISTICA_SUM AS (--Мне проще это отдельно расчитать, чем в UNION

    SELECT
     'Итого' AS EMAIL_LINE
   , START_PERIOD
   , STOP_PERIOD
   , ITOG_IND
   , 31 AS ORD
   , SUM(NVL(CLOSED_TICKETS,0)) AS CLOSED_TICKETS  --Закрыто обращений
   , SUM(NVL(PASSED_RATING,0)) AS PASSED_RATING --Прошли опрос всего (кол-во)
   , SUM(NVL(PASSED_RATING_FULL,0)) AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
   , SUM(NVL(PASSED_RATING_NOT_FULL,0)) AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
   , '-' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
   , SUM(NVL(MARK_5,0)) AS MARK_5 --Количество оценок 5
   , SUM(NVL(MARK_4,0)) AS MARK_4
   , SUM(NVL(MARK_3,0)) AS MARK_3
   , SUM(NVL(MARK_2,0)) AS MARK_2
   , SUM(NVL(MARK_1,0)) AS MARK_1 --Количество оценок 1
   , '-' AS CSAT
   , '-' AS CDSAT
   , SUM(NVL(REASON_1,0)) AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
   , SUM(NVL(REASON_2,0)) AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
   , SUM(NVL(REASON_3,0)) AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
   , SUM(NVL(REASON_4,0)) AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
   , SUM(NVL(REASON_5,0)) AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
   , SUM(NVL(REASON_6,0)) AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
   , SUM(NVL(REASON_7,0)) AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
   , SUM(NVL(REASON_8,0)) AS REASON_8 --Причина удовлетворенности «Вопрос решен»
   , SUM(NVL(REASON_9,0)) AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
   , SUM(NVL(REASON_10_NEG,0)) AS REASON_10_NEG --Причина неудовлетворенности «Другое»
   , SUM(NVL(REASON_10_POS,0)) AS REASON_10_POS --Причина удовлетворенности «Другое»
   , SUM(NVL(MARK_1_2_3,0)) AS MARK_1_2_3
   , SUM(NVL(MARK_4_5,0)) AS MARK_4_5
   , SUM(NVL(REASON_ALL_NEG,0)) AS REASON_ALL_NEG
   , SUM(NVL(REASON_ALL_POS,0)) AS REASON_ALL_POS
   
   
   FROM RATING_STATISTICA
   
   GROUP BY     
     START_PERIOD
   , STOP_PERIOD
   , ITOG_IND
  ) 
  
, RATING_STATISTICA_UNION AS (
   --Вывод значений по 1-й и 2-й линия поддержки
   SELECT 
     EMAIL_LINE
   , START_PERIOD
   , STOP_PERIOD
   , ITOG_IND
   , ORD
   , TO_CHAR(CLOSED_TICKETS) AS CLOSED_TICKETS  --Закрыто обращений
   , TO_CHAR(PASSED_RATING) AS PASSED_RATING --Прошли опрос всего (кол-во)
   , TO_CHAR(PASSED_RATING_FULL) AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
   , TO_CHAR(PASSED_RATING_NOT_FULL) AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
   , '-' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
   , TO_CHAR(MARK_5) AS MARK_5 --Количество оценок 5
   , TO_CHAR(MARK_4) AS MARK_4
   , TO_CHAR(MARK_3) AS MARK_3
   , TO_CHAR(MARK_2) AS MARK_2
   , TO_CHAR(MARK_1) AS MARK_1 --Количество оценок 1
   , '-' AS CSAT
   , '-' AS CDSAT
   , TO_CHAR(REASON_1) AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
   , TO_CHAR(REASON_2) AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
   , TO_CHAR(REASON_3) AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
   , TO_CHAR(REASON_4) AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
   , TO_CHAR(REASON_5) AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
   , TO_CHAR(REASON_6) AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
   , TO_CHAR(REASON_7) AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
   , TO_CHAR(REASON_8) AS REASON_8 --Причина удовлетворенности «Вопрос решен»
   , TO_CHAR(REASON_9) AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
   , TO_CHAR(REASON_10_NEG) AS REASON_10_NEG --Причина неудовлетворенности «Другое»
   , TO_CHAR(REASON_10_POS) AS REASON_10_POS --Причина удовлетворенности «Другое»
   
   FROM RATING_STATISTICA
   
   UNION ALL
   --Всего
      SELECT 
     EMAIL_LINE
   , START_PERIOD
   , STOP_PERIOD
   , ITOG_IND
   , ORD
   , TO_CHAR(CLOSED_TICKETS) AS CLOSED_TICKETS  --Закрыто обращений
   , TO_CHAR(PASSED_RATING) AS PASSED_RATING --Прошли опрос всего (кол-во)
   , TO_CHAR(PASSED_RATING_FULL) AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
   , TO_CHAR(PASSED_RATING_NOT_FULL) AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(PASSED_RATING/DECODE(CLOSED_TICKETS,0,1,CLOSED_TICKETS),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
   , TO_CHAR(MARK_5) AS MARK_5 --Количество оценок 5
   , TO_CHAR(MARK_4) AS MARK_4
   , TO_CHAR(MARK_3) AS MARK_3
   , TO_CHAR(MARK_2) AS MARK_2
   , TO_CHAR(MARK_1) AS MARK_1 --Количество оценок 1
   , REPLACE(TRIM(TO_CHAR(NVL(MARK_4_5/DECODE(PASSED_RATING,0,1,PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CSAT
   , REPLACE(TRIM(TO_CHAR(NVL(MARK_1/DECODE(PASSED_RATING,0,1,PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CDSAT
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_1/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_2/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_3/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_4/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_5/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_6/DECODE(REASON_ALL_POS,0,1,REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_7/DECODE(REASON_ALL_POS,0,1,REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_8/DECODE(REASON_ALL_POS,0,1,REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_8 --Причина удовлетворенности «Вопрос решен»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_9/DECODE(REASON_ALL_POS,0,1,REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_10_NEG/DECODE(REASON_ALL_NEG,0,1,REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_10_NEG --Причина неудовлетворенности «Другое»
   , REPLACE(TRIM(TO_CHAR(NVL(REASON_10_POS/DECODE(REASON_ALL_POS,0,1,REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_10_POS --Причина удовлетворенности «Другое»
   FROM RATING_STATISTICA_SUM
     
   
   UNION ALL 
   --1-я линия, %
    SELECT 
     '1-я линия, %' AS EMAIL_LINE
   , SL.START_PERIOD
   , SL.STOP_PERIOD
   , SL.ITOG_IND
   , 12 AS ORD
   , REPLACE(TRIM(TO_CHAR(NVL(SL.CLOSED_TICKETS/DECODE(SS.CLOSED_TICKETS,0,1,SS.CLOSED_TICKETS),0)*100,'990D9')),'.',',')||'%' AS CLOSED_TICKETS  --Закрыто обращений
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING/DECODE(SS.PASSED_RATING,0,1,SS.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING --Прошли опрос всего (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING_FULL/DECODE(SS.PASSED_RATING_FULL,0,1,SS.PASSED_RATING_FULL),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING_NOT_FULL/DECODE(SS.PASSED_RATING_NOT_FULL,0,1,SS.PASSED_RATING_NOT_FULL),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING/DECODE(SL.CLOSED_TICKETS,0,1,SL.CLOSED_TICKETS),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_5/DECODE(SL.MARK_5,0,1,SL.MARK_5),0)*100,'990D9')),'.',',')||'%' AS MARK_5 --Количество оценок 5
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_4/DECODE(SL.MARK_4,0,1,SL.MARK_4),0)*100,'990D9')),'.',',')||'%' AS MARK_4
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_3/DECODE(SL.MARK_3,0,1,SL.MARK_3),0)*100,'990D9')),'.',',')||'%' AS MARK_3
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_2/DECODE(SL.MARK_2,0,1,SL.MARK_2),0)*100,'990D9')),'.',',')||'%' AS MARK_2
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_1/DECODE(SL.MARK_1,0,1,SL.MARK_1),0)*100,'990D9')),'.',',')||'%' AS MARK_1 --Количество оценок 1
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_4_5/DECODE(SL.PASSED_RATING,0,1,SL.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CSAT
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_1/DECODE(SL.PASSED_RATING,0,1,SL.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CDSAT
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_1/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_2/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_3/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_4/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_5/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_6/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_7/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_8/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_8 --Причина удовлетворенности «Вопрос решен»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_9/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_10_NEG/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_10_NEG --Причина неудовлетворенности «Другое»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_10_POS/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_10_POS --Причина удовлетворенности «Другое»
   FROM RATING_STATISTICA_SUM SS
   JOIN RATING_STATISTICA SL 
    ON SL.START_PERIOD = SS.START_PERIOD AND
       SL.STOP_PERIOD = SS.STOP_PERIOD AND 
       SL.ITOG_IND = SS.ITOG_IND AND 
       SL.EMAIL_LINE = '1-я линия поддержки'

   UNION ALL 
   --2-я линия, %
    SELECT 
     '2-я линия, %' AS EMAIL_LINE
   , SL.START_PERIOD
   , SL.STOP_PERIOD
   , SL.ITOG_IND
   , 22 AS ORD
   , REPLACE(TRIM(TO_CHAR(NVL(SL.CLOSED_TICKETS/DECODE(SS.CLOSED_TICKETS,0,1,SS.CLOSED_TICKETS),0)*100,'990D9')),'.',',')||'%' AS CLOSED_TICKETS  --Закрыто обращений
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING/DECODE(SS.PASSED_RATING,0,1,SS.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING --Прошли опрос всего (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING_FULL/DECODE(SS.PASSED_RATING_FULL,0,1,SS.PASSED_RATING_FULL),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING_NOT_FULL/DECODE(SS.PASSED_RATING_NOT_FULL,0,1,SS.PASSED_RATING_NOT_FULL),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
   , REPLACE(TRIM(TO_CHAR(NVL(SL.PASSED_RATING/DECODE(SL.CLOSED_TICKETS,0,1,SL.CLOSED_TICKETS),0)*100,'990D9')),'.',',')||'%' AS PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_5/DECODE(SL.MARK_5,0,1,SL.MARK_5),0)*100,'990D9')),'.',',')||'%' AS MARK_5 --Количество оценок 5
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_4/DECODE(SL.MARK_4,0,1,SL.MARK_4),0)*100,'990D9')),'.',',')||'%' AS MARK_4
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_3/DECODE(SL.MARK_3,0,1,SL.MARK_3),0)*100,'990D9')),'.',',')||'%' AS MARK_3
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_2/DECODE(SL.MARK_2,0,1,SL.MARK_2),0)*100,'990D9')),'.',',')||'%' AS MARK_2
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_1/DECODE(SL.MARK_1,0,1,SL.MARK_1),0)*100,'990D9')),'.',',')||'%' AS MARK_1 --Количество оценок 1
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_4_5/DECODE(SL.PASSED_RATING,0,1,SL.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CSAT
   , REPLACE(TRIM(TO_CHAR(NVL(SL.MARK_1/DECODE(SL.PASSED_RATING,0,1,SL.PASSED_RATING),0)*100,'990D9')),'.',',')||'%' AS CDSAT
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_1/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_2/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_2 --Причина неудовлетворенности «Ответ непонятный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_3/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_3 --Причина неудовлетворенности «Ответ неполный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_4/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_4 --Причина неудовлетворенности «Вопрос не решен»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_5/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_6/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_7/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_8/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_8 --Причина удовлетворенности «Вопрос решен»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_9/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_10_NEG/DECODE(SL.REASON_ALL_NEG,0,1,SL.REASON_ALL_NEG),0)*100,'990D9')),'.',',')||'%' AS REASON_10_NEG --Причина неудовлетворенности «Другое»
   , REPLACE(TRIM(TO_CHAR(NVL(SL.REASON_10_POS/DECODE(SL.REASON_ALL_POS,0,1,SL.REASON_ALL_POS),0)*100,'990D9')),'.',',')||'%' AS REASON_10_POS --Причина удовлетворенности «Другое»
   FROM RATING_STATISTICA_SUM SS
   JOIN RATING_STATISTICA SL 
    ON SL.START_PERIOD = SS.START_PERIOD AND
       SL.STOP_PERIOD = SS.STOP_PERIOD AND 
       SL.ITOG_IND = SS.ITOG_IND AND 
       SL.EMAIL_LINE = '2-я линия поддержки'    
     
  )
  SELECT 
    EMAIL_LINE
  , (CASE
      WHEN ITOG_IND = 1
      THEN 'За весь период'
      ELSE TO_CHAR(START_PERIOD, 'dd.mm.yyyy hh24:mi')
    END) AS PERIOD --Период
  , STOP_PERIOD
  , ITOG_IND
  , ORD
  , CLOSED_TICKETS  --Закрыто обращений
  , PASSED_RATING --Прошли опрос всего (кол-во)
  , PASSED_RATING_FULL -- Прошли опрос полностью (кол-во)
  , PASSED_RATING_NOT_FULL --Прошли опрос НЕ полностью (кол-во)
  , PASSED_RATING_PERCENT --Доля прошедших опрос от закрытых обращений
  , MARK_5 --Количество оценок 5
  , MARK_4 --Количество оценок 4
  , MARK_3 --Количество оценок 3
  , MARK_2 --Количество оценок 2
  , MARK_1 --Количество оценок 1
  , CSAT
  , CDSAT
  , REASON_1 --Причина неудовлетворенности «Долго решали вопрос»
  , REASON_2 --Причина неудовлетворенности «Ответ непонятный»
  , REASON_3 --Причина неудовлетворенности «Ответ неполный»
  , REASON_4 --Причина неудовлетворенности «Вопрос не решен»
  , REASON_5 --Причина неудовлетворенности «Ответ не соответствует заданному вопросу/ситуации»
  , REASON_6 --Причина удовлетворенности «Быстро решили вопрос»
  , REASON_7 --Причина удовлетворенности «Ответ полный и понятный»
  , REASON_8 --Причина удовлетворенности «Вопрос решен»
  , REASON_9 --Причина удовлетворенности «Ответ соответствует заданному вопросу/ситуации»
  , REASON_10_NEG --Причина неудовлетворенности «Другое»
  , REASON_10_POS --Причина удовлетворенности «Другое»
  
  FROM RATING_STATISTICA_UNION
  ORDER BY ITOG_IND, START_PERIOD,STOP_PERIOD, ORD
  ;




TYPE t_acsi_mail_general IS TABLE OF cur_acsi_mail_general%rowtype;

FUNCTION fnc_acsi_mail_general
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_TYPE_TIME_FILTER NUMBER
      , I_GROUP VARCHAR
      , I_REGION NUMBER

) RETURN t_acsi_mail_general pipelined;


----------------------------------------------------------------------------------
--      Статистика по результатам опроса на удовлетворенность обработки E-mail в разрезе тематик              --
----------------------------------------------------------------------------------


CURSOR cur_acsi_mail_statistic (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип
  )
IS
WITH 
   GIS_ZHKH AS (SELECT * FROM DUAL)
,  ALL_TICKETS AS ( --Общая выборка оцененных обращений
          SELECT 
       TCK.ID_TICKET
      FROM QUALITY_RATING QR
      JOIN TICKETS TCK
       ON TCK.ID_TICKET = QR.FID_TICKET
      WHERE (QR.CREATED_AT >= I_INIT_TIME AND QR.CREATED_AT < I_FINISH_TIME) --Фильтр по дате опроса:
         AND TCK.IS_ACTIVE = 1

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
          WHERE TDT_LEV_2.NAME != 'Тестовое обращение'            
--          UNION ALL
--          SELECT 
--                  1001 AS ID_TYPE_LEVEL_1 --ID ТИПА ПЕРВОГО УРОВНЯ
--                  , 1001 AS ID_TYPE_LEVEL_2 --ID ТИПА ВТОРОГО УРОВНЯ  
--                  , 'Посторонний звонок' AS TYPE_NAME_LEVEL_1 --ТИП ПЕРВОГО УРОВНЯ
--                  , 'Посторонний звонок' AS TYPE_NAME_LEVEL_2 --ТИП ВТОРОГО УРОВНЯ
--                  , '-' AS CLASS_TYPE --(ГРАЖДАНИН ИЛИ НЕ ГРАЖДАНИН)
--                  , 3 AS ORD
--          FROM DUAL  
          --ORDER BY (case when act.code = 'not_citizen' then 1 else 2 end),TDT_LEV_1.ID_TYPE, TDT_LEV_2.ID_TYPE
  ) 
  , FORMAT AS (
          SELECT * 
          FROM /*PERIODS
                  ,*/ ALL_TYPES_FOR_FORMAT TTP
  --  ORDER BY START_PERIOD,ORD,(case when CLASS_TYPE = 'Гражданин' then 1 else 2 end), ID_TYPE_LEVEL_1,ID_TYPE_LEVEL_2
  )

, ALL_TICKETS_TYPES AS (--КлассификаторЫ
        SELECT
          TCK.ID_TICKET AS ID_TICKET
        , MAX(TDT.TYPE_NAME_LEVEL_1) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_1
        , MAX(TDT.TYPE_NAME_LEVEL_2) KEEP (DENSE_RANK LAST ORDER BY TTP.ID_HAS) AS TYPE_NAME_LEVEL_2
        , MAX((CASE WHEN DCT.NAME = 'Гражданин' then 'Гражданин' else 'НЕ гражданин' END)) KEEP (DENSE_RANK LAST ORDER BY CTP.ID_HAS) AS CLASS_TYPE
        , MAX(ADT.NAME) AS ADMIN_TYPE
        
        FROM ALL_TICKETS ATCK
        JOIN TICKETS TCK
         ON TCK.ID_TICKET = ATCK.ID_TICKET
        JOIN MAIL_MESSAGES MSG
         ON MSG.FID_TICKET = TCK.ID_TICKET
        JOIN TICKETS_HAS_TYPES TTP
         ON TTP.FID_TICKET = TCK.ID_TICKET
        LEFT JOIN ALL_TYPES TDT  --MUST JOIN
         ON TDT.ID_TYPE_LEVEL_2 = TTP.FID_TYPE
        LEFT JOIN TICKETS_HAS_CMP_TPS CTP
         ON CTP.FID_TICKET = TCK.ID_TICKET 
        LEFT JOIN TICKETS_D_COMPANY_TYPES DCT
         ON DCT.ID_COMPANY_TYPE = CTP.FID_COMPANY_TYPE
        LEFT JOIN TICKETS_D_ADM_TYPES ADT
         ON ADT.ID_TYPE = TCK.FID_ADM_TYPE
        
        GROUP BY TCK.ID_TICKET
  ),
  
 ALL_RATINGS AS (
   SELECT 
     TCK.ID_TICKET
   , QR.ID_RATING
   , (CASE WHEN TTS.FID_TICKET IS NULL THEN '1-я линия поддержки' ELSE '2-я линия поддержки' END) AS EMAIL_LINE --Линия: 1-я линия поддержки (НК) 2-я линия поддержки (Ланит)
   , MRK.MARK_NUM
   , TTP.TYPE_NAME_LEVEL_1
   , TTP.TYPE_NAME_LEVEL_2
   , TTP.CLASS_TYPE AS CLASS_TYPE


  FROM ALL_TICKETS TCK
  JOIN TICKETS TC
   ON TC.ID_TICKET = TCK.ID_TICKET
  JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN Q_RATING_D_MARKS MRK
   ON MRK.ID_MARK = QR.FID_MARK
  LEFT JOIN ALL_TICKETS_TYPES TTP
   ON TTP.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN (SELECT DISTINCT FID_TICKET
              FROM TICKETS_TASKS TTS
              JOIN ALL_TICKETS TCK
               ON TTS.FID_TICKET = TCK.ID_TICKET) TTS
   ON TTS.FID_TICKET = TCK.ID_TICKET
   
  WHERE (NVL(TC.FID_COMPANY_REGION,85) = I_REGION OR I_REGION IS NULL) 
    AND (TTP.ADMIN_TYPE = I_ADMIN_TYPE OR I_ADMIN_TYPE IS NULL OR (I_ADMIN_TYPE = 'Не задан' AND TTP.ADMIN_TYPE IS NULL))--ZHKKH-917--Административный тип
 ), 

 RATING_STATISTICA AS ( 
 
 SELECT
     DECODE(GROUPING(FT.TYPE_NAME_LEVEL_1)
                ,0,FT.TYPE_NAME_LEVEL_1,'Всего') AS TYPE_NAME_LEVEL_1 --Классификация по теме
   , FT.TYPE_NAME_LEVEL_2
   , FT.CLASS_TYPE
   , MAX(FT.ID_TYPE_LEVEL_1) AS ID_TYPE_LEVEL_1
   , MAX(FT.ID_TYPE_LEVEL_2) AS ID_TYPE_LEVEL_2
   , MAX(FT.ORD) AS ORD
   
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '1-я линия поддержки'
          THEN 1
          ELSE 0
         END) AS EMAIL_LINE_1
   , AVG(CASE
          WHEN RT.EMAIL_LINE = '1-я линия поддержки'
          THEN RT.MARK_NUM
          ELSE NULL
         END) AS AVG_MARK_LINE_1
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '1-я линия поддержки'
           AND RT.MARK_NUM IN (4,5)
          THEN 1 
          ELSE 0
         END) AS FOR_CSAT_LINE_1       
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '1-я линия поддержки'
           AND RT.MARK_NUM = 1
          THEN 1 
          ELSE 0
         END) AS FOR_CDSAT_LINE_1
         
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '2-я линия поддержки'
          THEN 1
          ELSE 0
         END) AS EMAIL_LINE_2
   , AVG(CASE
          WHEN RT.EMAIL_LINE = '2-я линия поддержки'
          THEN RT.MARK_NUM
          ELSE NULL
         END) AS AVG_MARK_LINE_2
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '2-я линия поддержки'
           AND RT.MARK_NUM IN (4,5)
          THEN 1 
          ELSE 0
         END) AS FOR_CSAT_LINE_2       
   , SUM(CASE
          WHEN RT.EMAIL_LINE = '2-я линия поддержки'
           AND RT.MARK_NUM = 1
          THEN 1 
          ELSE 0
         END) AS FOR_CDSAT_LINE_2
         
   , SUM(CASE
          WHEN RT.EMAIL_LINE IS NOT NULL
          THEN 1
          ELSE 0
         END) AS EMAIL_ITOGO
   , AVG(CASE
          WHEN RT.EMAIL_LINE IS NOT NULL
          THEN RT.MARK_NUM
          ELSE NULL
         END) AS AVG_MARK_ITOGO
   , SUM(CASE
          WHEN RT.EMAIL_LINE IS NOT NULL
           AND RT.MARK_NUM IN (4,5)
          THEN 1 
          ELSE 0
         END) AS FOR_CSAT_ITOGO       
   , SUM(CASE
          WHEN RT.EMAIL_LINE IS NOT NULL
           AND RT.MARK_NUM = 1
          THEN 1 
          ELSE 0
         END) AS FOR_CDSAT_ITOGO       
   
   FROM ALL_RATINGS RT
   RIGHT JOIN FORMAT FT 
    ON FT.TYPE_NAME_LEVEL_1 = RT.TYPE_NAME_LEVEL_1
   AND FT.TYPE_NAME_LEVEL_2 = RT.TYPE_NAME_LEVEL_2
   AND FT.CLASS_TYPE = RT.CLASS_TYPE

  GROUP BY ROLLUP(FT.TYPE_NAME_LEVEL_1, FT.TYPE_NAME_LEVEL_2, FT.CLASS_TYPE)--, ROLLUP(ST.PERIOD)--ST.PERIOD,ROLLUP(TTP.NAME)
  ORDER BY GROUPING(FT.TYPE_NAME_LEVEL_1),ORD, FT.CLASS_TYPE, ID_TYPE_LEVEL_1, ID_TYPE_LEVEL_2
 )
 SELECT 
   TYPE_NAME_LEVEL_1 -- Классификатор
 , TYPE_NAME_LEVEL_2
 , CLASS_TYPE
 , (CASE WHEN EMAIL_LINE_1 = 0 THEN 'нет оценок' ELSE TO_CHAR(EMAIL_LINE_1) END) AS EMAIL_LINE_1 -- 1-я линия поддержки (НК). Кол-во оцененных обращений по тематике  
 , COALESCE(REPLACE( REPLACE(TRIM(TO_CHAR(AVG_MARK_LINE_1,'990D9')),'.',',') ,',0',''),'нет оценок') AS AVG_MARK_LINE_1 -- 1-я линия поддержки (НК). Средний балл
 , (CASE 
     WHEN EMAIL_LINE_1 = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CSAT_LINE_1/DECODE(EMAIL_LINE_1,0,1,EMAIL_LINE_1),0)*100,'990D9')),'.',',')||'%'
    END) AS CSAT_LINE_1 -- 1-я линия поддержки (НК). CSAT
 , (CASE 
     WHEN EMAIL_LINE_1 = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CDSAT_LINE_1/DECODE(EMAIL_LINE_1,0,1,EMAIL_LINE_1),0)*100,'990D9')),'.',',')||'%'
    END) AS CDSAT_LINE_1 -- 1-я линия поддержки (НК). CDSAT

 , (CASE WHEN EMAIL_LINE_2 = 0 THEN 'нет оценок' ELSE TO_CHAR(EMAIL_LINE_2) END) AS EMAIL_LINE_2 -- 2-я линия поддержки (Ланит). Кол-во оцененных обращений по тематике  
 , COALESCE(REPLACE( REPLACE(TRIM(TO_CHAR(AVG_MARK_LINE_2,'990D9')),'.',',') ,',0',''),'нет оценок') AS AVG_MARK_LINE_2 -- 2-я линия поддержки (Ланит). Средний балл
 , (CASE 
     WHEN EMAIL_LINE_2 = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CSAT_LINE_2/DECODE(EMAIL_LINE_2,0,1,EMAIL_LINE_2),0)*100,'990D9')),'.',',')||'%'
    END) AS CSAT_LINE_2 -- 2-я линия поддержки (Ланит). CSAT
 , (CASE 
     WHEN EMAIL_LINE_2 = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CDSAT_LINE_2/DECODE(EMAIL_LINE_2,0,1,EMAIL_LINE_2),0)*100,'990D9')),'.',',')||'%'
    END) AS CDSAT_LINE_2 -- 2-я линия поддержки (Ланит). CDSAT
    
    
 , (CASE WHEN EMAIL_ITOGO = 0 THEN 'нет оценок' ELSE TO_CHAR(EMAIL_ITOGO) END) AS EMAIL_ITOGO -- Кол-во оцененных обращений по тематике, итого
 , COALESCE(REPLACE( REPLACE(TRIM(TO_CHAR(AVG_MARK_ITOGO,'990D9')),'.',',') ,',0',''),'нет оценок') AS AVG_MARK_ITOGO -- Средний балл, итого
 , (CASE 
     WHEN EMAIL_ITOGO = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CSAT_ITOGO/DECODE(EMAIL_ITOGO,0,1,EMAIL_ITOGO),0)*100,'990D9')),'.',',')||'%'
    END) AS CSAT_ITOGO -- CSAT, итого
 , (CASE 
     WHEN EMAIL_ITOGO = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CDSAT_ITOGO/DECODE(EMAIL_ITOGO,0,1,EMAIL_ITOGO),0)*100,'990D9')),'.',',')||'%'
    END) AS CDSAT_ITOGO -- CDSAT, итого    

 FROM RATING_STATISTICA
 WHERE     (TYPE_NAME_LEVEL_1 is not null AND TYPE_NAME_LEVEL_2 is not null AND CLASS_TYPE is not null) 
        OR TYPE_NAME_LEVEL_1 = 'Всего' --Убираем промежуточные суммы
 ;

TYPE t_acsi_mail_statistic IS TABLE OF cur_acsi_mail_statistic%rowtype;

FUNCTION fnc_acsi_mail_statistic
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

) RETURN t_acsi_mail_statistic pipelined;



----------------------------------------------------------------------------------
--      Статистика по результатам опроса на удовлетворенность обработки E-mail в разрезе операторов              --
----------------------------------------------------------------------------------


CURSOR cur_acsi_mail_statoper (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_LOGIN VARCHAR
      , I_MARK NUMBER
  )
IS
WITH 
  GIS_ZHKH AS (SELECT * FROM DUAL),
  ALL_TICKETS AS ( --Общая выборка оцененных обращений
    SELECT 
 TCK.ID_TICKET
FROM QUALITY_RATING QR
JOIN TICKETS TCK
 ON TCK.ID_TICKET = QR.FID_TICKET
WHERE (QR.CREATED_AT >= I_INIT_TIME AND QR.CREATED_AT < I_FINISH_TIME) --Фильтр по дате опроса:
   AND TCK.IS_ACTIVE = 1

),
 ALL_OPERATORS AS ( --Тут нужны только оператора, работающие с письмами (скоро создадут такой список в схеме)
    SELECT DISTINCT 
    US.LOGIN
    FROM CIS.NC_USERS US --ОПЕРАТОРЫ
    WHERE LOWER(replace(US.LOGIN,'_1','')) LIKE '%_gis_zhkh_vol'  
),

 ALL_OPERATORS_TICKETS AS (
   SELECT 
     TCT.ID_TICKET
   , MAX(US.LOGIN) KEEP (DENSE_RANK LAST ORDER BY ACL.ID_ACTION) AS LOGIN
   FROM ALL_TICKETS TCT
   JOIN USER_ACTIONS_LOG ACL
    ON ACL.LOGGABLE_ID = TCT.ID_TICKET AND ACL.LOGGABLE_TYPE = 'TICKETS'
   JOIN CIS.NC_USERS US 
    ON US.ID_USER = ACL.FID_USER
    GROUP BY TCT.ID_TICKET  
),

 ALL_RATINGS AS (
   SELECT 
     TCK.ID_TICKET
   , QR.ID_RATING
  -- , (CASE WHEN TTS.FID_TICKET IS NULL THEN '1-я линия поддержки' ELSE '2-я линия поддержки' END) AS EMAIL_LINE --Линия: 1-я линия поддержки (НК) 2-я линия поддержки (Ланит)
   , MRK.MARK_NUM
   , OPT.LOGIN

  FROM ALL_TICKETS TCK
  JOIN TICKETS TC
   ON TC.ID_TICKET = TCK.ID_TICKET
  JOIN QUALITY_RATING QR
   ON QR.FID_TICKET = TCK.ID_TICKET
  LEFT JOIN Q_RATING_D_MARKS MRK
   ON MRK.ID_MARK = QR.FID_MARK
  LEFT JOIN ALL_OPERATORS_TICKETS OPT
   ON OPT.ID_TICKET = TCK.ID_TICKET
  LEFT JOIN (SELECT DISTINCT FID_TICKET
              FROM TICKETS_TASKS TTS
              JOIN ALL_TICKETS TCK
               ON TTS.FID_TICKET = TCK.ID_TICKET) TTS
   ON TTS.FID_TICKET = TCK.ID_TICKET
   
  WHERE TTS.FID_TICKET IS NULL -- Только 1-я линия поддержки
   AND (NVL(TC.FID_COMPANY_REGION,85) = I_REGION OR I_REGION IS NULL)
   AND (MRK.MARK_NUM = I_MARK OR I_MARK IS NULL)
 ),
 RATING_STATISTICA AS (
 SELECT 
   DECODE(GROUPING(OPR.LOGIN)
                  ,0,OPR.LOGIN,'Всего') AS LOGIN
 , COUNT(ID_RATING) AS COUNT_RATING
 , AVG(MARK_NUM) AS AVG_MARK
 , SUM(CASE
        WHEN MARK_NUM IN (4,5)
        THEN 1
        ELSE 0
       END) AS FOR_CSAT                 
 , SUM(CASE
        WHEN MARK_NUM = 1
        THEN 1
        ELSE 0
       END) AS FOR_CDSAT                  
 FROM ALL_RATINGS RT
 RIGHT JOIN ALL_OPERATORS OPR
  ON OPR.LOGIN = RT.LOGIN
 WHERE (OPR.LOGIN = I_LOGIN OR I_LOGIN IS NULL) 
 GROUP BY ROLLUP(OPR.LOGIN)
 )
 SELECT 
   LOGIN --ФИО оператора
 , (CASE WHEN COUNT_RATING = 0 THEN 'нет оценок' ELSE TO_CHAR(COUNT_RATING) END) AS COUNT_RATING -- Кол-во оцененных обращений по тематике
 , COALESCE(REPLACE( REPLACE(TRIM(TO_CHAR(AVG_MARK,'990D9')),'.',',') ,',0',''),'нет оценок') AS AVG_MARK -- Средний балл
 , (CASE 
     WHEN COUNT_RATING = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CSAT/DECODE(COUNT_RATING,0,1,COUNT_RATING),0)*100,'990D9')),'.',',')||'%'
    END) AS CSAT -- CSAT
 , (CASE 
     WHEN COUNT_RATING = 0
     THEN 'нет оценок'
     ELSE REPLACE(TRIM(TO_CHAR(NVL(FOR_CDSAT/DECODE(COUNT_RATING,0,1,COUNT_RATING),0)*100,'990D9')),'.',',')||'%'
    END) AS CDSAT -- CDSAT 
 FROM RATING_STATISTICA
 ORDER BY LOGIN ASC
 ;

TYPE t_acsi_mail_statoper IS TABLE OF cur_acsi_mail_statoper%rowtype;

FUNCTION fnc_acsi_mail_statoper
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_LOGIN VARCHAR
      , I_MARK NUMBER

) RETURN t_acsi_mail_statoper pipelined;

END PKG_ACSI_MAIL_REPORTS;
/


CREATE OR REPLACE PACKAGE BODY PKG_ACSI_MAIL_REPORTS AS
--                                                                              --
-- Отчетность для оценки удовлетворенности обработки обращений на канале E-mail --
-- Заявки ZHKKH-714 и ZHKKH-718                                                 --                                                                            --
--
----------------------------------------------------------------------------------
--      Детализированный отчет по оценке удовлетворенности обработки E-mail     --
----------------------------------------------------------------------------------
  FUNCTION fnc_acsi_mail_log
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_EMAIL_LINE NUMBER

) RETURN t_acsi_mail_log pipelined AS
 BEGIN
   FOR L IN cur_acsi_mail_log(I_INIT_TIME, I_FINISH_TIME, I_EMAIL_LINE)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_acsi_mail_log;
  
  
----------------------------------------------------------------------------------
--       Сводный отчет по оценке удовлетворенности обработки E-mail             --
----------------------------------------------------------------------------------
  FUNCTION fnc_acsi_mail_general
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_TYPE_TIME_FILTER NUMBER
      , I_GROUP VARCHAR
      , I_REGION NUMBER

) RETURN t_acsi_mail_general pipelined AS
 BEGIN
   FOR L IN cur_acsi_mail_general(I_INIT_TIME, I_FINISH_TIME, I_TYPE_TIME_FILTER, I_GROUP, I_REGION)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_acsi_mail_general;
  
  
----------------------------------------------------------------------------------
--       Статистика по результатам опроса на удовлетворенность обработки E-mail в разрезе тематик             --
----------------------------------------------------------------------------------
  FUNCTION fnc_acsi_mail_statistic
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_ADMIN_TYPE VARCHAR2 := NULL --Административный тип

) RETURN t_acsi_mail_statistic pipelined AS
 BEGIN
   FOR L IN cur_acsi_mail_statistic(I_INIT_TIME, I_FINISH_TIME, I_REGION)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_acsi_mail_statistic;  
  
  
  
----------------------------------------------------------------------------------
--       Статистика по результатам опроса на удовлетворенность обработки E-mail в разрезе операторов             --
----------------------------------------------------------------------------------
  FUNCTION fnc_acsi_mail_statoper
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_REGION NUMBER
      , I_LOGIN VARCHAR
      , I_MARK NUMBER

) RETURN t_acsi_mail_statoper pipelined AS
 BEGIN
   FOR L IN cur_acsi_mail_statoper(I_INIT_TIME, I_FINISH_TIME, I_REGION, I_LOGIN, I_MARK)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_acsi_mail_statoper;    

END PKG_ACSI_MAIL_REPORTS;
/
