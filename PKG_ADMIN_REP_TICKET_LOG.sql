CREATE OR REPLACE PACKAGE PKG_ADMIN_REP_TICKET_LOG AS



--Этот пакет предназначен для отчетности,
--которая выгружается в редактор.
--ЗАЯВКА ZHKKH-761

-----------------------------------------------------------------
--           ЛОГ ОБРАЩЕНИЙ (ВЫГРУЖАЕТСЯ В АДМИНКУ)             --
-----------------------------------------------------------------

CURSOR cur_ticket_log (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS COMMON.T_NUM_ARRAY --СТАТУС ОБРАЩЕНИЯ
      , I_METKA COMMON.T_NUM_ARRAY -- МЕТКИ
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
 WHERE
      (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
  AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
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
      AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
      AND ACT.CODE IN ('open','assign') -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"

      AND US.LOGIN NOT IN ('i.a.strapko_gis_zhkh_Vol', 'v.v.iliykhin_gis_zhkh_Vol','t.aitkaliev') -- ДЛЯ ЗАЯВКИ ZHKKH-473
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
 , (CASE
    WHEN FLOOR(ALL_TIME/3600) < 10
     THEN '0' || TO_CHAR(FLOOR(ALL_TIME/3600))
     ELSE TO_CHAR(FLOOR(ALL_TIME/3600))
    END) ||':'||
  (CASE
    WHEN FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60) < 10
     THEN '0' || TO_CHAR(FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60))
     ELSE TO_CHAR(FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60))
    END) ||':'||
  (CASE
  WHEN (ALL_TIME - FLOOR(ALL_TIME/3600)*3600 - FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60)*60) < 10
   THEN '0' || TO_CHAR(FLOOR(ALL_TIME - FLOOR(ALL_TIME/3600)*3600 - FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60)*60))
   ELSE TO_CHAR(FLOOR(ALL_TIME - FLOOR(ALL_TIME/3600)*3600 - FLOOR((ALL_TIME - FLOOR(ALL_TIME/3600)*3600)/60)*60))
  END)
 as PROCESSING_TIME
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
    AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
    AND ADR.FID_ADDRESS_TYPE = 1
  )
, TICKETS_METKS AS --МЕТКИ ДЛЯ ОБРАЩЕНИЙ
 ( SELECT
     TCK.ID_TICKET AS ID_TICKET
  ,  LISTAGG(TDT.NAME,', ') WITHIN GROUP(ORDER BY TTG.FID_TAG) AS METKA
  ,  LISTAGG(TDT.ID_TAG,', ') WITHIN GROUP(ORDER BY TTG.FID_TAG) AS METKA_ID
  FROM
  TICKETS TCK
  JOIN TICKETS_HAS_TAGS TTG
   ON TTG.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TAGS TDT
   ON TDT.ID_TAG = TTG.FID_TAG
  WHERE
        (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
    AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
  GROUP BY TCK.ID_TICKET
  )
, TICKETS_METKS_FILTR AS --ФИЛЬТР ПО МЕТКАМ
  (SELECT DISTINCT
   ID_TICKET
  FROM TICKETS_METKS TMT
  JOIN (SELECT COLUMN_VALUE AS VAL  FROM TABLE(I_METKA)) MET
   ON TO_CHAR(TMT.METKA_ID) LIKE '%'||TO_CHAR(MET.VAL) || '%'
 )
, ALL_TICKETS_TYPES AS --КлассификаторЫ
  (SELECT
    TCK.ID_TICKET AS ID_TICKET
  , LISTAGG(TDT.NAME,', ') WITHIN GROUP(ORDER BY TTP.ID_HAS) AS CLASSIFIER
  FROM
  TICKETS TCK
  JOIN TICKETS_HAS_TYPES TTP
   ON TTP.FID_TICKET = TCK.ID_TICKET
  JOIN TICKETS_D_TYPES TDT
   ON TDT.ID_TYPE = TTP.FID_TYPE
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
   AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
  GROUP BY TCK.ID_TICKET
  )
--, ALL_TICKETS_TASKS AS --НОМЕРА ЗАЯВОК В JIRA
--  (SELECT
--  TCK.ID_TICKET AS FID_TICKET
--  , LISTAGG(TTS.TASK_CODE,', ') WITHIN GROUP(ORDER BY TTS.ID_TASK) AS TASK_JIRA
--  FROM
--  TICKETS TCK
--  JOIN TICKETS_TASKS TTS
--   ON TTS.FID_TICKET = TCK.ID_TICKET
--  WHERE
--       (TCK.CREATED_AT >= I_INIT_TIME AND TCK.CREATED_AT < I_FINISH_TIME)
--      --МНОЖЕСТВЕННЫЙ ВЫБОР
----      AND (I_STATUS like '% '|| TCK.FID_STATUS ||' %' OR nvl(I_STATUS,'1') = '1')--Статус обращения
--      --ДЛЯ ЕИС
--        AND (I_STATUS = TCK.FID_STATUS OR I_STATUS IS NULL)--Статус обращения
--  GROUP BY TCK.ID_TICKET
--  )
, ALL_TICKETS_TASKS AS--достает список задач в JIRA контакта
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
           AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения      
       )
       WHERE seq <= 10
       GROUP BY FID_TICKET
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
  WHERE
       (nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) >= I_INIT_TIME AND nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT) < I_FINISH_TIME)
   AND (TCK.FID_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)--Статус обращения
  GROUP BY TCK.ID_TICKET
  )
, ALL_TICKETS AS
  (SELECT
      TCK.ID_TICKET                                    AS ID_TICKET --№ обращения
    , TO_CHAR(nvl(TCK.REGISTERED_AT, TCK.UPDATED_AT),'dd.mm.yyyy hh24:mi')     AS CREATED_AT --Дата и время создания
    , ADR.MAIL_ADDRESS                                 AS MAIL_ADDRESS --E-mail
    , MSG.SUBJECT                                      AS SUBJECT --Тема
    , RTP.NAME                                         AS REQUESTER_NAME --Заявитель
    , SUBSTR(TRIM(TRIM(chr(13) FROM trim(chr(10) from REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(MSG.BODY,'<br>',chr(10)),'<style>.*</style>','',1, 0, 'nm'),'(\<(/?[^>]+)>)','')))),1,32766) AS MESSAGE_TEXT --Текст письма
    , TCK.PRIORITY                                     AS PRIORITY --Приоритет
    , TTP.CLASSIFIER                                   AS CLASSIFIER --Классификатор--
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
    , TCK.COMPANY_OGRN                                 AS COMPANY_OGRN -- OGRN


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
  LEFT JOIN TICKETS_D_STATUSES TST --ТИПЫ ОБРАЩЕНИЯ
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
  LEFT JOIN TICKETS_METKS_FILTR TMF--ФИЛЬТР ПО МЕТКАМ
   ON TMF.ID_TICKET = TCK.ID_TICKET
  WHERE
   NVL2(I_METKA,TMF.ID_TICKET,TCK.ID_TICKET) = TCK.ID_TICKET -- ФИЛЬТР ПО МЕТКАМ (ПОКА НЕ ПРИДУМАЮ ДЕЛАТЬ КОЛЛЕКЦИЮ ИЗ ВЫБОРКИ ВСЕХ МЕТОК)
                                                             -- ЕСЛИ МЕТКА NOT NULL, ТО ИДЕТ ПРЯМОЕ СРАВНЕНИЕ
                                                             -- TICKETS_METKS_FILTR.ID_TICKET И TICKETS.ID_TICKET
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
  , COMPANY_OGRN -- OGRN
  FROM ALL_TICKETS
  ;

  TYPE t_ticket_log IS TABLE OF cur_ticket_log%rowtype;

  FUNCTION fnc_ticket_log
  (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS COMMON.T_NUM_ARRAY --СТАТУС ОБРАЩЕНИЯ
      , I_METKA COMMON.T_NUM_ARRAY -- МЕТКИ
  ) RETURN t_ticket_log pipelined;


END PKG_ADMIN_REP_TICKET_LOG;
/


CREATE OR REPLACE PACKAGE BODY PKG_ADMIN_REP_TICKET_LOG AS


--Этот пакет предназначен для отчетности,
--которая выгружается в редактор.
--ЗАЯВКА ZHKKH-761
--ЗНАЮ, ЧТО ЭТО ИДИОТИЗМ, ДЕЛАТЬ ПАКЕТ ДЛЯ КАЖДОГО ОТЧЕТА, НО МАКСИМ УПЕРСЯ!!!
-----------------------------------------------------------------
--            ЛОГ ОБРАЩЕНИЙ (ВЫГРУЖАЕТСЯ В АДМИНКУ)            --
-----------------------------------------------------------------

    FUNCTION fnc_ticket_log
(
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS COMMON.T_NUM_ARRAY --СТАТУС ОБРАЩЕНИЯ
      , I_METKA COMMON.T_NUM_ARRAY -- МЕТКИ
) RETURN t_ticket_log pipelined AS
  BEGIN
   FOR L IN cur_ticket_log(I_INIT_TIME, I_FINISH_TIME, I_STATUS, I_METKA)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_ticket_log;


END PKG_ADMIN_REP_TICKET_LOG;
/
