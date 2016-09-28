CREATE OR REPLACE PACKAGE PKG_ADMIN_REP_MAIL_LOG AS


--Этот пакет предназначен для отчетности,
--которая выгружается в редактор.
--ЗАЯВКА ZHKKH-761
--
----------------------------------------------------------
--       ЛОГ ПИСЕМ  (ВЫГРУЖАЕТСЯ В АДМИНКУ)             --
----------------------------------------------------------

CURSOR cur_mail_log (
        I_INIT_TIME TIMESTAMP
      , I_FINISH_TIME TIMESTAMP
      , I_STATUS COMMON.T_NUM_ARRAY-- СТАТУС ПИСЬМА
      , I_LOGIN COMMON.T_NUM_ARRAY -- ОПЕРАТОР
      , I_DIRECTION VARCHAR2 -- НАПРАВЛЕНИЕ
      , I_TYPE COMMON.T_NUM_ARRAY --ТИП ПИСЬМА
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
  AND (ACT.CODE IN ('open','assign')) -- МЫ ВЫБИРАЕМ ТОЛЬКО ДЕЙСТВИЯ "ОТКРЫЛ" И "ПРИВЯЗАЛ"
  AND (MST.ID_MSG_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)
  AND (MTP.DIRECTION = lower(I_DIRECTION) OR I_DIRECTION IS NULL) -- НАПРАВЛЕНИЕ
  AND (MTP.ID_MSG_TYPE IN (select * from table(I_TYPE)) OR I_TYPE IS NULL)

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
LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
 ON US.ID_USER = MSG.FID_USER
WHERE
      (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
  AND (MST.ID_MSG_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)
  AND (MTP.DIRECTION = lower(I_DIRECTION) OR I_DIRECTION IS NULL) -- НАПРАВЛЕНИЕ
  AND (MTP.ID_MSG_TYPE IN (select * from table(I_TYPE)) OR I_TYPE IS NULL)
  AND ADR.FID_ADDRESS_TYPE = 1 -- ЗНАЧИТ ТОЛЬКО ОТПРАВИТЕЛЬ
  )
, LOGIN_OPERATORS AS
  (SELECT
     CLG.FID_MESSAGE AS FID_MESSAGE
   , MAX(US.ID_USER) KEEP (DENSE_RANK LAST ORDER BY CLG.ID_CHANGE_LOG) AS FID_USER
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
     AND ACT.CODE in ('assign','unbind') -- МЫ ВЫБИРАЕМ ТОЛЬКО "ПРИВЯЗАЛ"
     AND (MST.ID_MSG_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)
     AND (US.ID_USER IN (SELECT * FROM TABLE(I_LOGIN)) OR I_LOGIN IS NULL)
     AND (MTP.DIRECTION = lower(I_DIRECTION) OR I_DIRECTION IS NULL) -- НАПРАВЛЕНИЕ
     AND (MTP.ID_MSG_TYPE IN (select * from table(I_TYPE)) OR I_TYPE IS NULL)
  GROUP BY CLG.FID_MESSAGE
  )
, MESSAGES AS
  (SELECT
      MSG.ID_MESSAGE                                   AS ID_MESSAGE
    , TO_CHAR(MSG.CREATED_AT,'dd.mm.yyyy hh24:mi')     AS RECEIVING_TIME
    , ADR.MAIL_ADDRESS                                 AS MAIL_ADDRESS
    , MSG.SUBJECT                                      AS SUBJECT
    , RTP.NAME                                         AS REQUESTER_NAME
    , SUBSTR(TRIM(TRIM(chr(13) FROM trim(chr(10) from REGEXP_REPLACE(REGEXP_REPLACE(REPLACE(MSG.BODY,'<br>',chr(10)),'<style>.*</style>','',1, 0, 'nm'),'(\<(/?[^>]+)>)','')))),1,32766)  AS MESSAGE_TEXT
    , MTP.NAME                                         AS TYPE_LETTER
    , US.LOGIN                                         AS OPERATOR_LOGIN
    , FT.PROCESSING_TIME                               AS PROCESSING_TIME
    , MST.NAME                                         AS STATUS_NAME
    , TO_CHAR(MSG.RECEIVING_TIME,'dd.mm.yyyy hh24:mi') AS SUPPORT_TIME
    , MSG.FID_TICKET                                   AS ID_TICKET
    , (CASE
       WHEN MSG.FID_MSG_STATUS = 3
       THEN TO_CHAR(MSG.PROCESSING_TIME,'dd.mm.yyyy hh24:mi')
       ELSE ''
       END)                                            AS TICKET_TIME --Дата обработки письма (не пишется у исходящих писем)
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
  LEFT JOIN CIS.NC_USERS US --ОПЕРАТОРЫ
   ON US.ID_USER = LOP.FID_USER
  LEFT JOIN FINAL_TIME FT --ОПРЕДЕЛЯЕТ ВРЕМЯ ОБРАБОТКИ ПИСЬМА
   ON FT.FID_MESSAGE = MSG.ID_MESSAGE
  WHERE
        (MSG.CREATED_AT >= I_INIT_TIME AND MSG.CREATED_AT < I_FINISH_TIME)
    AND (MST.ID_MSG_STATUS IN (select * from table(I_STATUS)) OR I_STATUS IS NULL)
    AND (US.ID_USER IN (SELECT * FROM TABLE(I_LOGIN)) OR I_LOGIN IS NULL)
    AND (MTP.DIRECTION = lower(I_DIRECTION) OR I_DIRECTION IS NULL) -- НАПРАВЛЕНИЕ
    AND (MTP.ID_MSG_TYPE IN (select * from table(I_TYPE)) OR I_TYPE IS NULL)

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
  , TICKET_TIME -- Дата обработки письма
FROM MESSAGES
 ORDER BY ID_MESSAGE;

TYPE t_mail_log IS TABLE OF cur_mail_log%rowtype;

FUNCTION fnc_mail_log
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_STATUS COMMON.T_NUM_ARRAY :=NULL -- СТАТУС ПИСЬМА
    , I_LOGIN COMMON.T_NUM_ARRAY :=NULL -- ОПЕРАТОР
    , I_DIRECTION VARCHAR2 :=NULL-- НАПРАВЛЕНИЕ
    , I_TYPE COMMON.T_NUM_ARRAY :=NULL --ТИП ПИСЬМА
) RETURN t_mail_log pipelined;

END PKG_ADMIN_REP_MAIL_LOG;
/


CREATE OR REPLACE PACKAGE BODY PKG_ADMIN_REP_MAIL_LOG AS


--Этот пакет предназначен для отчетности,
--которая выгружается в редактор.
--ЗАЯВКА ZHKKH-761
--ЗНАЮ, ЧТО ЭТО ИДИОТИЗМ, ДЕЛАТЬ ПАКЕТ ДЛЯ КАЖДОГО ОТЧЕТА, НО МАКСИМ УПЕРСЯ!!!
----------------------------------------------------------
--         ЛОГ ПИСЕМ (ВЫГРУЖАЕТСЯ В АДМИНКУ)            --
----------------------------------------------------------

  FUNCTION fnc_mail_log
(
      I_INIT_TIME TIMESTAMP
    , I_FINISH_TIME TIMESTAMP
    , I_STATUS COMMON.T_NUM_ARRAY :=NULL -- СТАТУС ПИСЬМА
    , I_LOGIN COMMON.T_NUM_ARRAY  :=NULL-- ОПЕРАТОР
    , I_DIRECTION VARCHAR2 :=NULL-- НАПРАВЛЕНИЕ
    , I_TYPE COMMON.T_NUM_ARRAY :=NULL --ТИП ПИСЬМА
) RETURN t_mail_log pipelined AS
   BEGIN
   FOR L IN cur_mail_log(I_INIT_TIME, I_FINISH_TIME, I_STATUS, I_LOGIN, I_DIRECTION, I_TYPE)
    LOOP
    PIPE ROW (L);
   END LOOP;
  END fnc_mail_log;


END PKG_ADMIN_REP_MAIL_LOG;
/
