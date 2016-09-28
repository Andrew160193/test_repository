CREATE OR REPLACE PACKAGE P_QC_DATA AS

CURSOR c_get_data(
        p_session_id IN VARCHAR2,
        i_login IN VARCHAR2
) IS
WITH
gis_zhkh AS (
        SELECT * 
        FROM dual
)
, all_calls AS (
        SELECT
                cl.session_id
                , MAX(cl.id_call) AS id_call
        FROM core_calls cl
        WHERE cl.project_id = 'project245'
                AND cl.session_id = p_session_id
                AND (cl.operator_login = i_login OR i_login IS NULL)
        GROUP BY cl.session_id
)
SELECT * FROM (
        SELECT
                reg.NAME AS region_text,--1. Регион
                'Обращения' AS region_text_section,
                1 AS region_text_sectid,
                'text' AS region_text_type,
                
                ctp.NAME AS company_text,--2. Полномочия
                'Обращения' AS company_text_section,
                1 AS company_text_sectid,
                'text' AS company_text_type,
                
                res.NAME AS result_text,--3. Статус звонка
                'Звонок' AS result_text_section,
                2 AS result_text_sectid,
                'text' AS result_text_type,
                
                tdt_lev_1.NAME AS ticket_text_1,--4.Классификатор (Тип обращения)
                'Обращения' AS ticket_text_1_section,
                1 AS ticket_text_1_sectid,
                'text' AS ticket_text_1_type,
                
                tdt.NAME AS ticket_text_2,--4.Классификатор (Подтип обращения)
                'Обращения' AS ticket_text_2_section,
                1 AS ticket_text_2_sectid,
                'text' AS ticket_text_2_type,
                
                ADT.NAME AS ticket_text_3,--4.Классификатор (Административный тип)
                'Обращения' AS ticket_text_3_section,
                1 AS ticket_text_3_sectid,
                'text' AS ticket_text_3_type,
                
                cl.comments AS comment_text,--5. комментарий
                'Звонок' AS comment_text_section,
                2 AS comment_text_sectid,
                'text' AS comment_text_type,
                
                inc.company_ogrn AS ogrn_text,--6. ОГРН
                'Обращения' AS ogrn_text_section,
                1 AS ogrn_text_sectid,
                'text' AS ogrn_text_type,
                
                inc.ogrn_refuse_reason AS ogrn_refuse_text,--7. Отказ ОГРН
                'Обращения' AS ogrn_refuse_text_section,
                1 AS ogrn_refuse_text_sectid,
                'text' AS ogrn_refuse_text_type
        FROM
                all_calls acl
                JOIN core_calls cl
                    ON cl.session_id = acl.session_id
                LEFT JOIN inc_call_contact_data inc
                    ON inc.fid_call = cl.id_call
                LEFT JOIN tickets_d_regions reg
                    ON reg.id_region = inc.fid_company_region
                LEFT JOIN tickets_d_company_types ctp
                    ON ctp.id_company_type = cl.inc.fid_company_type
                LEFT JOIN core_calls_results res
                    ON res.id_result = cl.fid_result
                LEFT JOIN inc_call_questions qst
                    ON qst.fid_call=cl.id_call
                LEFT JOIN tickets_d_types tdt
                    ON tdt.id_type = qst.fid_ticket_type
                LEFT JOIN tickets_d_types tdt_lev_1
                    ON tdt_lev_1.id_type = tdt.id_parent 
                LEFT JOIN tickets_d_adm_types adt
                    ON adt.id_type = qst.fid_ticket_adm_type
)
unpivot exclude NULLS (
        (
                fieldval,
                section,
                sectid,
                TYPE
        ) FOR fieldname IN (
                (
                        region_text,
                        region_text_section,
                        region_text_sectid,
                        region_text_type
                ) AS 'Регион'
                , (
                        company_text,
                        company_text_section,
                        company_text_sectid,
                        company_text_type
                ) AS 'Полномочия'
                , (
                        result_text,
                        result_text_section,
                        result_text_sectid,
                        result_text_type
                ) AS 'Статус звонка'
                , (
                        ticket_text_1,
                        ticket_text_1_section,
                        ticket_text_1_sectid,
                        ticket_text_1_type
                ) AS 'Классификатор (Тип обращения)'
                , (
                        ticket_text_2,
                        ticket_text_2_section,
                        ticket_text_2_sectid,
                        ticket_text_2_type
                ) AS 'Классификатор (Подтип обращения)'
                , (
                        ticket_text_3,
                        ticket_text_3_section,
                        ticket_text_3_sectid,
                        ticket_text_3_type
                ) AS 'Классификатор (Административный тип)'
                , (
                        comment_text,
                        comment_text_section,
                        comment_text_sectid,
                        comment_text_type
                ) AS 'Комментарий'
                , (
                        ogrn_text,
                        ogrn_text_section,
                        ogrn_text_sectid,
                        ogrn_text_type
                ) AS 'ОГРН'
                , (
                        ogrn_refuse_text,
                        ogrn_refuse_text_section,
                        ogrn_refuse_text_sectid,
                        ogrn_refuse_text_type
                ) AS 'Отказ ОГРН'
        )
)
;


type t_get_data IS TABLE OF c_get_data%rowtype;

FUNCTION fnc_get_data(
    p_session_id VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL
                     )
  RETURN t_get_data pipelined;
  
 
 ------------------------------------------------
 --   Получение результата звонка по id сессии --
 ------------------------------------------------
  
    CURSOR c_get_call_info
(
  I_SESSION_ID IN VARCHAR2,
  I_LOGIN IN VARCHAR2 := NULL,
  I_TYPE IN NUMBER := NULL
) IS
 WITH GIS_ZHKH AS (SELECT * FROM DUAL),
 UNIC_CALLS AS (
SELECT
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    GROUP BY CL.SESSION_ID
)
  SELECT 
     CL.FID_RESULT
  ,  RES.NAME AS RESULT_TEXT
  ,  CL.OPERATOR_LOGIN
  ,  CL.CREATED_AT AS CALL_START_TIME 
  ,  CL.CLOSED_AT AS CALL_FINISH_TIME
  ,  CL.SESSION_ID AS SESSION_ID
  ,  CD.FID_TYPE AS ID_SUBJECT
  ,  TCT.NAME AS SUBJECT_NAME
  FROM UNIC_CALLS UCL
  JOIN CORE_CALLS CL
   ON CL.ID_CALL = UCL.ID_CALL
  LEFT JOIN CORE_CALLS_RESULTS RES
   ON RES.ID_RESULT = CL.FID_RESULT
  LEFT JOIN INC_CALL_CONTACT_DATA CD
   ON CD.FID_CALL = CL.ID_CALL
  LEFT JOIN TICKETS_D_TYPES TCT
   ON TCT.ID_TYPE = CD.FID_TYPE
   
  WHERE (CL.SESSION_ID = I_SESSION_ID OR I_SESSION_ID IS NULL)
    AND (CL.OPERATOR_LOGIN = I_LOGIN OR I_LOGIN IS NULL)
    AND (TCT.ID_TYPE = I_TYPE OR I_TYPE IS NULL)
   ;
   
   
   type t_get_call_info IS TABLE OF c_get_call_info%rowtype;

FUNCTION fnc_get_call_info(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
  )
  RETURN t_get_call_info pipelined;
  
  
 ------------------------------------------------
 --   Получение результата звонка по id сессии (новое название)--
 ------------------------------------------------
  
    CURSOR c_get_result
(
  I_SESSION_ID IN VARCHAR2,
  I_LOGIN IN VARCHAR2 := NULL,
  I_TYPE IN NUMBER := NULL
) IS
 WITH GIS_ZHKH AS (SELECT * FROM DUAL),
 UNIC_CALLS AS (
SELECT
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    GROUP BY CL.SESSION_ID
)
  SELECT 
     CL.FID_RESULT
  ,  RES.NAME AS RESULT_TEXT
  ,  CL.OPERATOR_LOGIN
  ,  CL.CREATED_AT AS CALL_START_TIME 
  ,  CL.CLOSED_AT AS CALL_FINISH_TIME
  ,  CL.SESSION_ID AS SESSION_ID
  ,  CD.FID_TYPE AS ID_SUBJECT
  ,  TCT.NAME AS SUBJECT_NAME
  FROM UNIC_CALLS UCL
  JOIN CORE_CALLS CL
   ON CL.ID_CALL = UCL.ID_CALL
  LEFT JOIN CORE_CALLS_RESULTS RES
   ON RES.ID_RESULT = CL.FID_RESULT
  LEFT JOIN INC_CALL_CONTACT_DATA CD
   ON CD.FID_CALL = CL.ID_CALL
  LEFT JOIN TICKETS_D_TYPES TCT
   ON TCT.ID_TYPE = CD.FID_TYPE
   
  WHERE (CL.SESSION_ID = I_SESSION_ID OR I_SESSION_ID IS NULL)
    AND (CL.OPERATOR_LOGIN = I_LOGIN OR I_LOGIN IS NULL)
    AND (TCT.ID_TYPE = I_TYPE OR I_TYPE IS NULL)
   ;
   
   
   type t_get_result IS TABLE OF c_get_result%rowtype;

FUNCTION fnc_get_result(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
  )
  RETURN t_get_result pipelined;
  
  
  -----------------------------------------------------
  --   Получение справочника результатов звонка      --     
  -----------------------------------------------------
 CURSOR c_get_results_list
  IS 
 SELECT 
   ID_RESULT AS ID_RESULT,
   NAME AS RESULT_TEXT
 FROM CORE_CALLS_RESULTS
  ORDER BY ID_RESULT;  


   type t_get_results_list IS TABLE OF c_get_results_list%rowtype;

FUNCTION fnc_get_results_list
  RETURN t_get_results_list pipelined;
  
  
  -----------------------------------------------------
  --         Список тематик звонка                   --     
  -----------------------------------------------------
 CURSOR c_get_subjects_list
  IS 
 SELECT 
   ID_TYPE AS ID_SUBJECT,
   NAME AS SUBJECT_TEXT
 FROM TICKETS_D_TYPES
  ORDER BY ID_TYPE;  


   type t_get_subjects_list IS TABLE OF c_get_subjects_list%rowtype;

FUNCTION fnc_get_subjects_list
  RETURN t_get_subjects_list pipelined;
  
  
  
 ------------------------------------------------
 --        Тематика звонка                     --
 ------------------------------------------------
  
    CURSOR c_get_subject
(
  I_SESSION_ID IN VARCHAR2,
  I_LOGIN IN VARCHAR2 := NULL,
  I_TYPE IN NUMBER := NULL
) IS
 WITH GIS_ZHKH AS (SELECT * FROM DUAL),
 UNIC_CALLS AS (
SELECT
      CL.SESSION_ID
    , MAX(CL.ID_CALL) AS ID_CALL
    FROM CORE_CALLS CL
    GROUP BY CL.SESSION_ID
)
  SELECT 
     CL.FID_RESULT
  ,  RES.NAME AS RESULT_TEXT
  ,  CL.OPERATOR_LOGIN
  ,  CL.CREATED_AT AS CALL_START_TIME 
  ,  CL.CLOSED_AT AS CALL_FINISH_TIME
  ,  CL.SESSION_ID AS SESSION_ID
  ,  CD.FID_TYPE AS ID_SUBJECT
  ,  TCT.NAME AS SUBJECT_NAME
  FROM UNIC_CALLS UCL
  JOIN CORE_CALLS CL
   ON CL.ID_CALL = UCL.ID_CALL
  LEFT JOIN CORE_CALLS_RESULTS RES
   ON RES.ID_RESULT = CL.FID_RESULT
  LEFT JOIN INC_CALL_CONTACT_DATA CD
   ON CD.FID_CALL = CL.ID_CALL
  LEFT JOIN TICKETS_D_TYPES TCT
   ON TCT.ID_TYPE = CD.FID_TYPE
   
  WHERE (CL.SESSION_ID = I_SESSION_ID OR I_SESSION_ID IS NULL)
    AND (CL.OPERATOR_LOGIN = I_LOGIN OR I_LOGIN IS NULL)
    AND (TCT.ID_TYPE = I_TYPE OR I_TYPE IS NULL)
   ;
   
   
   type t_get_subject IS TABLE OF c_get_subject%rowtype;

FUNCTION fnc_get_subject(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
  )
  RETURN t_get_subject pipelined;
  
  
  --Опрос по оценке удовлетворенности. Оценка по первому вопросу
  --------------------------------------------------
  --Оценка разговора (CSAT).
  --------------------------------------------------
  
CURSOR cur_get_call_rating
(
  i_session_id IN VARCHAR2,
  I_LOGIN IN VARCHAR2  := NULL 
) IS
   WITH GIS_ZHKH AS (SELECT * FROM DUAL)
      SELECT 
        max(CAST(cp.param_value AS VARCHAR2(50))) AS call_rating,
        cl.session_id
      FROM naucrm.call_params cp
      JOIN CORE_CALLS cl
        ON cl.session_id = cp.session_id         
      WHERE 
            (cp.session_id = i_session_id OR i_session_id IS NULL) 
        AND (cl.session_id = i_session_id OR i_session_id IS NULL)    
        AND (cl.operator_login = I_LOGIN OR I_LOGIN IS NULL)
        AND cp.param_name = 'OUT_CS2'
        AND to_number(cp.param_value)>0
      GROUP BY cl.session_id
        ;
    
TYPE t_get_call_rating IS TABLE OF cur_get_call_rating%rowtype;

FUNCTION fnc_get_call_rating
(
  i_session_id VARCHAR2,
  I_LOGIN IN VARCHAR2  := NULL 
)
RETURN t_get_call_rating pipelined;
  

END P_QC_DATA;
/


CREATE OR REPLACE PACKAGE BODY P_QC_DATA AS

FUNCTION fnc_get_data(
    p_session_id VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL )
  RETURN t_get_data pipelined
AS
BEGIN
  FOR l IN c_get_data ( p_session_id, I_LOGIN )
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_data;


 ------------------------------------------------
 --   Получение результата звонка по id сессии --
 ------------------------------------------------

FUNCTION fnc_get_call_info(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
    )
  RETURN t_get_call_info pipelined
AS
BEGIN
  FOR l IN c_get_call_info ( I_SESSION_ID, I_LOGIN, I_TYPE )
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_call_info;

 ------------------------------------------------
 --   Получение результата звонка по id сессии (новое название)--
 ------------------------------------------------

FUNCTION fnc_get_result(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
    )
  RETURN t_get_result pipelined
AS
BEGIN
  FOR l IN c_get_result ( I_SESSION_ID, I_LOGIN, I_TYPE )
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_result;


  -----------------------------------------------------
  --   Получение справочника результатов звонка      --     
  -----------------------------------------------------
  
  FUNCTION fnc_get_results_list
  RETURN t_get_results_list pipelined
AS
BEGIN
  FOR l IN c_get_results_list ()
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_results_list;

  -----------------------------------------------------
  --         Список тематик звонка                   --     
  -----------------------------------------------------
  
  FUNCTION fnc_get_subjects_list
  RETURN t_get_subjects_list pipelined
AS
BEGIN
  FOR l IN c_get_subjects_list ()
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_subjects_list;


 ------------------------------------------------
 --        Тематика звонка                     --
 ------------------------------------------------
  
FUNCTION fnc_get_subject(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL,
    I_TYPE IN NUMBER := NULL
    )
  RETURN t_get_subject pipelined
AS
BEGIN
  FOR l IN c_get_subject ( I_SESSION_ID, I_LOGIN, I_TYPE )
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_subject;


 ------------------------------------------------
 --        Оценка разговора (CSAT)             --
 ------------------------------------------------
  
FUNCTION fnc_get_call_rating(
    I_SESSION_ID IN VARCHAR2,
    I_LOGIN IN VARCHAR2 := NULL
    )
  RETURN t_get_call_rating pipelined
AS
BEGIN
  FOR l IN cur_get_call_rating( I_SESSION_ID, I_LOGIN)
  LOOP
    pipe row (l);
  END LOOP;
END fnc_get_call_rating;
  

END P_QC_DATA;
/
