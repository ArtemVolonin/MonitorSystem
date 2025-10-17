prompt PL/SQL Developer Export User Objects for user RETAILGROUP@UNIC
prompt Created by SBU on 17 Октябрь 2025 г.
set define off
spool user_objects.log

prompt
prompt Creating sequence COMPETITORS_SEQ
prompt =================================
prompt
create sequence COMPETITORS_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 41
increment by 1
nocache;

prompt
prompt Creating sequence PRODUCTS_SEQ
prompt ==============================
prompt
create sequence PRODUCTS_SEQ
minvalue 1
maxvalue 9999999999999999999999999999
start with 41
increment by 1
nocache;

prompt
prompt Creating type T_PRICE_ROW
prompt =========================
prompt
CREATE OR REPLACE TYPE t_price_row AS OBJECT (
  product_id      NUMBER,
  product_name    VARCHAR2(200),
  competitor_id   NUMBER,
  competitor_name VARCHAR2(200),
  price           NUMBER(10,2)
);
/

prompt
prompt Creating type T_PRICE_TAB
prompt =========================
prompt
CREATE OR REPLACE TYPE t_price_tab AS TABLE OF t_price_row;
/

prompt
prompt Creating type T_PRICE_REC_OBJ
prompt =============================
prompt
CREATE OR REPLACE TYPE t_price_rec_obj AS OBJECT (
  product_id     NUMBER,
  competitor_id  NUMBER,
  monitor_date   DATE,
  price          NUMBER
);
/

prompt
prompt Creating type T_PRICE_TAB_OBJ
prompt =============================
prompt
CREATE OR REPLACE TYPE t_price_tab_obj AS TABLE OF t_price_rec_obj;
/

prompt
prompt Creating package PRICE_MONITORING
prompt =================================
prompt
CREATE OR REPLACE PACKAGE PRICE_MONITORING AS
  v_session_id NUMBER := SYS_CONTEXT('USERENV','SESSIONID');
  -- Процедура выборки цен
FUNCTION get_prices_pipe(p_date IN DATE) RETURN t_price_tab PIPELINED; 

  -- Процедура сохранения/обновления цен
PROCEDURE save_prices( p_prices IN t_price_tab_obj);
  
FUNCTION get_prev_price(p_product_id IN number, p_competitor_id IN NUMBER, p_monitor_date IN DATE) RETURN NUMBER;
      
FUNCTION get_prices_cursor(p_date IN DATE) RETURN SYS_REFCURSOR;  

END PRICE_MONITORING;
/

prompt
prompt Creating package body PRICE_MONITORING
prompt ======================================
prompt
CREATE OR REPLACE PACKAGE BODY PRICE_MONITORING AS

  -- 1. Процедура выборки цен
FUNCTION get_prices_pipe(
  p_date IN DATE
) RETURN t_price_tab PIPELINED IS 
BEGIN
  
  FOR rec IN (
  SELECT p.id AS product_id, 
  p.name AS product_name, 
  c.id AS competitor_id, 
  c.name AS competitor_name, 
  pr.price
  FROM products p 
  LEFT JOIN GTT_REPORT_FILTER g 
  on (g.session_id = v_session_id AND g.report_name = 'Mainquery') 
  LEFT JOIN competitors c 
  ON (c.id=g.param_num AND c.status = 1) 
  LEFT JOIN prices pr 
  ON (pr.product_id = p.id AND pr.competitor_id = c.id AND pr.monitor_date = p_date) 
  -- CROSS JOIN competitors c 
  WHERE p.status = 1 AND g.param_num is NOT NULL 
  ORDER BY c.name
  ) LOOP
    PIPE ROW(t_price_row(
      rec.product_id,
      rec.product_name,
      rec.competitor_id,
      rec.competitor_name,
      rec.price
    ));
  END LOOP;

  RETURN;
END get_prices_pipe;

  -- 2. Процедура сохранения/обновления цен
  PROCEDURE save_prices(p_prices IN t_price_tab_obj) IS
  BEGIN
    FOR i IN 1 .. p_prices.COUNT LOOP
      IF p_prices(i).price IS NOT NULL THEN
        -- Обновляем, если запись существует, иначе вставляем
        MERGE INTO PRICES pr
        USING (SELECT p_prices(i).product_id AS product_id,
                      p_prices(i).competitor_id AS competitor_id,
                      TRUNC(p_prices(i).monitor_date) AS monitor_date,
                      p_prices(i).price AS price
               FROM dual) src
        ON (pr.product_id = src.product_id
            AND pr.competitor_id = src.competitor_id
            AND pr.monitor_date = src.monitor_date)
        WHEN MATCHED THEN
          UPDATE SET pr.price = src.price
        WHEN NOT MATCHED THEN
          INSERT (product_id, competitor_id, monitor_date, price)
          VALUES (src.product_id, src.competitor_id, src.monitor_date, src.price);
      END IF;
    END LOOP;

    COMMIT;  -- сохраняем все изменения
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;  -- откат при ошибке
      RAISE;
  END save_prices;

---Получение предыдущей цены по конкуренту ------------------
FUNCTION get_prev_price(
  p_product_id    IN NUMBER,
  p_competitor_id IN NUMBER,
  p_monitor_date  IN DATE
) RETURN NUMBER
IS
  prev_price NUMBER := 0;
BEGIN
  BEGIN
    SELECT sp.price
      INTO prev_price
      FROM prices sp
     WHERE sp.monitor_date = p_monitor_date - 1
       AND sp.product_id = p_product_id
       AND sp.competitor_id = p_competitor_id;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      prev_price := 0; -- если данных нет, возвращаем 0
  END;

  RETURN prev_price;
END get_prev_price;


---Другой вариант2 -  Процедура сохранения/обновления цен------------------------------------------------------------
FUNCTION get_prices_cursor(p_date IN DATE) RETURN SYS_REFCURSOR IS
  v_session_id NUMBER := SYS_CONTEXT('USERENV','SESSIONID');
  v_cols       VARCHAR2(4000);
  v_sql        CLOB;
  v_rc         SYS_REFCURSOR;
BEGIN
  -- собираем список конкурентов
  SELECT LISTAGG('''' || g.param_num || ''' AS price_comp_' || g.param_num, ',') 
         WITHIN GROUP (ORDER BY g.param_num)
  INTO v_cols
  FROM GTT_REPORT_FILTER g
  WHERE g.session_id = v_session_id
    AND g.report_name = 'Mainquery';

  v_sql := '
    SELECT *
    FROM (
      SELECT p.id AS product_id,
             p.name AS product_name,
             c.id AS competitor_id,
             pr.price
      FROM products p
      LEFT JOIN GTT_REPORT_FILTER g 
             ON g.session_id = ' || v_session_id || '
            AND g.report_name = ''Mainquery''
      LEFT JOIN competitors c 
             ON c.id = g.param_num
            AND c.status = 1
      LEFT JOIN prices pr
             ON pr.product_id = p.id
            AND pr.competitor_id = c.id
            AND pr.monitor_date = :p_date
      WHERE p.status = 1
        AND g.param_num IS NOT NULL
    ) src
    PIVOT (
      MAX(price) FOR competitor_id IN (' || v_cols || ')
    )
    ORDER BY product_name';

  OPEN v_rc FOR v_sql USING p_date;
  RETURN v_rc;
END get_prices_cursor;  

END PRICE_MONITORING;
/

prompt
prompt Creating trigger COMPETITORS_BI
prompt ===============================
prompt
CREATE OR REPLACE TRIGGER COMPETITORS_BI
BEFORE INSERT ON COMPETITORS
FOR EACH ROW
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT COMPETITORS_SEQ.NEXTVAL INTO :NEW.ID FROM dual;
    END IF;
END;
/

prompt
prompt Creating trigger PRODUCTS_BI
prompt ============================
prompt
CREATE OR REPLACE TRIGGER PRODUCTS_BI
BEFORE INSERT ON PRODUCTS
FOR EACH ROW
BEGIN
    IF :NEW.ID IS NULL THEN
        SELECT PRODUCTS_SEQ.NEXTVAL INTO :NEW.ID FROM dual;
    END IF;
END;
/

prompt
prompt Creating trigger TRG_FILL_SESSION_ID
prompt ====================================
prompt
CREATE OR REPLACE TRIGGER trg_fill_session_id
BEFORE INSERT ON GTT_REPORT_FILTER
FOR EACH ROW
BEGIN
  -- если поле session_id не передано явно, заполняем его автоматически
  IF :NEW.session_id IS NULL THEN
    :NEW.session_id := SYS_CONTEXT('USERENV', 'SESSIONID');
  END IF;
END;
/


prompt Done
spool off
set define on
