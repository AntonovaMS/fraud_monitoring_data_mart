
CREATE VIEW REPORT AS

--1)ÏĞÎÑĞÎ×ÅÍÍÛÉ ÏÀÑÏÎĞÒ
SELECT TRANS_DATE AS  FRAUD_DT, PASSPORT_NUM AS PASSPORT, LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRINYMIC AS FIO , PHONE , 'ÑÎÂÅĞØÅÍÈÅ ÎÏÅĞÀÖÈÉ Ñ ÏĞÎÑĞÎ×ÅÍÍÛÌ ÏÀÏÎĞÒÎÌ' AS FRAUD_TYPE, SYSDATE AS REPORT_DT
FROM(SELECT *
           FROM FACT_TRANSACTIONS T
          JOIN DIM_CARDS C
          ON T.CARD_NUM = C.CARD_NUM
          JOIN DIM_ACCOUNTS A
          ON A.ACCOUNT_NUM = C.ACCOUNT_NUM
          JOIN DIM_CLIENTS CL
          ON CL.CLIENT_ID = A.CLIENT
          WHERE t.trans_date>= CL.PASSPORT_VALID_TO +1)
UNION ALL

--2)ÏĞÎÑĞÎ×ÅÍÍÛÉ ÄÎÃÎÂÎĞ
SELECT TRANS_DATE AS FRAUD_DT, PASSPORT_NUM AS PASSPORT , LAST_NAME ||' '|| FIRST_NAME ||' '|| PATRINYMIC AS FIO, PHONE , 'ÑÎÂÅĞØÅÍÈÅ ÎÏÅĞÀÖÈÉ Ñ ÏĞÎÑĞÎ×ÅÍÍÛÌ ÄÎÃÎÂÎĞÎÌ' AS FRAUD_TYPE, SYSDATE AS REPORT_DT
FROM (SELECT * 
               FROM FACT_TRANSACTIONS T
                 JOIN DIM_CARDS C
                    ON T.CARD_NUM = C.CARD_NUM
                 JOIN DIM_ACCOUNTS A
                    ON A.ACCOUNT_NUM = C.ACCOUNT_NUM
                 JOIN DIM_CLIENTS CL
                    ON CL.CLIENT_ID = A.CLIENT
            WHERE t.trans_date>= A.VALID_TO +1)

UNION ALL
--3) ÑÎÂÅĞØÅÍÈÅ ÎÏÅĞÀÖÈÉ Â ĞÀÇÍÛÕ ÃÎĞÎÄÀÕ Â ÒÅ×ÅÍÈÅ ÎÄÍÎÃÎ ×ÀÑÀ

select t1. td AS FRAUD_DT, c.PASSPORT_NUM AS PASSPORT,  C.LAST_NAME ||' '|| C.FIRST_NAME ||' '|| C.PATRINYMIC AS FIO, C.PHONE AS PHONE, 'ÎÏÅĞÀÖÈB Â ĞÀÇÍÛÕ ÃÎĞÎÄÀÕ Â ÒÅ×ÅÍÈÅ ÎÄÍÎÃÎ ×ÀÑÀ ' AS FRAUD_TYPE, SYSDATE AS REPORT_DT
FROM (select a.trans_date as td, a.card_num as cn, a.terminal_city as tc ,b.trans_date as trans_date, b.card_num as card_num,b.terminal_city as terminal_city1, abs( a.trans_date -b.trans_date) as r , to_date('15-10-2021 06:17:37' , 'DD.MM.YYYY HH24:MI:ss') - to_date('15-10-2021 05:17:37' , 'DD.MM.YYYY HH24:MI:ss') as w  
              from (select a.* ,te.terminal_city  from fact_transactions a  join dim_terminals te  on a.terminal= te.terminal_id) a
               join (select a.* ,te.terminal_city  from fact_transactions a   join dim_terminals te  on a.terminal= te.terminal_id)  b 
                on a.card_num = b .card_num 
          where a.trans_date <> b.trans_date and a.terminal_city <> b.terminal_city 
              and abs( a.trans_date -b.trans_date) <  to_date('15-10-2021 06:17:37' , 'DD.MM.YYYY HH24:MI:ss') - to_date('15-10-2021 05:17:37' , 'DD.MM.YYYY HH24:MI:ss'))   T1
JOIN DIM_CARDS C
ON C.CARD_NUM = T1.CARD_NUM
JOIN DIM_ACCOUNTS A
ON A.ACCOUNT_NUM = C.ACCOUNT_NUM
JOIN DIM_CLIENTS C
ON A.CLIENT = C.CLIENT_ID

UNION ALL
/* 4)Ïîïûòêà ïîäáîğà ñóìì.Â òå÷åíèå 20 ìèíóò ïğîõîäèò áîëåå 3õ îïåğàöèé ñî ñëåäóşùèì øàáëîíîì - êàæäàÿ
ïîñëåäóşùàÿ ìåíüøå ïğåäûäóùåé ïğè ıòîì îòêëîíåíû âñå êğîìå ïîñëåäíåé.Ïîñëåäíÿÿ îïåğàöèÿ (óñïåøíàÿ) â òàêîé öåïî÷êå ñ÷èòàåòñÿ ìîøåííè÷åñêîé.*/

SELECT  t6. TRANS_DATE AS FRAUD_DT, c.PASSPORT_NUM AS PASSPORT,  C.LAST_NAME ||' '|| C.FIRST_NAME ||' '|| C.PATRINYMIC AS FIO, C.PHONE AS PHONE, 'ÏÎÏÛÒÊÀ ÏÎÄÁÎĞÀ ÑÓÌÌ ' AS FRAUD_TYPE, SYSDATE  AS REPORT_DT           
FROM(SELECT T5.*
FROM(SELECT T4.* , SUM(CC) OVER (PARTITION BY RAN, CARD_NUM )   AS SU2       
FROM(SELECT T3.*,  count(AMT)  OVER (PARTITION BY  CARD_NUM,  RAN ORDER BY TRANS_DATE ROWS  BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING  ) AS CO, -- ÁÎËÅÅ ÒĞÅÕ ÎÏÅĞÀÖÈÉ 
                                  LEAD(amt) OVER (PARTITION BY card_num ORDER BY trans_date) AS LS,  
                                  CASE WHEN  Amt>COALESCE(LEAD(amt) OVER (PARTITION BY card_num ORDER BY trans_date),0) THEN 0 ELSE 1 END AS CC -- ÂÛßÂËßÅÌ ÎÏÅĞÀÖÈÈ ÍÅ ÑÎÎÒÂÅÒÑÒÂÓŞÙÈÅ ÓÑËÎÂÈŞ ÊÀÆÄÀß ÌÅÍÜØÅ ÏĞÅÄÈÄÓÙÅÉ
            FROM(SELECT T2.*, DENSE_RANK() OVER (PARTITION BY CARD_NUM  ORDER BY TIM) AS RAN  --ĞÀÍÆÈĞÓÅÌ ÎÏÅĞÀÖÈÈ ,ÊÎÒÎĞÛÅ ÂÕÎÄßÒ Â ÈÍÒÅĞÂÀË È ÍÅ ÂÕÎÄßÒ
                          FROM (select t.*,count(AMT)  OVER (PARTITION BY  CARD_NUM  ORDER BY TRANS_DATE ROWS  BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING  ) AS SU,
                                                   FIRST_VALUE(TRANS_DATE) OVER (PARTITION BY  CARD_NUM ORDER BY trans_date ) AS F,
                                                   FIRST_VALUE(TRANS_DATE) OVER (PARTITION BY  CARD_NUM ORDER BY trans_date ) + ( to_date('15-10-2021 06:20:00' , 'DD.MM.YYYY HH24:MI:ss') - to_date('15-10-2021 06:00:00' , 'DD.MM.YYYY HH24:MI:ss')) AS FV,
                                                   CASE WHEN TRANS_DATE BETWEEN  FIRST_VALUE(TRANS_DATE) OVER (PARTITION BY  CARD_NUM ORDER BY trans_date ) AND
                                                   FIRST_VALUE(TRANS_DATE) OVER (PARTITION BY  CARD_NUM ORDER BY trans_date ) + ( to_date('15-10-2021 06:20:00' , 'DD.MM.YYYY HH24:MI:ss') - to_date('15-10-2021 06:00:00' , 'DD.MM.YYYY HH24:MI:ss')) 
                                                   THEN 1 ELSE 0 END AS TIM --ÂÕÎÄÈÒ ËÈ ÎÏÅĞÀÖÈß Â ÈÍÒÅĞÂÀË 20 ÌÈÍ
                                       from FACT_TRANSACTIONS   t  )  t2 )T3 )T4 )T5
WHERE SU2=0 AND t5.CO >=3 AND t5.OPER_RESULT = 'Óñïåøíî' and oper_type != 'Ïîïîëíåíèå') T6
JOIN DIM_CARDS C
ON C.CARD_NUM = T6.CARD_NUM
JOIN DIM_ACCOUNTS A
ON A.ACCOUNT_NUM = C.ACCOUNT_NUM
JOIN DIM_CLIENTS C
ON A.CLIENT = C.CLIENT_ID



