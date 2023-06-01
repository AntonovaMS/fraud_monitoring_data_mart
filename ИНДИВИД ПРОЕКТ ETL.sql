
CREATE OR REPLACE PROCEDURE META_FIRST_TIME
IS
BEGIN
-- ЗАПОЛНЕНИЕ ТАБЛИЦЫ МЕТА ДАННЫХ ДЛЯ ПЕРВОЙ ЗАГРУЗКИ  
insert into META
select 'ANTONOVA', 'FACT_TRANSACTIONS', to_date('01.01.1800', 'dd.mm.yyyy') from dual
where (select count(*) from META where db_name='ANTONOVA' and tbl_name='FACT_TRANSACTIONS')=0;
COMMIT;    
 
insert into META
select 'ANTONOVA', 'DIM_TERMINALS', to_date('01.01.1800', 'dd.mm.yyyy') from dual
where (select count(*) from META where db_name='ANTONOVA' and tbl_name='DIM_TERMINALS')=0;
COMMIT;  
 
insert into META
select 'ANTONOVA', 'DIM_CARDS', to_date('01.01.1800', 'dd.mm.yyyy') from dual
where (select count(*) from META where db_name='ANTONOVA' and tbl_name='DIM_CARDS')=0;
COMMIT;    

insert into META
select 'ANTONOVA', 'DIM_ACCOUNTS', to_date('01.01.1800', 'dd.mm.yyyy') from dual
where (select count(*) from META where db_name='ANTONOVA' and tbl_name='DIM_ACCOUNTS')=0;
COMMIT;  
 
insert into META
select 'ANTONOVA', 'DIM_CLIENTS', to_date('01.01.1800', 'dd.mm.yyyy') from dual
where (select count(*) from META where db_name='ANTONOVA' and tbl_name='DIM_CLIENTS')=0;
COMMIT;  
END;
 
 
CREATE OR REPLACE PROCEDURE ENCRIMENT_AND_UPLOAD
IS
BEGIN
---------------------ВЫДЕЛЕНИЕ ИНКРИМЕНТА И ЗАГРУЗКА В ТАБЛИЦЫ
-- Выделение и загрузка  инкремента в stg

insert into STG_ALL
select * from SRC_ALL 
where (to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) > ( select max_dt_update 
                                                                                   from META
                                                                                 where db_name='ANTONOVA'
                                                                                    and tbl_name='FACT_TRANSACTIONS');
COMMIT;

----ЗАГРУЗКА В ТАБЛИЦУ DIM_CLIENTS
merge into DIM_CLIENTS  cl
using (select distinct client AS CLIENT_ID , last_name, first_name, patronymic AS PATRINYMIC, date_of_birth,passport AS PASSPORT_NUM, passport_valid_to, phone , trunc(TO_DATE(datee, 'DD.MM.YYYY HH24:MI:ss')) AS CREATE_DT ,trunc(TO_DATE(datee, 'DD.MM.YYYY HH24:MI:ss'))   as UPDATE_DT 
          FROM (SELECT A. * FROM STG_ALL  A
                      INNER JOIN (SELECT MAX(DATEE) AS DATEE , CLIENT  FROM STG_ALL GROUP BY CLIENT)  B 
                                    ON A.CLIENT= B.CLIENT AND A.DATEE = B.DATEE ) A ) STG
on (cl.client_id = stg.client_ID)
when matched then update set  cl.LAST_NAME = STG.LAST_NAME , cl.FIRST_NAME =  STG.FIRST_NAME, cl.PATRINYMIC = STG.PATRiNYMIC,
                                                 cl.DATE_OF_BIRTH= STG.DATE_OF_BIRTH, cl.PASSPORT_NUM =   STG.PASSPORT_num, cl.PASSPORT_VALID_TO = STG.PASSPORT_VALID_TO,
                                                 cl.PHONE = STG.PHONE, cl.update_DT = STG.update_dt
when not matched then insert (cl.CLIENT_ID, cl.LAST_NAME, cl.FIRST_NAME, cl.PATRINYMIC, cl.DATE_OF_BIRTH,cl.PASSPORT_NUM, cl.PASSPORT_VALID_TO, cl.PHONE, cl.CREATE_DT,  cl.UPDATE_DT)
                                     values (stg.client_ID, stg.last_name, stg.first_name, stg.patrinymic,stg.date_of_birth, stg.passport_NUM, stg.passport_valid_to , stg.phone, stg.CREATE_DT , stg.UPDATE_DT);
commit;

----ЗАГРУЗКА В ТАБЛИЦУ DIM_TERMINALS
merge into DIM_terminals  te
using (select distinct terminal, terminal_type, city, address,  trunc(TO_DATE(datee, 'DD.MM.YYYY HH24:MI:ss')) as dd from  STG_ALL )stg 
on (te.terminal_id = stg.terminal)
when matched then update set 
                                                te.terminal_type = STG.terminal_type,
                                                te.terminal_city  = stg.city,
                                                te.terminal_address = stg.address,
                                               te.UPDATE_DT = stg.dd
when not matched then insert (te.terminal_id,  te.terminal_type, te.terminal_city, te.terminal_address, te.CREATE_DT, TE.UPDATE_DT )
                                     values (stg.terminal, stg.terminal_type, stg.city, stg.address,  stg.dd ,STG.DD);                 
commit;


----УКЛАДКА ИНСЕРТ И АПДЕЙТ ЗАПИСЕЙ В  ТАБЛИЦУ ПРИЕМНИК DIM_ACCOUNTS  
merge into DIM_ACCOUNTS  ac
using (select distinct account, account_valid_to, client, trunc(TO_DATE(datee, 'DD.MM.YYYY HH24:MI:ss')) as dd from  STG_ALL ) stg 
on (AC.ACCOUNT_NUM = stg.account)
when matched then update set 
                                                AC.VALID_TO=stg.ACCOUNT_VALID_TO, 
                                                AC.CLIENT = STG.CLIENT, 
                                               ac.UPDATE_DT = stg.dd
when not matched then insert (AC.ACCOUNT_NUM, AC.VALID_TO,AC.CLIENT,AC.CREATE_DT ,AC.UPDATE_DT)
                                     values (stg.ACCOUNT, stg.ACCOUNT_VALID_TO, STG.CLIENT, stg.dd, STG.DD);
commit;

----УКЛАДКА ИНСЕРТ И АПДЕЙТ ЗАПИСЕЙ В  ТАБЛИЦУ ПРИЕМНИК DIM_CARDS
merge into DIM_cards  ca
using ( select distinct card, account, trunc(TO_DATE(datee, 'DD.MM.YYYY HH24:MI:ss')) as dd  from stg_all ) stg 
on (ca.card_num = stg.card)
when matched then update set 
                                                ca.account_num = STG.account, 
                                               ca.UPDATE_DT = stg.dd
when not matched then insert (ca.card_num,  ca.account_num, ca.CREATE_DT , CA.UPDATE_DT)
                                     values (stg.card, stg.ACCOUnt,  stg.dd, STG.DD);
commit;

--ЗАГРУЗКА ТРАНЗАКЦИЙ В ФАКТ-ТРАЗАКЦИОНС

  
insert into FACT_TRANSACTIONS (TRANS_ID , TRANS_DATE, CARD_NUM, OPER_TYPE, AMT, OPER_RESULT, TERMINAL)
SELECT TRANS_ID, TO_timestamp(DATEE,'dd.mm.yyyy hh24:mi:ss'), CARD, OPER_TYPE, AMOUNT, OPER_RESULT, TERMINAL 
   FROM STG_ALL WHERE (to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) > ( select max_dt_update 
                                                                                                                   from META
                                                                                                                 where db_name='ANTONOVA'
                                                                                                                    and tbl_name='FACT_TRANSACTIONS');
commit;
END;



CREATE OR REPLACE PROCEDURE META_UPDATE
IS
BEGIN    
/*6 Обновление мета-данных*/
update META set max_dt_update = (select max(to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) from STG_ALL) WHERE  db_name='ANTONOVA'
        and tbl_name='FACT_TRANSACTIONS';
update META set max_dt_update = (select max(to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) from STG_ALL) WHERE  db_name='ANTONOVA'
        and tbl_name='DIM_TERMINALS';
update META set max_dt_update = (select max(to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) from STG_ALL) WHERE  db_name='ANTONOVA'
        and tbl_name='DIM_CARDS';
update META set max_dt_update = (select max(to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) from STG_ALL) WHERE  db_name='ANTONOVA'
        and tbl_name='DIM_ACCOUNTS';
update META set max_dt_update = (select max(to_date(DATEE,'dd.mm.yyyy hh24:mi:ss')) from STG_ALL) WHERE  db_name='ANTONOVA'
        and tbl_name='DIM_CLIENTS';
commit;
--ОЧИСТКА СТЕЙДЖИНГА
DELETE STG_ALL;
COMMIT;
END;
