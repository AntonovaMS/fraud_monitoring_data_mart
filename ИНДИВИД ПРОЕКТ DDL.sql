
alter SESSION set NLS_DATE_FORMAT = 'DD.MM.YYYY HH24:MI:SS';

DROP TABLE DIM_CLIENTS cascade constraints ;
DROP TABLE DIM_TERMINALS cascade constraints ;
DROP TABLE DIM_ACCOUNTS cascade constraints ;
DROP TABLE DIM_CARDS cascade constraints;
DROP TABLE FACT_TRANSACTIONS cascade constraints ;
DROP TABLE META ;
drop table STG_ALL;


CREATE TABLE META (
db_name varchar2(30),
tbl_name varchar2(30),
max_dt_update date
);

create table STG_ALL as select * from SRC_ALL where 1=0;

CREATE TABLE DIM_CLIENTS
(
  client_id VARCHAR2(50) ,
  last_name varchar2(50),
  first_name VARCHAR2(50),
  patrinymic VARCHAR2(50),
  date_of_birth date,
  passport_num varchar2(50),
  passport_valid_to date,
  phone varchar2(20),
  create_dt date,
  update_dt date,
  constraint pk_clients primary key (client_id)
);



CREATE TABLE DIM_TERMINALS
(
  terminal_id VARCHAR2(50) ,
  terminal_type VARCHAR2(50),
  terminal_city VARCHAR2(50),
  terminal_address VARCHAR2(100),
  create_dt date,
  update_dt date,
  constraint pk_terminals primary key (terminal_id)
);


CREATE TABLE DIM_ACCOUNTS 
(
  account_num VARCHAR2(20) ,
  valid_to date,
  client  VARCHAR2(20),
  create_dt date,
  update_dt date,
  constraint pk_accounts primary key (account_num),
  CONSTRAINT fk_clients FOREIGN KEY (client) REFERENCES STUDENT_ANTONOVA.DIM_CLIENTS(client_id)
);


CREATE TABLE DIM_CARDS
(
  card_num VARCHAR2(50) ,
  account_num VARCHAR2(50),
  create_dt date,
  update_dt date,
  constraint pk_cards primary key (card_num),
  CONSTRAINT fk_accounts FOREIGN KEY (account_num) REFERENCES STUDENT_ANTONOVA.DIM_ACCOUNTS(account_num)
);



CREATE TABLE FACT_TRANSACTIONS 
(
  trans_id VARCHAR2(50) ,
  trans_date date,
  card_num VARCHAR2(50),
  oper_type VARCHAR2(50),
  amt decimal,
  oper_result varchar2(50),
  terminal varchar2(50),
  CONSTRAINT fk_cards FOREIGN KEY (card_num) REFERENCES STUDENT_ANTONOVA.DIM_CARDS(card_num),
  CONSTRAINT fk_terminals FOREIGN KEY (terminal) REFERENCES STUDENT_ANTONOVA.DIM_TERMINALS(terminal_id)
);

