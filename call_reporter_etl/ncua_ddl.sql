-- This defines the tables for NCUA data.

create schema if not exists ncua;

/*
create table if not exists ncua.acct_descgrants as (
    field_name varchar primary key,
    type varchar,
    size numeric(32,2),
    description varchar,
    year integer(4),
    month integer(2),
    source varchar
);
*/

create table if not exists ncua.foicu (
    cu_number integer,
    cycle_date varchar,
    join_number  integer,
    rssd numeric(32,5),
    cu_type  integer,
    cu_name varchar,
    city varchar,
    state varchar,
    charterstate varchar,
    state_code  integer,
    zip_code varchar,
    county_code  integer,
    cong_dist numeric(32,5),
    smsa  integer,
    attention_of varchar,
    street varchar,
    region  integer,
    se varchar,
    district  integer,
    year_opened  integer,
    tom_code  char(4),
    limited_inc  integer,
    issue_date varchar,
    peer_group  integer,
    quarter_flag  integer,
    ismdi  integer,
    insured_date varchar,
    source_year int,
    source_month int,
    source varchar,
    cu_number_mon_year_key char(14) primary key
);
