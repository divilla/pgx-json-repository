create schema if not exists test;

create table if not exists test1
(
    id serial not null
        constraint test1_pk
            primary key,
    a_a text not null,
    "b_B" integer not null,
    cc_cc boolean default true not null
);

create table if not exists test."Test2"
(
    "Id" serial not null
        constraint test2_pk
            primary key,
    "X" text not null,
    "Y" integer not null,
    "Z" boolean default true not null
);
