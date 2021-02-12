create schema test;

create table if not exists test1
(
    id serial not null
        constraint test1_pk
            primary key,
    a text not null,
    b integer not null,
    c boolean default true not null
);

create table if not exists test."Test2"
(
    "Id" serial not null,
    "X" text not null,
    "Y" integer not null,
    "Z" boolean default true not null
);
