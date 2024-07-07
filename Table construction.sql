create database project1 encoding = 'utf8';

create table line
(
    name          varchar(200)     not null
        constraint line_pk primary key,
    start_time    time,
    end_time      time,
    intro         varchar(8000),
    mileage       double precision not null,
    color         varchar(20)      not null,
    first_opening date,
    url           varchar(1000)    not null
);

create table station
(
    id           serial primary key,
    district     varchar(200),
    English_name varchar(200) not null
        constraint English_name_unq unique,
    Chinese_name varchar(200) not null,
    intro        varchar(8000)
);

create table line_detail
(
    line_name    varchar(200) not null
        references line (name),
    station_name varchar(200) not null
        references station (English_name)
);

create table exit
(
    id         serial primary key,
    station_id integer      not null
        references station (id),
    number     varchar(100) not null
);

create table buildings
(
    id      serial primary key,
    exit_id integer      not null
        references exit (id),
    name    varchar(200) not null
);

create table bus_stop
(
    id      serial primary key,
    exit_id integer      not null
        references exit (id),
    name    varchar(200) not null
);

create table bus_line
(
    bus_stop_id integer      not null
        constraint bus_lines_fk
            references bus_stop,
    name        varchar(200) not null,

    primary key (name, bus_stop_id)
);

create table card
(
    code        varchar(400) not null
        constraint card_pk primary key,
    money       float4       not null,
    create_time timestamp
);

create table passenger
(
    id           varchar(400) not null
        constraint passenger_pk primary key,
    name         varchar(20)  not null,
    phone_number bigint,
    gender       varchar(10)  not null,
    district     varchar(400) not null
);

CREATE TABLE passenger_ride
(
    id            SERIAL PRIMARY KEY,
    passenger_id  VARCHAR(400) NOT NULL
        constraint passenger_id_fk
            references passenger (id),
    start_station VARCHAR(200) NOT NULL REFERENCES station (english_name),
    end_station   VARCHAR(200) NOT NULL REFERENCES station (english_name),
    price         INTEGER,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP
);

CREATE TABLE card_ride
(
    id            SERIAL PRIMARY KEY,
    card_code     VARCHAR(400) NOT NULL
        constraint card_code_fk
            references card (code),
    start_station VARCHAR(200) NOT NULL REFERENCES station (english_name),
    end_station   VARCHAR(200) NOT NULL REFERENCES station (english_name),
    price         INTEGER,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP
);