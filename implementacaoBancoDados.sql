select current_database();

create database dados_estacao;

create table dado_diario (
	codigo serial not null primary key,
	dia timestamp not null unique,
	media_umidade double precision not null,
	minimo_umidade double precision not null,
	maximo_umidade double precision not null,
	mediana_umidade double precision not null,
	moda_umidade double precision not null,
	media_pressao double precision not null,
	minimo_pressao double precision not null,
	maximo_pressao double precision not null,
	mediana_pressao double precision not null,
	moda_pressao double precision not null,
	media_temp_int double precision not null,
	minimo_temp_int double precision not null,
	maximo_temp_int double precision not null,
	mediana_temp_int double precision not null,
	moda_temp_int double precision not null,
	media_temp_ext double precision not null,
	minimo_temp_ext double precision not null,
	maximo_temp_ext double precision not null,
	mediana_temp_ext double precision not null,
	moda_temp_ext double precision not null
);

create or replace view  medias_diarias as
select dia, media_umidade, media_pressao, media_temp_int, media_temp_ext from dado_diario
;

create or replace view medias_min_max_temp as
select 
dia,media_temp_int, minimo_temp_int, maximo_temp_int,
media_temp_ext, minimo_temp_ext, maximo_temp_ext
from dado_diario;

create or replace view minimas_totais as 
select
codigo, dia, minimo_umidade , minimo_pressao ,minimo_temp_int ,minimo_temp_ext
from dado_diario;

create or replace view maximas_totais as
select
codigo, dia, maximo_umidade, maximo_pressao, maximo_temp_int, maximo_tem_ext
from dado_diario;

create or replace view medianas_totais as
select
codigo, dia, mediana_umidade ,mediana_pressao ,mediana_temp_int ,mediana_temp_ext
from dado_diario;

create or replace view modas_totais as
select
codigo, dia, moda_umidade ,moda_pressao ,moda_temp_int ,moda_temp_ext
from dado_diario;

create or replace view medias_totais as
select
codigo, dia, media_umidade ,moda_pressao ,moda_temp_int ,moda_temp_ext
from dado_diario;

select * from minimas_totais;

drop view minimas_totais


select * from medias_diarias 
where 
dia between '2022-1-1' and '2022-12-31'
and media_temp_int <= 28
order by dia

select * from medias_min_max_temp
where
dia between '2023-1-1' and '2023-6-2'
and minimo_temp_int <= 20
order by dia

select min(minimo_temp_int) from medias_min_max_temp

select * from dado_diario where maximo_temp_int >= 60

select * from dado_diario where dia='2023-6-3';

truncate table dado_diario;

drop table dado_diario cascade;

show datastyle;



