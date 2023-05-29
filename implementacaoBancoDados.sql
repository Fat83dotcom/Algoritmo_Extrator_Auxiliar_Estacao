select current_database();

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
select dia, media_umidade, media_pressao, media_temp_int, media_temp_ext from dado_diario;

create or replace view medias_min_max_temp as
select 
dia,media_temp_int, minimo_temp_int, maximo_temp_int,
media_temp_ext, minimo_temp_ext, maximo_temp_ext
from dado_diario;

drop view medias_min_max_diarias

select * from medias_diarias 
where 
dia between '2022-1-1' and '2022-12-31'
and media_temp_int <= 28
order by dia

select * from medias_min_max_temp
where
dia between '2023-1-1' and '2023-12-31'
and minimo_temp_int <= 25
order by dia

select * from dado_diario where maximo_temp_int >= 60


truncate table dado_diario;

drop table dado_diario;

show datastyle;



