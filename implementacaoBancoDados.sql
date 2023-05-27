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

select * from dado_diario;

