CREATE SCHEMA IF NOT EXISTS b_arganaraz_londero_coderhouse;

CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos(
	Clave_Compuesta VARCHAR(50) PRIMARY KEY,
	Fecha_Generacion DATE,
	Codigo VARCHAR(50),
	Empresa VARCHAR(300),
	Sector VARCHAR(50),
	Industria VARCHAR(100),
	Apertura DECIMAL(10,2),
	Maximo DECIMAL(10,2),
	Minimo DECIMAL(10,2),
	Cierre DECIMAL(10,2),
	Volumen INT,
	Fecha_Extraccion TIMESTAMP
);

CREATE TABLE IF NOT EXISTS b_arganaraz_londero_coderhouse.cotizacion_magnificos_staging(
	Clave_Compuesta VARCHAR(50) PRIMARY KEY,
	Fecha_Generacion DATE,
	Codigo VARCHAR(50),
	Empresa VARCHAR(300),
	Sector VARCHAR(50),
	Industria VARCHAR(100),
	Apertura DECIMAL(10,2),
	Maximo DECIMAL(10,2),
	Minimo DECIMAL(10,2),
	Cierre DECIMAL(10,2),
	Volumen INT,
	Fecha_Extraccion TIMESTAMP
);
