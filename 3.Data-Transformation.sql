/***************************************************************************************************
Session:      Snowflake 101
Version:      v1
Chapter:      3. Data Transformation
Create Date:  2025-03-01
Author:       Pablo Sáenz de Tejada 
***************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                Comments
------------------- --------------------- ---------------------------------------------------------
2025-03-01          Pablo Sáenz de Tejada Initial Training Release
***************************************************************************************************/

-- Vamos a crear tablas adicionales que permitan un análisis más sencillo de los datos ingestados en RAW.

-- Para ello, primero vamos a crear un nuevo rol: DATA_ENGINEER_DGT

USE ROLE USERADMIN;
CREATE ROLE DATA_ENGINEER_DGT
    COMMENT = 'Data Engineering role for database DGT';

GRANT ROLE DATA_ENGINEER_DGT TO ROLE DBA;
GRANT ROLE DATA_ENGINEER_DGT TO USER PSAENZDETEJADA;

USE ROLE SECURITYADMIN;
GRANT USAGE ON DATABASE DGT1 TO ROLE DATA_ENGINEER_DGT;
GRANT USAGE ON SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;
GRANT ALL ON FUTURE TABLES IN SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;
GRANT ALL ON FUTURE DYNAMIC TABLES IN SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;
GRANT CREATE DYNAMIC TABLE ON SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;
GRANT CREATE TABLE ON SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;
GRANT CREATE DYNAMIC TABLE ON SCHEMA DGT1.MATRICULACIONES TO ROLE DATA_ENGINEER_DGT;

-- Vamos también a crear un nuevo warehouse para la transformación de datos.
USE ROLE WAREHOUSE_ADMIN;

CREATE OR REPLACE WAREHOUSE dgt_data_transformation WITH
COMMENT = 'Warehouse para la transformación de datos de la DGT'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true -- turn on 
    INITIALLY_SUSPENDED = true;

GRANT USAGE ON WAREHOUSE DGT_DATA_TRANSFORMATION TO ROLE DATA_ENGINEER_DGT;

-- Comencemos a trabajar con los datos
USE ROLE DATA_ENGINEER_DGT;
USE WAREHOUSE DGT_DATA_TRANSFORMATION;
SELECT * FROM DGT1.MATRICULACIONES.RAW_MATRICULACIONES LIMIT 100;

-- Creamos las distintas tablas de dimensiones con los valores de negocio en lugar de códigos
USE SCHEMA DGT1.MATRICULACIONES;
USE ROLE DBA;
CREATE OR REPLACE TABLE RAW_DIM_COD_CLASE_MAT ( COD_CLASE_MAT CHAR, CLASE_MAT VARCHAR);
INSERT INTO RAW_DIM_COD_CLASE_MAT VALUES
    ('0', 'Ordinaria'),
    ('1', 'Turística'),
    ('2', 'Remolque'),
    ('3', 'Diplomática'),
    ('4', 'Reservada'),
    ('5', 'Vehículo especial'),
    ('6', 'Ciclomotor'),
    ('7', 'Transporte Temporal'),
    ('8', 'Histórica');

CREATE OR REPLACE TABLE RAW_DIM_COD_PROCEDENCIA (COD_PROCEDENCIA CHAR, PROCEDENCIA VARCHAR);
INSERT INTO RAW_DIM_COD_PROCEDENCIA VALUES
        ('0','Fabricación Nacional'),
        ('1','Importación no comunitaria'),
        ('2','Subasta'),
        ('3','Importación UE');

CREATE OR REPLACE TABLE RAW_DIM_COD_TIPO (COD_TIPO VARCHAR(2), TIPO VARCHAR);
INSERT INTO RAW_DIM_COD_TIPO VALUES
    ('00','Camión'),
    ('01','Camión plataforma'),
    ('02','Camión caja'),
    ('03','Camión furgón'),
    ('04','Camión botellero'),
    ('05','Camión cisterna'),
    ('06','Camión jaula'),
    ('07','Camión frigorífico'),
    ('08','Camión taller'),
    ('09','Camión para cantera'),
    ('0A','Camión portavehículos'),
    ('0B','Camión mixto'),
    ('0C','Camión portacontenedores'),
    ('0D','Camión basurero'),
    ('0E','Camión isotermo'),
    ('0F','Camión silo'),
    ('0G','Vehículo mixto adaptable'),
    ('10','Camión articulado'),
    ('11','Camión articulado plataforma'),
    ('12','Camión articulado caja'),
    ('13','Camión articulado furgón'),
    ('14','Camión articulado botellero'),
    ('15','Camión articulado cisterna'),
    ('16','Camión articulado jaula'),
    ('17','Camión articulado frigorífico'),
    ('18','Camión articulado taller'),
    ('19','Camión articulado para cantera'),
    ('1A','Camión articulado vivienda o caravana'),
    ('1C','Camión articulado hormigonera'),
    ('1D','Camión articulado volquete'),
    ('1E','Camión articulado grúa'),
    ('1F','Camión articulado contra incendios'),
    ('20','Furgoneta'),
    ('21','Furgoneta mixta'),
    ('22','Ambulancia'),
    ('23','Coche fúnebre'),
    ('24','Camioneta'),
    ('25','Todo terreno'),
    ('30','Autobús'),
    ('31','Autobús articulado'),
    ('32','Autobús mixto'),
    ('33','Bibliobús'),
    ('34','Autobús articulado'),
    ('35','Autobús taller'),
    ('36','Autobús sanitario'),
    ('40','Turismo'),
    ('50','Motocicleta de 2 ruedas sin sidecar'),
    ('51','Motocicleta con sidecar'),
    ('52','Motocarro'),
    ('53','Automóvil de 3 ruedas'),
    ('54','Cuatriciclo pesado'),
    ('60','Coche de inválido'),
    ('70','Vehículo especial'),
    ('71','Pala cargadora'),
    ('72','Pala excavadora'),
    ('73','Carretilla elevadora'),
    ('74','Moniveladera'),
    ('75','Compactadora'),
    ('76','Apisonadora'),
    ('77','Girogravilladora'),
    ('78','Machacadora'),
    ('79','Quitanieves'),
    ('7A','Vivienda'),
    ('7B','Barredora'),
    ('7C','Hormigonera'),
    ('7D','Volquete de canteras'),
    ('7E','Grúa'),
    ('7F','Servicio contra incendios'),
    ('7G','Aspiradora de Fangos'),
    ('7H','Motocultor'),
    ('7I','Maquinaria agrícola automotriz'),
    ('7J','Pala cargadora-retroexcavadora'),
    ('7K','Tren hasta 160 plazas'),
    ('80','Tractor'),
    ('81','Tractocamión'),
    ('82','Tractocarro'),
    ('90','Ciclomotor de 2 ruedas'),
    ('91','Ciclomotor de 3 ruedas'),
    ('92','Cuatriciclo ligero'),
    ('EX','Extranjero'),
    ('R0','Remolque'),
    ('R1','Remolque plataforma'),
    ('R2','Remolque caja'),
    ('R3','Remolque furgón'),
    ('R4','Remolque botellero'),
    ('R5','Remolque cisterna'),
    ('R6','Remolque jaula'),
    ('R7','Remolque frigorífico'),
    ('R8','Remolque Taller'),
    ('R9','Remolque para canteras'),
    ('RA','Remolque vivienda o caravana'),
    ('RB','Remolque de viajeros o de autobús'),
    ('RC','Remolque hormigonera'),
    ('RD','Remolque colquete de cantera'),
    ('RE','Remolque de grúa'),
    ('RF','Remolque contra incendios'),
    ('RH','Maq. agrícola arrastrada de 2 ejes'),
    ('S0','Semirremolque'),
    ('S1','Semirremolque plataforma'),
    ('S2','Semirremolque caja'),
    ('S3','Semirremolque furgón'),
    ('S4','Semirremolque botellero'),
    ('S5','Semirremolque cisterna'),
    ('S6','Semirremolque jaula'),
    ('S7','Semirremolque frigorífico'),
    ('S8','Semirremolque taller'),
    ('S9','Semirremolque cantera'),
    ('SA','Semirremolque vivienda o caravana'),
    ('SB','Semirremolque viajeros o autobús'),
    ('SC','Semirremolque hormigonera'),
    ('SD','Semirremolque volquete de cantera'),
    ('SE','Semirremolque grúa'),
    ('SF','Semirremolque contra incendios'),
    ('SH','Maq. agrícola arrastrada de 1 eje');


CREATE TABLE IF NOT EXISTS RAW_DIM_COD_PROPULSION (COD_CLASE_MAT VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_COD_CLASE_MAT VALUES
    ('0', 'Ordinaria'),
    ('1', 'Turística'),
    ('2', 'Remolque'),
    ('3', 'Diplomática'),
    ('4', 'Reservada'),
    ('5', 'Vehículo especial'),
    ('6', 'Ciclomotor'),
    ('7', 'Transporte Temporal'),
    ('8', 'Histórica');

CREATE TABLE IF NOT EXISTS RAW_DIM_COD_PROPULSION (COD_PROPULSION VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_COD_PROPULSION VALUES
    ('0', 'Gasolina'),
    ('1', 'Diesel'),
    ('2', 'Eléctrico'),
    ('3', 'Otros'),
    ('4', 'Butano'),
    ('5', 'Solar'),
    ('6', 'Gas Licuado de Petróleo'),
    ('7', 'Gas Natural Comprimido'),
    ('8', 'Gas Natural Licuado'),
    ('9', 'Hidrógeno'),
    ('A', 'Biometano'),
    ('B', 'Etanol'),
    ('C', 'Biodiesel');

CREATE TABLE IF NOT EXISTS RAW_DIM_COD_PROVINCIA_VEH (COD_PROVINCIA_VEH VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_COD_PROVINCIA_VEH VALUES
    ('A', 'Alicante/Alacant'),
    ('AB', 'Albacete'),
    ('AL', 'Almería'),
    ('AV', 'Ávila'),
    ('B', 'Barcelona'),
    ('BA', 'Badajoz'),
    ('BI', 'Bizkaia'),
    ('BU', 'Burgos'),
    ('C', 'Coruña (A)'),
    ('CA', 'Cádiz'),
    ('CC', 'Cáceres'),
    ('CE', 'Ceuta'),
    ('CO', 'Córdoba'),
    ('CR', 'Ciudad Real'),
    ('CS', 'Castellón/Castelló'),
    ('CU', 'Cuenca'),
    ('DS', 'Desconocido'),
    ('EX', 'Extranjero'),
    ('GC', 'Palmas (las)'),
    ('GI', 'Girona'),
    ('GR', 'Granada'),
    ('GU', 'Guadalajara'),
    ('H', 'Huelva'),
    ('HU', 'Huesca'),
    ('J', 'Jaen'),
    ('L', 'Lleida'),
    ('LE', 'León'),
    ('LO', 'Rioja (La)'),
    ('LU', 'Lugo'),
    ('M', 'Madrid'),
    ('MA', 'Málaga'),
    ('ML', 'Melilla'),
    ('MU', 'Murcia'),
    ('NA', 'Navarra'),
    ('O', 'Asturias'),
    ('OU', 'Ourense'),
    ('P', 'Palencia'),
    ('IB', 'Balears (Illes)'),
    ('PO', 'Pontevedra'),
    ('S', 'Cantabria'),
    ('SA', 'Salamanca'),
    ('SE', 'Sevilla'),
    ('SG', 'Segovia'),
    ('SO', 'Soria'),
    ('SS', 'Gipuzkoa'),
    ('T', 'Tarragona'),
    ('TE', 'Teruel'),
    ('TF', 'Santa Cruz de Tenerife'),
    ('TO', 'Toledo'),
    ('V', 'Valencia'),
    ('VA', 'Valladolid'),
    ('VI', 'Araba/Álava'),
    ('Z', 'Zaragoza'),
    ('ZA', 'Zamora');

CREATE TABLE IF NOT EXISTS RAW_DIM_COD_PROVINCIA_MAT (COD_PROVINCIA_MAT VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_COD_PROVINCIA_MAT VALUES
    ('A', 'Alicante/Alacant'),
    ('AB', 'Albacete'),
    ('AL', 'Almería'),
    ('AV', 'Ávila'),
    ('B', 'Barcelona'),
    ('BA', 'Badajoz'),
    ('BI', 'Bizkaia'),
    ('BU', 'Burgos'),
    ('C', 'Coruña (A)'),
    ('CA', 'Cádiz'),
    ('CC', 'Cáceres'),
    ('CE', 'Ceuta'),
    ('CO', 'Córdoba'),
    ('CR', 'Ciudad Real'),
    ('CS', 'Castellón/Castelló'),
    ('CU', 'Cuenca'),
    ('DS', 'Desconocido'),
    ('EX', 'Extranjero'),
    ('GC', 'Palmas (las)'),
    ('GI', 'Girona'),
    ('GR', 'Granada'),
    ('GU', 'Guadalajara'),
    ('H', 'Huelva'),
    ('HU', 'Huesca'),
    ('J', 'Jaen'),
    ('L', 'Lleida'),
    ('LE', 'León'),
    ('LO', 'Rioja (La)'),
    ('LU', 'Lugo'),
    ('M', 'Madrid'),
    ('MA', 'Málaga'),
    ('ML', 'Melilla'),
    ('MU', 'Murcia'),
    ('NA', 'Navarra'),
    ('O', 'Asturias'),
    ('OU', 'Ourense'),
    ('P', 'Palencia'),
    ('IB', 'Balears (Illes)'),
    ('PO', 'Pontevedra'),
    ('S', 'Cantabria'),
    ('SA', 'Salamanca'),
    ('SE', 'Sevilla'),
    ('SG', 'Segovia'),
    ('SO', 'Soria'),
    ('SS', 'Gipuzkoa'),
    ('T', 'Tarragona'),
    ('TE', 'Teruel'),
    ('TF', 'Santa Cruz de Tenerife'),
    ('TO', 'Toledo'),
    ('V', 'Valencia'),
    ('VA', 'Valladolid'),
    ('VI', 'Araba/Álava'),
    ('Z', 'Zaragoza'),
    ('ZA', 'Zamora');
    
CREATE TABLE IF NOT EXISTS RAW_DIM_CLAVE_TRAMITE (CLAVE_TRAMITE VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_CLAVE_TRAMITE VALUES 
    ('1','Matriculación ordinaria y de ciclomotores'),
    ('2','Transferencia'),
    ('3','Baja definitiva'),
    ('4','Baja definitiva por Plan Renove'),
    ('5','Rematriculación'),
    ('6','Baja temporal'),
    ('7','Baja definitiva por exportación y por tránsito comunitario'),
    ('8','Matriculación vehículo especial'),
    ('9','Matriculación temporal'),
    ('A','Prorroga matrícula temporal'),
    ('B','Paso de matrícula temporal a definitiva'); 

CREATE TABLE IF NOT EXISTS RAW_DIM_SERVICIO (SERVICIO VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_SERVICIO VALUES 
    ('A00','Público PUBL-Sin especificar'),
    ('A01','Público PUBL-Alquiler sin conductor'),
    ('A02','Público PUBL-Alquiler con conductor'),
    ('A03','Público PUBL-Aprendizaje de conducción'),
    ('A04','Público PUBL-Taxi'),
    ('A05','Público PUBL-Auxilio de carretera'),
    ('A07','Público PUBL-Ambulancia'),
    ('A08','Público PUBL-Funerario'),
    ('A09','Particular PART-Obras'),
    ('A10','Público PUBL-Marcancías peligrosas'),
    ('A11','Público PUBL-Basurero'),
    ('A12','Público PUBL-Transporte escolar'),
    ('A13','Público PUBL-Policía'),
    ('A14','Público PUBL-Bomberos'),
    ('A15','Público PUBL-Protección civil y salvamento'),
    ('A16','Público PUBL-Defensa'),
    ('A18','Público PUBL-Actividad económica'),
    ('A20','Público PUBL-Mercancías perecederas'),
    ('B00','Particular PART-Sin especificar'),
    ('B06','Particular PART-Agrícola'),
    ('B07','Particular PART-'),
    ('B09','Particular PART-Obras'),
    ('B17','Particular PART-Vivienda'),
    ('B18','Público PART-Actividad económica'),
    ('B19','Particular PART-Recreativa'),
    ('B21','Particular PART-Vehícula para ferias');
    
CREATE TABLE IF NOT EXISTS RAW_DIM_IND_BAJA_DEF (IND_BAJA_DEF VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_IND_BAJA_DEF VALUES 
    ('0','Desguace'),
    ('1','Agotamiento'),
    ('2','Antigüedad'),
    ('3','Renovación del parque'),
    ('4','Otros motivos'),
    ('5','RDL 4/1994, RDL 10/1994, RDL 4/1997'),
    ('7','Voluntaria'),
    ('8','Exportación'),
    ('9','Tránsito comunitario'),
    ('A','De oficio por abandono'),
    ('B','De oficio por seguridad'),
    ('C','Por tratamiento residual');

CREATE TABLE IF NOT EXISTS RAW_DIM_CATEGORIA_VEHICULO_ELECTRICO (CATEGORIA_VEHICULO_ELECTRICO VARCHAR, DESCRIPCION VARCHAR);
INSERT INTO RAW_DIM_CATEGORIA_VEHICULO_ELECTRICO VALUES 
    ('PHEV','Eléctrico enchufable'),
    ('REEV','Eléctrico de autonomía extendida'),
    ('HEV','Eléctrico híbrido'),
    ('BEV','Eléctrico de batería');

SELECT * FROM DGT1.MATRICULACIONES.RAW_MATRICULACIONES LIMIT 10;
-- Podemos fácilmente trabajar con datos semi-estructurados en Snowflake
CREATE OR REPLACE DYNAMIC TABLE DGT1.MATRICULACIONES.REFINED_MATRICULACIONES (
    AUTONOMIA_VEHICULO_ELECTRICO,
    BASTIDOR,
    CARROCERIA,
    CATEGORIA_HOMOLOGACION_EUROPEA,
    CATEGORIA_VEHICULO_ELECTRICO,
    CILINDRADA,
    CLASIFICACION_REGLAMENTO_VEHICULOS,
    CLAVE_TRAMITE,
    CO2,
    CODIGO_ECO,
    CODIGO_POSTAL,
    COD_CLASE_MAT,
    COD_MUNICIPIO_INE,
    COD_PROCEDENCIA,
    COD_PROPULSION,
    COD_PROVINCIA_MATRICULACION,
    COD_PROVINCIA_VEHICULO,
    COD_TIPO,
    CONSUMO,
    DISTANCIA_EJES,
    ECO_INNOVACION,
    FABRICANTE,
    FABRICANTE_VEHICULO_BASE,
    FECHA_MATRICULACION,
    FECHA_PROCESO,
    FECHA_TRAMITE,
    IND_BAJA_TEMPORAL,
    IND_EMBARGO,
    IND_NUEVO_USADO,
    IND_PRECINTO,
    IND_SUSTRACCION,
    KW,
    LOCALIDAD_VEHICULO,
    MARCA,
    MASA_MAXIMA_TECNICA_ADMISIBLE,
    MASA_ORDEN_MARCHA,
    MODELO,
    MUNICIPIO,
    NIVEL_EMISIONES_EURO,
    NUM_PLAZAS,
    NUM_PLAZAS_MAX,
    NUM_TRANSMISIONES,
    PERSONA_FISICA_JURIDICA,
    PESO_MAX,
    PLAZAS_PIE,
    POTENCIA,
    RENTING,
    SERVICIO,
    TARA,
    TIPO_ALIMENTACION,
    VARIANTE,
    VERSION,
    VIA_ANTERIOR,
    VIA_POSTERIOR
    )
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = DGT_DATA_TRANSFORMATION
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    AS (
    SELECT 
        DATA:AUTONOMIA_VEHICULO_ELECTRICO::VARCHAR,
        DATA:BASTIDOR_ITV::VARCHAR,
        DATA:CARROCERIA::VARCHAR,
        DATA:CATEGORIA_HOMOLOGACION_EUROPEA_ITV::VARCHAR,
        DATA:CATEGORIA_VEHICULO_ELECTRICO::VARCHAR,
        DATA:CILINDRADA::FLOAT,
        DATA:CLASIFICACION_REGLAMENTO_VEHICULOS_ITV::VARCHAR,
        DATA:CLAVE_TRAMITE::VARCHAR,
        DATA:CO2_ITV::FLOAT,
        DATA:CODIGO_ECO_ITV::VARCHAR,
        DATA:CODIGO_POSTAL::VARCHAR,
        DATA:COD_CLASE_MAT::VARCHAR,
        DATA:COD_MUNICIPIO_INE_VEH::VARCHAR,
        DATA:COD_PROCEDENCIA_ITV::VARCHAR,
        DATA:COD_PROPULSION_ITV::VARCHAR,
        DATA:COD_PROVINCIA_MAT::VARCHAR,
        DATA:COD_PROVINCIA_VEH::VARCHAR,
        DATA:COD_TIPO::VARCHAR,
        DATA:CONSUMO_WH_KM_IT::FLOAT,
        DATA:DISTANCIA_EJES_12_ITV::FLOAT,
        DATA:ECO_INNOVACION_ITV::VARCHAR,
        DATA:FABRICANTE_ITV::VARCHAR,
        DATA:FABRICANTE_VEHICULO_BASE::VARCHAR,
        DATA:FEC_MATRICULA::DATE,
        DATA:FEC_PROCESO::DATE,
        DATA:FEC_TRAMITE::DATE,
        DATA:IND_BAJA_TEMP::BOOLEAN,
        DATA:IND_EMBARGO::BOOLEAN,
        DATA:IND_NUEVO_USADO::VARCHAR,
        DATA:IND_PRECINTO::BOOLEAN,
        DATA:IND_SUSTRACCION::BOOLEAN,
        DATA:KW_ITV::FLOAT,
        DATA:LOCALIDAD_VEHICULO::VARCHAR,
        DATA:MARCA_ITV::VARCHAR,
        DATA:MASA_MAXIMA_TECNICA_ADMISIBLE::FLOAT,
        DATA:MASA_ORDEN_MARCHA_ITV::FLOAT,
        DATA:MODELO_ITV::VARCHAR,
        DATA:MUNICIPIO::VARCHAR,
        DATA:NIVEL_EMISIONES_EURO_ITV::VARCHAR,
        DATA:NUM_PLAZAS::INT,
        DATA:NUM_PLAZAS_MAX::INT,
        DATA:NUM_TRANSMISIONES:INT,
        DATA:PERSONA_FISICA_JURIDICA::VARCHAR,
        DATA:PESO_MAX::FLOAT,
        DATA:PLAZAS_PIE::FLOAT,
        DATA:POTENCIA_ITV::FLOAT,
        DATA:RENTING::BOOLEAN,
        DATA:SERVICIO::VARCHAR,
        DATA:TARA::FLOAT,
        DATA:TIPO_ALIMENTACION_ITV::VARCHAR,
        DATA:VARIANTE_ITV::VARCHAR,
        DATA:VERSION_ITV::VARCHAR,
        DATA:VIA_ANTERIOR_ITV::FLOAT,
        DATA:VIA_POSTERIOR_ITV::FLOAT    
    FROM DGT1.MATRICULACIONES.RAW_MATRICULACIONES);

SELECT * FROM DGT1.MATRICULACIONES.REFINED_MATRICULACIONES LIMIT 10;

-- Ahora vamos a crear una segunda tabla dinámica con una join al resto de dimensiones.
USE ROLE data_engineer_dgt;

CREATE OR REPLACE DYNAMIC TABLE dgt1.matriculaciones.curated_matriculaciones
TARGET_LAG = '1 hour'
    WAREHOUSE = dgt_data_transformation
    REFRESH_MODE = auto
    INITIALIZE = on_create
AS (
WITH matriculaciones AS (SELECT
    *
FROM dgt1.matriculaciones.refined_matriculaciones),
provincias_mat AS (SELECT
    cod_provincia_mat,
    descripcion
FROM dgt1.matriculaciones.raw_dim_cod_provincia_mat),
provincias_veh AS (SELECT
    cod_provincia_veh,
    descripcion
FROM dgt1.matriculaciones.raw_dim_cod_provincia_veh),
servicios AS (SELECT
    servicio,
    descripcion
FROM dgt1.matriculaciones.raw_dim_servicio),
tipos AS (SELECT
    cod_tipo,
    tipo
FROM dgt1.matriculaciones.raw_dim_cod_tipo),
procedencias AS (SELECT
    cod_procedencia,
    procedencia
FROM dgt1.matriculaciones.raw_dim_cod_procedencia),
clases AS (SELECT
    cod_clase_mat,
    clase_mat
FROM dgt1.matriculaciones.raw_dim_cod_clase_mat),
tramites AS (SELECT
    clave_tramite,
    descripcion
FROM dgt1.matriculaciones.raw_dim_clave_tramite),
propulsiones AS (SELECT
    cod_propulsion,
    descripcion
FROM dgt1.matriculaciones.raw_dim_cod_propulsion),
electricos AS (SELECT
    categoria_vehiculo_electrico,
    descripcion
FROM dgt1.matriculaciones.raw_dim_categoria_vehiculo_electrico)
SELECT 
    t1.* EXCLUDE (categoria_vehiculo_electrico, cod_provincia_matriculacion, cod_provincia_vehiculo, servicio, clave_tramite, cod_clase_mat, cod_tipo, cod_procedencia, cod_propulsion),
    t2.descripcion AS provincia_matriculacion,
    t3.descripcion AS provincia_vehiculo,
    t4.descripcion AS tipo_servicio,
    t5.tipo AS tipo_vehiculo,
    t6.procedencia AS procedencia,
    t7.clase_mat AS clase_matricula,
    t8.descripcion AS clave_tramite,
    t9.descripcion AS propulsion,
    t10.descripcion AS categoria_vehiculo_electrico
FROM matriculaciones t1 
    LEFT JOIN provincias_mat t2 ON t1.cod_provincia_matriculacion = t2.cod_provincia_mat
    LEFT JOIN provincias_veh t3 ON t1.cod_provincia_vehiculo = t3.cod_provincia_veh
    LEFT JOIN servicios t4 ON t1.SERVICIO = t4.servicio
    LEFT JOIN tipos t5 ON t1.COD_TIPO = t5.cod_tipo
    LEFT JOIN procedencias t6 ON t1.COD_PROCEDENCIA = t6.cod_procedencia
    LEFT JOIN clases t7 ON t1.COD_CLASE_MAT = t7.cod_clase_mat
    LEFT JOIN tramites t8 ON t1.CLAVE_TRAMITE = t8.clave_tramite
    LEFT JOIN propulsiones t9 ON t1.cod_propulsion = t9.cod_propulsion
    LEFT JOIN electricos t10 ON t1.categoria_vehiculo_electrico = t10.categoria_vehiculo_electrico);

SELECT * FROM dgt1.matriculaciones.curated_matriculaciones LIMIT 1;

USE ROLE DATA_ENGINEER_DGT;
SELECt * FROM dgt1.matriculaciones.curated_matriculaciones limit 100;

-- A continuación vamos a crear dos vistas, una sólo de vehículos eléctricos y otra sólo de turismos.

USE ROLE dba;
CREATE OR REPLACE VIEW curated_matriculaciones_vehiculos_electricos
  AS SELECT
    fecha_matriculacion,
    bastidor,
    codigo_postal,
    municipio,
    provincia_matriculacion,
    marca,
    modelo,
    peso_max,
    potencia,
    cilindrada,
    propulsion,
    procedencia,
    categoria_vehiculo_electrico    
    FROM dgt1.matriculaciones.curated_matriculaciones WHERE categoria_vehiculo_electrico IS NOT NULL;

SELECT * FROM CURATED_MATRICULACIONES_VEHICULOS_ELECTRICOS;
