/***************************************************************************************************
Session:      Snowflake 101
Version:      v1
Chapter:      2. Data ingestion
Create Date:  2025-03-01
Author:       Pablo Sáenz de Tejada 
***************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                Comments
------------------- --------------------- ---------------------------------------------------------
2025-03-01          Pablo Sáenz de Tejada Initial Training Release
***************************************************************************************************/


/*-------------------------------------------------------------------------------------------------

Paso 1 - Objetos de Snowflake

    Snowflake utiliza una variedad de objetos para organizar y gestionar los datos. 
    Estos son los principales:

    · Objetos de Base de Datos:
    
        - Bases de datos: Colecciones de esquemas, tablas, vistas y otros objetos. Organizan el contenido 
        de datos de alto nivel.
        - Esquemas: Subdivisiones dentro de una base de datos. Agrupan tablas, vistas y otros 
        objetos relacionados.
        - Tablas: Almacenan datos en filas y columnas. Son la base para la mayoría de las operaciones de datos.
        - Vistas: Consultas guardadas que presentan datos de una o más tablas. Proporcionan una forma de
        simplificar y personalizar el acceso a los datos.
        - Funciones: Rutinas de codigo SQL o Python que se utilizan para calcular valores que se usan dentro
        de consultas. Permiten la reutilización de lógica y la personalización del comportamiento de SQL.
        - Procedimientos almacenados: Rutinas de código SQL o Python que realizan una secuencia de operaciones.
        Usados para automatizar tareas complejas y la lógica de negocio.
        - Stages: Ubicaciones de almacenamiento para archivos de datos. Sirven como área de preparación para
        la carga y descarga de datos.

    · Objetos de Seguridad:

        - Roles: Conjuntos de privilegios que se otorgan a los usuarios. Controlan el acceso a los objetos de datos.
        - Usuarios: Cuentas individuales que acceden a Snowflake. Se les asignan roles para determinar sus permisos.
    
    · Objetos de Cómputo:

        - Virtual Warehouses: Clústeres de cómputo que ejecutan consultas y transformaciones de datos. Permiten
        escalar el cómputo de forma independiente del almacenamiento.

    · Otros Objetos:

        - File Formats: Definiciones de la estructura de archivos de datos (por ejemplo, CSV, JSON). Permiten a
        Snowflake interpretar y procesar archivos de datos.
        - Sequences: Generadores de valores numéricos únicos. Se utilizan para crear claves primarias y otros
        identificadores únicos.

    Snowflake cuenta con un modelo de seguridad RBAC - Role-based access control - en el que los permisos a
    estos objetos se otorgan a roles, no a usuarios individuales.
    
----------------------------------------------------------------------------------------------------------/

/* Para ingestar datos en el schema DGT.MATRICULACIONES, lo primero que tenemos que hacer es definir
el contexto que queremos utilizar: El rol, base de datos y esquema.
*/

USE ROLE dba;
USE SCHEMA dgt1.matriculaciones;
USE WAREHOUSE dgt_data_loading;

-- Para cargar los datos, necesitamos un Stage
USE ROLE securityadmin;
GRANT CREATE STAGE ON SCHEMA dgt1.matriculaciones TO ROLE dba;
USE ROLE dba;
USE SCHEMA dgt1.matriculaciones;
CREATE OR REPLACE STAGE matriculaciones_stg
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = (TYPE = JSON)
    COMMENT = 'Stage para ingestar ficheros de matriculaciones en formato JSON';

-- Y vamos a cargar los ficheros en el Stage desde Snowsight (Snowflake's Web UI)

-- Una vez cargados los ficheros, necesitamos una tabla donde ingestarlos en crudo
USE ROLE dba;
CREATE OR REPLACE TABLE raw_matriculaciones
    (
    INGESTION_TIMESTAMP TIMESTAMP,
    FILENAME VARCHAR,
    DATA VARIANT
    )
DATA_RETENTION_TIME_IN_DAYS = 7
COMMENT = 'Datos RAW de matriculaciones de la DGT';

-- Ahora, vamos a cargar los ficheros en la nueva tabla
COPY INTO dgt1.matriculaciones.raw_matriculaciones
     FROM (
        SELECT CURRENT_TIMESTAMP AS INGESTION_TIMESTAMP,
        METADATA$FILENAME AS FILENAME,
        $1 AS DATA
     FROM '@MATRICULACIONES_STG/2025-03-11')
     FILE_FORMAT = ( TYPE = JSON );

-- Comprobamos la ingesta
SELECT * FROM dgt1.matriculaciones.raw_matriculaciones LIMIT 100;
