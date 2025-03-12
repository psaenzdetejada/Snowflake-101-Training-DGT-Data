/***********************************************************************************************************
Session:      Snowflake 101
Version:      v1
Chapter:      First Steps
Create Date:  2025-03-01
Author:       Pablo Sáenz de Tejada
************************************************************************************************************

SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                Comments
------------------- --------------------- ------------------------------------------------------------------
2025-03-01          Pablo Sáenz de Tejada Initial Training Release
************************************************************************************************************/

/*----------------------------------------------------------------------------------------------------------

Paso 1 - Objetos de Snowflake. Creación y asignación de roles, bases de datos y esquemas. 

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
    
----------------------------------------------------------------------------------------------------------------------*/

-- Lo primero, vamos a revisar nuestro usuario.
SELECT CURRENT_USER();
SHOW GRANTS TO USER PSAENZDETEJADA;

-- Ahora vamos a definir el contexto con el que queremos trabajar, en este caso, el rol USERADMIN para crear roles.
USE ROLE USERADMIN;

/* Ahora crearemos un nuevo rol que se encarge de gestionar nuestra futura base de datos de la DGT.
Como mejores prácticas, se recomienda separar permisos y privilegios de account-management y permisos
entity-specific en diferentes roles. */

CREATE ROLE dba
    COMMENT = 'Rol de Administrador de Bases de datos';

-- It's recommended that you create a role hierarchy that ultimately assigns all custom roles to the 
-- SYSADMIN role, this role also has the ability to grant privileges on warehouses, databases, 
-- and other objects to other roles.
USE ROLE sysadmin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE dba;

-- Y asigamos el rol a nuestro al rol SYSADMIN para crear una jerarquía de roles, y también a nuestro usuario.
GRANT ROLE dba TO ROLE sysadmin;
GRANT ROLE dba TO USER psaenzdetejada;

-- Vamos además a definir el rol DBA como el predeterminado y a limitar el uso de roles secundarios para nuestro usuario.
USE ROLE useradmin;
ALTER USER psaenzdetejada SET
    DEFAULT_ROLE = 'dba';
    
-- A continuación crearemos nuestra primera base de datos para nuestro caso de uso de la DGT usando el nuevo rol.
USE ROLE dba;
CREATE OR REPLACE DATABASE dgt1
    COMMENT = 'Base de datos con datos abiertos de la Dirección General de Tráfico';

-- Y creamos también un Schema para la base de datos donde tengamos toda la información de matriculaciones.
CREATE OR REPLACE SCHEMA dgt1.matriculaciones
    COMMENT = 'Información de datos abiertos de la DGT sobre matriculaciones de vehículos';
    
/*----------------------------------------------------------------------------------

Paso 2 - Virtual Warehouses y Configuración

    Los Virtual Warehouses de Snowflake son clusters de cómputo independientes que permiten 
    realizar consultas y transformaciones  de datos. Son fundamentales porque separan el cómputo 
    del almacenamiento, lo que permite escalar recursos de forma independiente y optimizar costes.
    
    Su correcta creación, configuración y monitorización es clave para:
    
    · Rendimiento:
        - Dimensionar adecuadamente el tamaño del warehouse para cada carga de trabajo.
        - Evitar la contención de recursos entre usuarios y procesos.
    
    · Costes:
        - Pausar o suspender warehouses cuando no se utilizan.
        - Elegir el tipo de warehouse adecuado para cada necesidad.
    
    · Concurrencia:
        - Permite a varios usuarios y aplicaciones acceder a los mismos datos simultáneamente 
        sin afectar al rendimiento.
    
    En resumen, los Virtual Warehouses son la base de la flexibilidad y eficiencia de Snowflake,
    y su gestión adecuada es esencial para aprovechar al máximo la plataforma.
    
-------------------------------------------------------------------------------------------------*/

-- Vamos a crear un rol específico para gestionar VIRTUAL WAREHOUSES
USE ROLE useradmin;
CREATE ROLE warehouse_admin 
    COMMENT = 'Rol encargado de la creación y gestión de Warehouses';

/* A continuación vamos a dar permisos a dicho rol para crear warehouses, y asignaremos
dicho rol también a SYSADMIN para crear mantener la jerarquía de roles.
*/
USE ROLE sysadmin;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE warehouse_admin;
GRANT ROLE warehouse_admin TO ROLE sysadmin;

-- Con nuestro nuevo rol, vamos a crear un warehouse específico para la ingesta de datos de la DGT.
USE ROLE warehouse_admin;

CREATE OR REPLACE WAREHOUSE dgt_data_loading WITH
COMMENT = 'Warehouse para la carga e ingesta de datos de la DGT'
    WAREHOUSE_TYPE = 'standard'
    WAREHOUSE_SIZE = 'xsmall'
    AUTO_SUSPEND = 60
    AUTO_RESUME = true -- turn on 
    INITIALLY_SUSPENDED = true;

-- Y vamos a dar a nuestro rol DBA permisos para usar el nuevo warehouse.
USE ROLE dba;
SHOW WAREHOUSES;
USE ROLE securityadmin;
GRANT USAGE ON WAREHOUSE dgt_data_loading TO ROLE dba;
USE ROLE dba;
SHOW WAREHOUSES;

-- También podemos eliminar el permiso
USE ROLE securityadmin;
SELEcT * FROM eu_superstore.public.orders;
REVOKE USAGE ON WAREHOUSE dgt_data_loading FROM ROLE dba;
USE ROLE dba;
SELECT CURRENT_SECONDARY_ROLES() ;
SHOW WAREHOUSES;

    
    /**
     1) Warehouse Type: Warehouses are required for queries, as well as all DML operations, including
         loading data into tables. Snowflake supports Standard (most-common) or Snowpark-optimized
          Warehouse Types. Snowpark-optimized warehouses should be considered for memory-intensive
          workloads.

     2) Warehouse Size: Size specifies the amount of compute resources available per cluster in a warehouse.
         Snowflake supports X-Small through 6X-Large sizes.

     3) Max Cluster Count: With multi-cluster warehouses, Snowflake supports allocating, either statically
         or dynamically, additional clusters to make a larger pool of compute resources available.
         A multi-cluster warehouse is defined by specifying the following properties:
            - Min Cluster Count: Minimum number of clusters, equal to or less than the maximum (up to 10).
            - Max Cluster Count: Maximum number of clusters, greater than 1 (up to 10).

     4) Scaling Policy: Specifies the policy for automatically starting and shutting down clusters in a
         multi-cluster warehouse running in Auto-scale mode.

     5) Auto Suspend: By default, Auto-Suspend is enabled. Snowflake automatically suspends the warehouse
         if it is inactive for the specified period of time, in our case 60 seconds.

     6) Auto Resume: By default, auto-resume is enabled. Snowflake automatically resumes the warehouse
         when any statement that requires a warehouse is submitted and the warehouse is the
         current warehouse for the session.

     7) Initially Suspended: Specifies whether the warehouse is created initially in the ‘Suspended’ state.
    **/
 

/*----------------------------------------------------------------------------------
Step 3 - Monitorizar costes con Resource Monitors

 Un Resource Monitor puede ayudar a controlar los costes y evitar el uso inesperado 
 de créditos causado por la ejecución de virtual warehouses. Un virtual warehouse 
 consume créditos de Snowflake mientras se ejecuta. Puedes utilizar un Resource Monitor
 para supervisar el uso de créditos por parte de los virtual warehouses. 
 También puedes configurar un Resource Monitor para suspender un virtual warehouse 
 cuando alcance un límite de crédito.

Solo los usuarios con el rol ACCOUNTADMIN pueden crear un Resource Monitor, pero un 
administrador de la cuenta puede otorgar privilegios a otros roles para permitir que 
otros usuarios vean y modifiquen los monitores de recursos.
----------------------------------------------------------------------------------*/

-- Crear un Resouce Monitor
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE RESOURCE MONITOR dgt_data_transformation_rm
WITH
    CREDIT_QUOTA = 100 -- set the quota to 100 credits
    FREQUENCY = monthly -- reset the monitor monthly
    START_TIMESTAMP = immediately -- begin tracking immediately
    TRIGGERS
        ON 75 PERCENT DO NOTIFY -- notify accountadmins at 75%
        ON 100 PERCENT DO SUSPEND -- suspend warehouse at 100 percent, let queries finish
        ON 110 PERCENT DO SUSPEND_IMMEDIATE; -- suspend warehouse and cancel all queries at 110 percent

-- Aplicar el Resource Monitor a un Virtual Warehouse
ALTER WAREHOUSE dgt_data_transformation
    SET RESOURCE_MONITOR = dgt_data_transformation_rm;
