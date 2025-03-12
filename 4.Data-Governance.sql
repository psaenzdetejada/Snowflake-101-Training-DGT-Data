/***************************************************************************************************
Session:      Snowflake 101
Version:      v1
Chapter:      4. Data Governance
Create Date:  2025-03-01
Author:       Pablo Sáenz de Tejada 
***************************************************************************************************
SUMMARY OF CHANGES
Date(yyyy-mm-dd)    Author                Comments
------------------- --------------------- ---------------------------------------------------------
2025-03-01          Pablo Sáenz de Tejada Initial Training Release
***************************************************************************************************/

USE ROLE dba;

USE SCHEMA dgt1.matriculaciones;

CREATE OR REPLACE TAG
    administrators;

CREATE OR REPLACE MASKING POLICY string_mask
    AS (VAL STRING) RETURNS STRING -> 
        CASE WHEN CURRENT_ROLE() IN ('DBA') THEN val 
        ELSE '***MASKED***'
        END;

ALTER TAG administrators SET MASKING POLICY string_mask;

ALTER TAG administrators SET MASKING POLICY string_mask;


ALTER DYNAMIC TABLE dgt1.matriculaciones.curated_matriculaciones ALTER COLUMN bastidor SET TAG administrators = 'tag-based policies';

SELECT * FROM dgt1.matriculaciones.curated_matriculaciones LIMIT 100;

USE ROLE data_engineer_dgt;
SELECT * FROM dgt1.matriculaciones.curated_matriculaciones LIMIT 100;
