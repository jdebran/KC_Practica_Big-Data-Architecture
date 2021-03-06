create database kcbda_raw;
create database kcbda_staging;
create database kcbda_trusted;

use kcbda_raw;

CREATE EXTERNAL TABLE ODS_DM_AGENTES_CC (
  ID_AGENTE_CC int,
  DE_AGENTE_CC string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_AGENTES_CC";

CREATE EXTERNAL TABLE ODS_DM_AGENTES_PRV (
  ID_AGENTE_PRV int,
  DE_AGENTE_PRV string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_AGENTES_PRV";

CREATE EXTERNAL TABLE ODS_DM_CANALES (
  ID_CANAL int,
  DE_CANAL string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_CANALES";

CREATE EXTERNAL TABLE ODS_DM_CICLOS_FACTURACION (
  ID_CICLO_FACTURACION int,
  DE_CICLO_FACTURACION string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_CICLOS_FACTURACION";

CREATE EXTERNAL TABLE ODS_DM_CIUDADES_ESTADOS (
  ID_CIUDAD_ESTADO int,
  DE_CIUDAD string,
  DE_ESTADO string,
  ID_PAIS int,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_CIUDADES_ESTADOS";

CREATE EXTERNAL TABLE ODS_DM_COMPANYAS (
  ID_COMPANYA int,
  DE_COMPANYA string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_COMPANYAS";

CREATE EXTERNAL TABLE ODS_DM_DEPARTAMENTOS_CC (
  ID_DEPARTAMENTO_CC int,
  DE_DEPARTAMENTO_CC string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_DEPARTAMENTOS_CC";

CREATE EXTERNAL TABLE ODS_DM_FASES (
  ID_FASE int,
  DE_FASE string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_FASES";

CREATE EXTERNAL TABLE ODS_DM_METODOS_PAGO (
  ID_METODO_PAGO int,
  DE_METODO_PAGO string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_METODOS_PAGO";

CREATE EXTERNAL TABLE ODS_DM_PAISES (
  ID_PAIS int,
  DE_PAIS string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_PAISES";

CREATE EXTERNAL TABLE ODS_DM_PRODUCTOS (
  ID_PRODUCTO int,
  DE_PRODUCTO string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_PRODUCTOS";

CREATE EXTERNAL TABLE ODS_DM_PROFESIONES (
  ID_PROFESION int,
  DE_PROFESION string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_PROFESIONES";

CREATE EXTERNAL TABLE ODS_DM_SEXOS (
  ID_SEXO int,
  DE_SEXO string,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_DM_SEXOS";

CREATE EXTERNAL TABLE ODS_HC_CLIENTES (
  ID_CLIENTE int,
  NOMBRE_CLIENTE string,
  APELLIDOS_CLIENTE string,
  NUMDOC_CLIENTE string,
  ID_SEXO int,
  ID_DIRECION_CLIENTE int,
  TELEFONO_CLIENTE bigint,
  EMAIL string,
  FC_NACIMIENTO date,
  ID_PROFESION int,
  ID_COMPANYA int,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_CLIENTES";

CREATE EXTERNAL TABLE ODS_HC_DIRECCIONES (
  ID_DIRECCION int,
  DE_DIRECCION string,
  DE_CP int,
  ID_CIUDAD_ESTADO int,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_DIRECCIONES";

CREATE EXTERNAL TABLE ODS_HC_FACTURAS (
  ID_FACTURA int,
  ID_CLIENTE int,
  FC_INICIO timestamp,
  FC_FIN timestamp,
  FC_ESTADO timestamp,
  FC_PAGO timestamp,
  ID_CICLO_FACTURACION int,
  ID_METODO_PAGO int,
  CANTIDAD decimal(4,2),
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_FACTURAS";

CREATE EXTERNAL TABLE ODS_HC_LLAMADAS (
  ID_LLAMADA int,
  ID_IVR int,
  TELEFONO_LLAMADA bigint,
  ID_CLIENTE int,
  FC_INICIO_LLAMADA timestamp,
  FC_FIN_LLAMADA timestamp,
  ID_DEPARTAMENTO_CC int,
  FLG_TRANSFERIDO tinyint,
  ID_AGENTE_CC int,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_LLAMADAS";

CREATE EXTERNAL TABLE ODS_HC_PROVISIONES (
  ID_PROVISION int,
  ID_ORDER int,
  ID_SERVICIO int,
  ID_FASE int,
  ID_AGENTE_PRV int,
  FC_INICIO timestamp,
  FC_FIN timestamp,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_PROVISIONES";

CREATE EXTERNAL TABLE ODS_HC_SERVICIOS (
  ID_SERVICIO int,
  ID_CLIENTE int,
  ID_PRODUCTO int,
  PUNTO_ACCESO string,
  ID_CANAL int,
  ID_AGENTE int,
  ID_DIRECCION_SERVICIO int,
  FC_INICIO timestamp,
  FC_INSTALACION timestamp,
  FC_FIN timestamp,
  FC_INSERT timestamp,
  FC_MODIFICATION timestamp
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ","
STORED AS TEXTFILE
LOCATION "/kcbda/RAW/ODS_HC_SERVICIOS";
