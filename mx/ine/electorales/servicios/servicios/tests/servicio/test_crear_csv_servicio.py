import pytest
from ...servicio.crear_csv_servicio import CrearCSV
from ....persistencia.repositorios.repositorio.repositorio_queries import RepoQueries
from ....persistencia.config.configuracion import Config
import json
import os
from gzip import GzipFile
from io import BytesIO

acta = [
    {
        "SHA": "-",
        "RUTA": "-",
        "ID_ACTA": 4166,
        "SECCION": 393,
        "COTEJADA": "0",
        "ID_ESTADO": 1,
        "UBICACION": "1",
        "ID_CASILLA": 2,
        "llave_acta": "1153932C061",
        "CONTABILIZA": 0,
        "ID_DISTRITO": 1,
        "PUBLICACION": "-",
        "EXT_CONTIGUA": 0,
        "ID_MUNICIPIO": 5,
        "TIPO_CASILLA": "C",
        "OBSERVACIONES": "-",
        "VOTOS_SACADOS": "-",
        "DIGITALIZACION": 0,
        "TIPO_DOCUMENTO": "-",
        "CIRCUNSCRIPCION": 2,
        "VOTOS_ASENTADOS": "-",
        "estado_distrito": "611",
        "PERSONAS_VOTARON": "-",
        "BOLETAS_SOBRANTES": "-",
        "FECHA_HORA_ACOPIO": "-",
        "FECHA_HORA_CAPTURA": "-",
        "ID_TIPO_CANDIDATURA": os.environ.get('INE_PREP2021_TIPO_CANDIDATURA_MR', 2),
        "MECANISMOS_TRASLADO": "-",
        "TIPO_DIGITALIZACION": "-",
        "ID_PROCESO_ELECTORAL": 1,
        "REPRESENTANTES_PP_CI": "46",
        "LISTA_NOMINAL_CASILLA": 746,
        "FECHA_HORA_VERIFICACION": "-",
        "REPRESENTANTES_P_VOTARON": "-",
        "asociacion_1": {
            "LOGO": "PAN.png",
            "COLOR": "#00579c",
            "ORDEN": 1,
            "VOTOS": "-",
            "candidato": " FRANCISCO JAVIER LUEVANO NU\u00d1EZ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "1",
            "TIPO_ASOCIACION": 1
        },
        "asociacion_61": {
            "LOGO": "CNR.png",
            "COLOR": "#D8BC8F",
            "ORDEN": 61,
            "VOTOS": "-",
            "candidato": "  ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "61",
            "TIPO_ASOCIACION": 8
        },
        "asociacion_62": {
            "LOGO": "VN.png",
            "COLOR": "#D1D1D1",
            "ORDEN": 62,
            "VOTOS": "-",
            "candidato": "  ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "62",
            "TIPO_ASOCIACION": 9
        }
    }
]

acta_rp = [
    {
        "SHA": "-",
        "RUTA": "-",
        "ID_ACTA": 153218,
        "SECCION": 470,
        "COTEJADA": "0",
        "ID_ESTADO": 1,
        "UBICACION": "2",
        "ID_CASILLA": 1,
        "llave_acta": "11104701S082",
        "CONTABILIZA": 0,
        "ID_DISTRITO": 1,
        "PUBLICACION": "-",
        "EXT_CONTIGUA": 0,
        "ID_MUNICIPIO": 10,
        "TIPO_CASILLA": "S",
        "OBSERVACIONES": "-",
        "VOTOS_SACADOS": "-",
        "DIGITALIZACION": 0,
        "TIPO_DOCUMENTO": "-",
        "CIRCUNSCRIPCION": 2,
        "VOTOS_ASENTADOS": "-",
        "estado_distrito": "811",
        "PERSONAS_VOTARON": "-",
        "BOLETAS_SOBRANTES": "-",
        "FECHA_HORA_ACOPIO": "-",
        "FECHA_HORA_CAPTURA": "-",
        "ID_TIPO_CANDIDATURA": os.environ.get('INE_PREP2021_TIPO_CANDIDATURA_RP', 4),
        "MECANISMOS_TRASLADO": "-",
        "TIPO_DIGITALIZACION": "-",
        "ID_PROCESO_ELECTORAL": 1,
        "REPRESENTANTES_PP_CI": "42",
        "LISTA_NOMINAL_CASILLA": 0,
        "FECHA_HORA_VERIFICACION": "-",
        "REPRESENTANTES_P_VOTARON": "-",
        "asociacion_1": {
            "LOGO": "PAN.png",
            "COLOR": "#00579c",
            "ORDEN": 1,
            "VOTOS": "-",
            "candidato": " FRANCISCO JAVIER LUEVANO NU\u00d1EZ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "1",
            "TIPO_ASOCIACION": 1
        },
        "asociacion_61": {
            "LOGO": "CNR.png",
            "COLOR": "#D8BC8F",
            "ORDEN": 61,
            "VOTOS": "-",
            "candidato": "  ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "61",
            "TIPO_ASOCIACION": 8
        },
        "asociacion_62": {
            "LOGO": "VN.png",
            "COLOR": "#D1D1D1",
            "ORDEN": 62,
            "VOTOS": "-",
            "candidato": "  ",
            "ASOCIACIONES": "-",
            "ID_ASOCIACION": "62",
            "TIPO_ASOCIACION": 9
        }
    }
]

queries = {
    f"""SELECT *
                    FROM (
                        SELECT *
                            FROM PREP.C_ASOCIACIONES WHERE TIPO_ASOCIACION = 1
                        union
                        SELECT *
                            FROM PREP.C_ASOCIACIONES WHERE TIPO_ASOCIACION = 4 AND NOMBRE_ASOCIACION != 'CI'
                        UNION
                        SELECT *
                            FROM PREP.C_ASOCIACIONES WHERE TIPO_ASOCIACION NOT IN (1,4)
                    ) CANDIDATURAS
                    WHERE CANDIDATURAS.ID_ASOCIACION IN (
                        SELECT DISTINCT ID_ASOCIACION
                            FROM PREP.C_ESCENARIOS WHERE ID_TIPO_CANDIDATURA IN (%(candidaturas)s)
                        )
        ;""":
    1,
    f"""SELECT ID_ESTADO, NOMBRE_ESTADO FROM PREP.C_ESTADOS;""":
    2,
    f"""SELECT * FROM PREP.C_DISTRITOS_FEDERALES WHERE ID_DISTRITO_FEDERAL > 0;""":
    3,
    """SELECT COUNT(ID_ACTA)
        FROM PREP.TR_ACTAS WHERE ID_TIPO_CANDIDATURA IN (%(candidaturas)s);""":
    4,
    """SELECT COUNT(ID_ACTA_FUERA_CATALOGO)
            FROM PREP.TR_ACTAS_FUERA_CATALOGO
            WHERE ID_TIPO_CANDIDATURA IN (%(candidaturas)s);""":
    5,
    """
                SELECT
                ES.ID_ESTADO as ESTADO,
                DF.ID_DISTRITO_FEDERAL as DISTRITO,
                ASOC.NOMBRE_ASOCIACION as PARTIDO_CI,
                CAND.NOMBRE_PROPIETARIO as CANDIDATURA_PROPIETARIA,
                CAND.NOMBRE_SUPLENTE as CANDIDATURA_SUPLENTE
                FROM PREP.C_DISTRITOS_FEDERALES DF
                inner join PREP.C_ESTADOS ES on DF.ID_ESTADO = ES.ID_ESTADO
                inner join PREP.C_ESCENARIOS ESC on ESC.ID_DISTRITO = DF.ID_DISTRITO_FEDERAL
                and DF.ID_ESTADO = ESC.ID_ESTADO
                inner join PREP.C_CANDIDATOS CAND on ESC.ID_ESCENARIO = CAND.ID_ESCENARIO
                inner join PREP.C_ASOCIACIONES ASOC on ESC.ID_ASOCIACION = ASOC.ID_ASOCIACION
                where ESC.ID_ASOCIACION not in (61,62,63,64,65,66,67)
                and ESC.ID_TIPO_CANDIDATURA IN (%(candidaturas)s);
            """:
    6
}

queries_results = {
    1: [
        (1, 1, 'PAN', 'PAN', 'PAN.png', '#00579c', 1), (2, 1, 'PRI', 'PRI', 'PRI.png', '#15a152', 2),
        (3, 1, 'PRD', 'PRD', 'PRD.png', '#ffde00', 3), (4, 1, 'PVEM', 'PVEM', 'PVEM.png', '#87c344', 4),
        (5, 1, 'PT', 'PT', 'PT.png', '#ee3d44', 5), (6, 1, 'MC', 'MC', 'MC.png', '#fd8204', 6),
        (8, 1, 'MORENA', 'MORENA', 'MORENA.png', '#a53421', 8), (9, 1, 'PES', 'PES', 'PES.png', '#662680', 9),
        (10, 1, 'RSP', 'RSP', 'RSP.png', '#404143', 10), (11, 1, 'FS', 'FS', 'FS.png', '#e30613', 11),
        (20, 3, 'PAN-PRD-MC', 'PAN-PRD-MC', 'PAN-PRD-MC.png', '#669FF1', 20),
        (21, 6, 'PAN-PRD', 'PAN-PRD', 'PAN-PRD.png', '#669FF1', 21),
        (22, 6, 'PAN-MC', 'PAN-MC', 'PAN-MC.png', '#669FF1', 22),
        (23, 6, 'PRD-MC', 'PRD-MC', 'PRD-MC.png', '#669FF1', 23),
        (30, 3, 'PT-MORENA-PES', 'PT-MORENA-PES', 'PT-MORENA-PES.png', '#F28F8F', 40),
        (31, 6, 'PT-MORENA', 'PT-MORENA', 'PT-MORENA.png', '#F28F8F', 41),
        (32, 6, 'PT-PES', 'PT-PES', 'PT-PES.png', '#F28F8F', 42),
        (33, 6, 'MORENA-PES', 'MORENA-PES', 'MORENA-PES.png', '#F28F8F', 43),
        (40, 3, 'PRI-PVEM-RSP', 'PRI-PVEM-RSP', 'PRI-PVEM-RSP.png', '#82E0A8', 30),
        (41, 6, 'PRI-PVEM', 'PRI-PVEM', 'PRI-PVEM.png', '#82E0A8', 31),
        (42, 6, 'PRI-RSP', 'PRI-RSP', 'PRI-RSP.png', '#82E0A8', 32),
        (43, 6, 'PVEM-RSP', 'PVEM-RSP', 'PVEM-RSP.png', '#82E0A8', 33), (50, 4, 'CI', 'CI', 'CI.png', '#CA9E67', 14),
        (51, 4, 'CI_01', 'CI_01', 'CI_01.png', '#8AA89D', 12), (52, 4, 'CI_02', 'CI_02', 'CI_02.png', '#CA9E67', 13),
        (61, 8, 'CNR', 'CNR', 'CNR.png', '#D8BC8F', 61), (62, 9, 'VN', 'VN', 'VN.png', '#D1D1D1', 62),
        (63, 10, 'BOLETAS SOBRANTES', 'BS', 'pn.png', '#D1D1D1', 63),
        (64, 11, 'PERSONAS QUE VOTARON', 'PN', 'pn.png', '#D1D1D1', 64),
        (65, 12, 'REPRESENTANTES DE PARTIDOS QUE VOTARON', 'RPV', 'rpv.png', '#D1D1D1', 65),
        (66, 13, 'VOTOS ASENTADOS', 'VA', 'va.png', '#D1D1D1', 66),
        (67, 14, 'VOTOS SACADOS', 'VS', 'vs.png', '#D1D1D1', 67)
    ],
    2: [(1, 'Aguascalientes')],
    3: [(1, 1, 'Jesús María')],
    4: [(157859, )],
    5: [(157, )],
    6: [(1, 1, 'PAN', 'FRANCISCO JAVIER LUEVANO NUÑEZ', 'GUSTAVO ARMENDARIZ VIRAMONTES')],
    7: [(1, 1)]
}

queries_description = {
    6: [
        ('ESTADO', 3, None, None, None, None, 0, 20491), ('DISTRITO', 3, None, None, None, None, 0, 20483),
        ('PARTIDO_CI', 253, None, None, None, None, 0, 4097),
        ('CANDIDATURA_PROPIETARIA', 253, None, None, None, None, 1, 0),
        ('CANDIDATURA_SUPLENTE', 253, None, None, None, None, 1, 0)
    ]
}


def dic_to_bytes(data):
    data = json.dumps(data)
    gz_body = BytesIO()
    gz = GzipFile(None, 'wb', 5, gz_body)
    gz.write(data.encode('utf-8'))
    gz.close()
    data = gz_body.getvalue()
    return data


class testCursor:

    def __init__(self, **kwargs):
        self.query = 0
        self.description = ''

    def close(self):
        pass

    def execute(self, query, valores):
        print(query)
        self.query = queries.get(query, 7)

    def fetchall(self):
        self.description = queries_description.get(self.query, '')
        return queries_results.get(self.query)


class testSQL:

    def __init__(self, **kwargs):
        pass

    def connect(self, *args, **kwargs):
        return testSQL()

    def cursor(self, *args, **kwargs):
        return testCursor()

    def close(self, *args, **kwargs):
        pass


class testS3:

    def __init__(self, *args, **kwargs):
        self.acta_rp = kwargs.get('acta_rp', False)

    def read(self):
        return dic_to_bytes(acta) if not self.acta_rp else dic_to_bytes(acta_rp)

    def list_objects_v2(self, **kwargs):
        return {'Contents': {}}

    def get_object(self, **kwargs):
        if 'corte_rp' in kwargs.get('Key'):
            self.acta_rp = True
        return {'Body': testS3(acta_rp=self.acta_rp)}

    def upload_file(self, *args, **kwargs):
        pass


class testBoto:

    def __init__(self, **params):
        pass

    def client(self, name, **kwargs):
        if name == 's3':
            return testS3()


def test_CrearCSV():
    # Config(sqlconnect='')
    ejs = CrearCSV(
        repositorio=RepoQueries(objconexion=Config(sqlconnect=testSQL()), sqlcredenciales={}), boto=testBoto()
    )
    ejs.crear_csv()
