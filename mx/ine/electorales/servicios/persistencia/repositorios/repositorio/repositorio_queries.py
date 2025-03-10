import os
import json
import boto3
from ...config.configuracion import Config
from . import lista_queries as lq


class RepoQueries():
    actas_mr = os.environ.get('TIPO_CANDIDATURA_MR')
    actas_esp = os.environ.get('TIPO_CANDIDATURA_ESP')
    actas_ve = os.environ.get('TIPO_CANDIDATURA_VE')

    LAMBDA = boto3.client('lambda')

    def __init__(self, **params):
        self.objconexion = params.get('objconexion', Config())
        self.corte_nayarit = params.get('corte_nayarit', False)
        self.credenciales = params.get('sqlcredenciales')

    def obtiene_redis(self, llave, valor):
        respuesta = self.LAMBDA.invoke(
            FunctionName=os.environ.get('LAMBDA_GET_EC'),
            InvocationType='RequestResponse', Payload=json.dumps({'llave': llave, 'valor': valor})
        )
        respuesta = json.loads(respuesta.get('Payload').read())
        return respuesta

    def ejecuta_query(self, query, valores=()):
        conexion = self.objconexion.obtiene_conexion(self.credenciales)
        cursor = conexion.cursor()
        cursor.execute(query, valores)
        r = cursor.fetchall()
        d = cursor.description
        cursor.close()
        conexion.close()
        return r, d

    def concatena_candidaturas(self):
        candidaturas = [self.actas_mr, self.actas_esp, self.actas_ve]
        return candidaturas

    def obtener_asociaciones(self):
        return self.obtiene_redis('csv', 'asociaciones')

    def obtener_nombre_estado(self):
        return self.obtiene_redis('csv', 'nombre_estado')

    def obtener_cabecera_distrital(self):
        return self.obtiene_redis('csv', 'cabecera_distrital')

    def obtener_actas_esperadas(self):
        return self.obtiene_redis('csv', 'actas_esperadas')

    def obtener_actas_fuera_catalogo(self):
        candidaturas_lista = self.concatena_candidaturas()
        query = lq.fuera_cat_3cand if not self.corte_nayarit else lq.fuera_cat_2and
        r, _ = self.ejecuta_query(query, valores=candidaturas_lista)
        return r

    def obtener_info_actas_fuera_catalogo(self):
        query = lq.info_fuera_cat
        r, d = self.ejecuta_query(query, valores=())
        columnas = [x[0] for x in d]
        return {columnas[c]: [row[c] for row in r] for c in range(len(columnas))}

    def obtener_info_voto_extranjero(self):
        query = lq.info_voto_extranjero
        r, d = self.ejecuta_query(query, valores=())
        columnas = [x[0] for x in d]
        return {columnas[c]: [row[c] for row in r] for c in range(len(columnas))}

    def obtiene_candidaturas(self):
        candidaturas = self.obtiene_redis('csv', 'cat_candidatos')
        return candidaturas['respuesta'], candidaturas['descripcion']

    def obtener_estados_distritos(self):
        return self.obtiene_redis('csv', 'estados_distritos')
