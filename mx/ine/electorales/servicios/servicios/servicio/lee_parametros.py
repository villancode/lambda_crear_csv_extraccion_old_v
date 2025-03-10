import os
import boto3
import botocore.config
from datetime import datetime
import dateutil.tz
from .secreto import obtiene_secreto

secreto = obtiene_secreto()
corte_inicio = datetime.now(tz=dateutil.tz.gettz('America/Mexico_City'))


def formato(dtm):
    return dtm.strftime('%Y-%m-%d %H:%M:%S.%f')


def define_parametros(**params):

    params = params.get('body', params)

    params_corte = {
        'ruta_info_corte': params.get('ruta_info_corte', 'informacion_corte/'),
        'nombre_archivo_corte': params.get('nombre_archivo_corte', 'corte_edo_%s_dto_%s.json'),
        'nombre_archivo_corte_rp': params.get('nombre_archivo_corte_rp', 'corte_rp_edo_%s_dto_%s.json')
    }

    params_hora = {'fecha_corte_inicio': params.get('fecha_corte_inicio', formato(corte_inicio))}

    params_bucket = {
        'corte_nayarit':
        params.get('corte_nayarit', False),
        'ultimo_corte':
        params.get('ultimo_corte', False),
        'bucket':
        params.get('bucket', os.environ.get('BUCKET_EXTRACT')),
        'boto':
        params.get('boto', boto3),
        'sqlcredenciales':
        params.get(
            'sqlcredenciales', {
                'host': secreto.get('host'),
                'user': secreto.get('username'),
                'passwd': secreto.get('password'),
                'database': secreto.get('dbname'),
                'port': secreto.get('port')
            }
        )
    }
    params_csv = {
        'tmp_archivo_candidatura': params.get('tmp_archivo_candidatura', 'tmp_archivo_presidencia.csv'),
        'tmp_csv_base': params.get('tmp_csv_base', 'base_diputaciones.csv'),
        'tmp_info_candidaturas': params.get('tmp_info_candidaturas', 'info_senadurias.csv'),
        'ruta_bitacora_csv': params.get('ruta_bitacora_csv', 'bitacoradif/csv/'),
        'archivo_bitacora_info_candidatura': params.get('archivo_bitacora_info_candidatura', 'PRES_FED_2024.csv'),
        'archivo_bitacora_candidaturas': params.get('archivo_bitacora_candidaturas', 'PRES_FED_Candidaturas_2024.csv')
    }

    params = {**params, **params_corte, **params_bucket, **params_hora, **params_csv}

    params['s3'] = params['boto'].client(
        's3', **params.get('botocredenciales', {}), config=botocore.client.Config(max_pool_connections=600)
    )

    fecha_header = datetime.strptime(params['fecha_corte_inicio'], '%Y-%m-%d %H:%M:%S.%f')
    params['fecha_header'] = fecha_header.strftime('%d/%m/%Y %H:%M')

    return params
