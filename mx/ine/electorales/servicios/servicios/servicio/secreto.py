import json
from botocore.exceptions import ClientError
from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import boto3
import os


def obtiene_secreto(variable='DB_SECRET'):
    client = boto3.client(service_name='secretsmanager')
    secret_name = os.environ.get(variable)

    try:
        cache = SecretCache(SecretCacheConfig(), client)
        secreto = cache.get_secret_string(secret_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            raise e
    else:
        return json.loads(secreto)
