import pymysql.cursors


class Config():

    def __init__(self, **params):
        self.sqlconnect = params.get('sqlconnect', pymysql)

    def obtiene_conexion(self, valor):
        try:
            mydb = self.sqlconnect.connect(**valor)
            return mydb
        except Exception as e:
            print("error al conectar")
            raise e
