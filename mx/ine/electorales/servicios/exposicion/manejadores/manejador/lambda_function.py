from ....servicios.servicio.crear_csv_servicio import CrearCSV


def lambda_handler(event, context):
    ccsv = CrearCSV(**event)
    ccsv.crear_csv()
    print(context)
    return "Base creada"
