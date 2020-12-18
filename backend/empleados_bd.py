import pymysql
import sys

from backend.modulos.rutas import unir_cadenas


class BdatosEmpleados:
    def __init__(self, usuario, psword, nombre_bd, tabla):
        self.host = ''
        self.usuario = usuario
        self.psw = psword
        self.nombre_bd = nombre_bd
        self.conexion = pymysql.connect(
            self.host, self.usuario, self.psw, self.nombre_bd)
        self.cursor = self.conexion.cursor()
        self.tabla = tabla

    def crear_tabla(self, campos, campo_primario, campos_unicos):
        """Crea Tabla los campos deben ser pasados en cadena
                ejemplplo: id INT AUTO_INCREMENT,control INT(8) NOT NULL,"""

        orden = "CREATE TABLE {}.{}({} PRIMARY KEY({}), UNIQUE KEY({}))".format(self.nombre_bd, self.tabla,
                                                                                campos, campo_primario, campos_unicos)

        self.cursor.execute(orden)
        self.conexion.commit()

    def insertar_filas(self, campos, datos):
        log_guardado = list()
        try:
            tabla = "SELECT 1 FROM {} LIMIT 1".format(self.tabla)
            self.cursor.execute(tabla)

        except pymysql.err.ProgrammingError:
            print("No existe la tabla")
            pass

        try:
            orden = "INSERT INTO {}({}) \
			VALUES({})".format(self.tabla, campos, datos)
            self.cursor.execute(orden)
            self.conexion.commit()
            log_guardado.append('EXITOSO:' + datos)

        except pymysql.err.IntegrityError:
            log_guardado.append('ERROR YA EXISTE REGISTRO:' + datos)
            print("WARNING:El Registro {} No se Pudo Guardar por que ya existe".format(
                datos.split(',')))

        except:
            print("ERROR INESPERADO:", sys.exc_info()[0])
            log_guardado.append('ERROR:'+str(sys.exc_info()[0]) + datos)
            

        self.conexion.close()
        return log_guardado

    def consultar(self, campos, condiciones):
        try:
            orden = "SELECT {} FROM {}.{}\
			INNER JOIN {}.{} {}".format(campos, self.nombre_bd, self.tabla, self.nombre_bd, "recibos", condiciones)

            self.cursor.execute(orden)
            registro = self.cursor.fetchall()
            return registro

        except pymysql.err.ProgrammingError:
            print("No existen datos de este año")
            pass


def ejecutar_acciones_emple(datos_peticion):

    # conexion.send(datos_dev.encode())
    tipo_accion = datos_peticion[0].split(":")
    conexion_datos = datos_peticion[2].split(",")
    if conexion_datos[1] == ' ':
        conexion_datos[1] = ''

    if tipo_accion[0] == 'INSERTAR':
        campos = tipo_accion[1]
        datos = datos_peticion[1]

        base_datos = BdatosEmpleados(conexion_datos[0], conexion_datos[1],
                                     conexion_datos[2], conexion_datos[3])
        log = base_datos.insertar_filas(campos, datos)

        if log:
            errores_format = formatear_datos(log)
            return errores_format
        else:
            return 'SIN ERRORES DE GUARDADO'

    if tipo_accion[0] == 'CONSULTAR':
        campos = tipo_accion[1]
        condiciones = datos_peticion[1]
        base_datos = BdatosEmpleados(conexion_datos[0],
                                     conexion_datos[1], conexion_datos[2], conexion_datos[3])
        registros = base_datos.consultar(campos, condiciones)
        registros = list(registros)

        datos_lista = list()
        registros_str = list()
        for registro in registros:
            for dato in registro:
                datos_lista.append(str(dato))

            datos = unir_cadenas('|', datos_lista)
            datos_lista.clear()
            registros_str.append(datos)

        registros = unir_cadenas(',', registros_str)
        return registros


def formatear_datos(lista_datos):
    datos = unir_cadenas('|', lista_datos)

    return datos
