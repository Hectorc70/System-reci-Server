
import socket
import sys
import time

from backend.bdatos import Bdatos
from backend.modulos.rutas import unir_cadenas

from backend.recibos_bd import ejecutar_acciones_reci

from backend.empleados_bd import ejecutar_acciones_emple


class Servidor:

    def __init__(self):
        self.host = ''
        self.puerto = 3306
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.bind((self.host, self.puerto))
        self.server.listen(10)

    def aceptar_conexion(self):
        print('SERVIDOR INICIADO...''\n''Esperando CONEXIONES...')
        try:
                while  True:
                        self.conexion, self.addr = self.server.accept()
                        print('NUEVA conexion Extablecida: {}'.format(self.addr[0]))
                        peticion = self.conexion.recv(1024).decode()

                        datos = peticion.split('|')				
                        self.__verificar_tabla(datos)


        except:
                print('---------ERROR RENICIANDO SERVIDOR -------------')			
                
                self.aceptar_conexion() 

      
    def __verificar_tabla(self, datos):
        datos_conex = datos[2].split(",")
        tabla = datos_conex[-1]
        if tabla == 'recibos':
            registro = ejecutar_acciones_reci(datos)

            self.conexion.send(registro.encode())

            self.terminar_conexion()
        elif tabla == 'empleados':
            registro = ejecutar_acciones_emple(datos)
            self.conexion.send(registro.encode())
            self.terminar_conexion()

    def terminar_conexion(self):
        self.conexion.close()


