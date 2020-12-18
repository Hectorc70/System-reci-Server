import pymysql


class Bdatos:
	def __init__(self, host, usuario, psword, nombre_bd, tabla):       
		self.host    = host
		self.usuario = usuario
		self.psw     = psword
		self.nombre_bd = nombre_bd
		self.conexion = pymysql.connect(self.host, self.usuario, self.psw, self.nombre_bd)
		self.cursor    = self.conexion.cursor()	
		self.tabla = tabla
	
	def crear_tabla(self, campos, campo_primario, campos_unicos):

		"""Crea Tabla los campos deben ser pasados en cadena
			ejemplplo: id INT AUTO_INCREMENT,control INT(8) NOT NULL,"""
		
		orden = "CREATE TABLE {}.{}({} PRIMARY KEY({}), UNIQUE KEY({}))".format(self.nombre_bd, self.tabla,
																					campos, campo_primario, campos_unicos)


		self.cursor.execute(orden)
		self.conexion.commit()
		

	def insertar_filas(self, campos, datos):
		errores_guardado = list()
		try:
			tabla = "SELECT 1 FROM {} LIMIT 1".format(self.tabla)
			self.cursor.execute(tabla)

		except pymysql.err.ProgrammingError:
			print("No existe la tabla")
			return False	
		
		
		try:
			orden = "INSERT INTO {}({}) \
			VALUES({})".format(self.tabla, campos, datos) 
			self.cursor.execute(orden)
			self.conexion.commit()			
				
		except pymysql.err.IntegrityError:
			errores_guardado.append(datos)
			print("WARNING:El Registro {} No se Pudo Guardar".format(datos.split(',')))
		

		
				
		self.conexion.close()
		return errores_guardado	
			
			
	
	def consultar(self, campos, condiciones):
		try:
			orden = "SELECT {} FROM {}.{}\
			INNER JOIN {}.{} {}".format(campos, self.nombre_bd, self.tabla, self.nombre_bd,"recibos",condiciones)
				
			self.cursor.execute(orden)
			registro = self.cursor.fetchall()
			return registro
			
		except pymysql.err.ProgrammingError:
			print("No existen datos de este año")		
			pass
		
	
		
	

		
