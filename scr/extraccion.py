"""
Módulo de extracción de datos desde MongoDB.
Extrae las colecciones Listings, Reviews y calendar.
"""
import os
from datetime import datetime
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError


class Logs:
    """Clase para gestionar el logging de eventos del sistema."""
    
    def __init__(self):
        """Inicializa el logger y crea el archivo de log."""
        self.log_dir = "logs"
        os.makedirs(self.log_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        self.log_file = os.path.join(self.log_dir, f"log_{timestamp}.txt")
        
        self._write_log("INFO", "Sistema de logging inicializado")
    
    def _write_log(self, level, message):
        """Escribe un mensaje en el archivo de log."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)
    
    def info(self, message):
        """Registra un mensaje de nivel INFO."""
        self._write_log("INFO", message)
    
    def warning(self, message):
        """Registra un mensaje de nivel WARNING."""
        self._write_log("WARNING", message)
    
    def error(self, message):
        """Registra un mensaje de nivel ERROR."""
        self._write_log("ERROR", message)


class Extraccion:
    """Clase para extraer datos desde MongoDB."""
    
    def __init__(self, uri="mongodb://localhost:27017", db_name="bi_mx"):
        """
        Inicializa la conexión a MongoDB.
        
        Args:
            uri: URI de conexión a MongoDB
            db_name: Nombre de la base de datos
        """
        self.logger = Logs()
        self.uri = uri
        self.db_name = db_name
        self.client = None
        self.db = None
        
        self.logger.info("Inicializando módulo de extracción")
    
    def conectar(self):
        """Establece la conexión con MongoDB."""
        try:
            self.logger.info(f"Intentando conectar a MongoDB: {self.uri}")
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión
            self.client.server_info()
            
            self.db = self.client[self.db_name]
            self.logger.info(f"Conexión exitosa a la base de datos: {self.db_name}")
            return True
            
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            self.logger.error(f"Error al conectar a MongoDB: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"Error inesperado en conexión: {str(e)}")
            return False
    
    def _extraer_coleccion(self, nombre_coleccion):
        """
        Extrae una colección completa y la convierte a DataFrame.
        
        Args:
            nombre_coleccion: Nombre de la colección a extraer
            
        Returns:
            DataFrame con los datos de la colección
        """
        try:
            if self.db is None:
                self.logger.error("No hay conexión activa a la base de datos")
                return pd.DataFrame()
            
            # Verificar si existe la colección
            if nombre_coleccion not in self.db.list_collection_names():
                self.logger.warning(f"La colección '{nombre_coleccion}' no existe en la base de datos")
                return pd.DataFrame()
            
            self.logger.info(f"Extrayendo colección: {nombre_coleccion}")
            
            coleccion = self.db[nombre_coleccion]
            documentos = list(coleccion.find())
            
            if not documentos:
                self.logger.warning(f"La colección '{nombre_coleccion}' está vacía")
                return pd.DataFrame()
            
            df = pd.DataFrame(documentos)
            
            # Eliminar el campo _id de MongoDB si existe
            if '_id' in df.columns:
                df = df.drop('_id', axis=1)
            
            self.logger.info(f"Colección '{nombre_coleccion}' extraída: {len(df)} documentos")
            return df
            
        except Exception as e:
            self.logger.error(f"Error al extraer colección '{nombre_coleccion}': {str(e)}")
            return pd.DataFrame()
    
    def obtener_listings(self):
        """
        Extrae la colección Listings.
        
        Returns:
            DataFrame con los datos de Listings
        """
        return self._extraer_coleccion("Listings")
    
    def obtener_reviews(self):
        """
        Extrae la colección Reviews.
        
        Returns:
            DataFrame con los datos de Reviews
        """
        return self._extraer_coleccion("Reviews")
    
    def obtener_calendar(self):
        """
        Extrae la colección calendar.
        
        Returns:
            DataFrame con los datos de calendar
        """
        return self._extraer_coleccion("calendar")
    
    def cerrar_conexion(self):
        """Cierra la conexión a MongoDB."""
        if self.client:
            self.client.close()
            self.logger.info("Conexión a MongoDB cerrada")


if __name__ == "__main__":
    # Prueba del módulo
    extractor = Extraccion()
    
    if extractor.conectar():
        df_listings = extractor.obtener_listings()
        df_reviews = extractor.obtener_reviews()
        df_calendar = extractor.obtener_calendar()
        
        print(f"Listings: {len(df_listings)} registros")
        print(f"Reviews: {len(df_reviews)} registros")
        print(f"Calendar: {len(df_calendar)} registros")
        
        extractor.cerrar_conexion()
    

