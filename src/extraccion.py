"""Módulo de extracción para cargar colecciones de MongoDB en DataFrames."""

from typing import Optional

import pandas as pd
from pandas import DataFrame
from pymongo import MongoClient
from pymongo.errors import PyMongoError

from logs_manage import Logs


class Extraccion:
    """
    Gestiona la extracción de datos desde MongoDB.
    Conecta a la base de datos bi_mx y extrae las colecciones: listings, reviews, calendar.
    """

    def __init__(self, uri: str, db_name: str, logger: Optional[Logs] = None) -> None:
        """
        Inicializar extractor de datos.
        
        Args:
            uri: URI de conexión a MongoDB
            db_name: Nombre de la base de datos
        """
        self.uri = uri
        self.db_name = db_name
        self.logger = (
            logger.crear_sublogger(header="LOG DE EXTRACCIÓN - AIRBNB CDMX")
            if logger
            else Logs(header="LOG DE EXTRACCIÓN - AIRBNB CDMX")
        )
        self._client: Optional[MongoClient] = None
        self._db = None
        
        self.logger.info("Extractor inicializado")
        self.logger.info(f"URI: {self.uri}")
        self.logger.info(f"Base de datos: {self.db_name}")

    def conectar(self) -> bool:
        """
        Establecer conexión con la base de datos MongoDB.
        
        Returns:
            True si la conexión fue exitosa, False en caso contrario
        """
        try:
            self.logger.nueva_entrada("Estableciendo conexión a MongoDB")
            self.logger.info(f"Conectando a: {self.uri}")
            
            # Crear cliente MongoDB con timeout
            self._client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            
            # Verificar conexión con ping
            self._client.admin.command("ping")
            
            # Obtener referencia a la base de datos
            self._db = self._client[self.db_name]
            
            self.logger.info(f"✓ Conexión establecida exitosamente con '{self.db_name}'")
            return True
            
        except PyMongoError as e:
            self.logger.error(f"✗ Error al conectar con MongoDB: {str(e)}")
            return False
        except Exception as e:
            self.logger.error(f"✗ Error inesperado al conectar: {str(e)}")
            return False

    def _extraer_coleccion(self, nombre_coleccion: str) -> DataFrame:
        """
        Método privado para extraer una colección y convertirla en DataFrame.
        
        Args:
            nombre_coleccion: Nombre de la colección a extraer
            
        Returns:
            DataFrame con los datos de la colección
        """
        try:
            self.logger.info(f"\n--- EXTRAYENDO COLECCIÓN: {nombre_coleccion} ---")
            
            # Extraer documentos de la colección
            coleccion = self._db[nombre_coleccion]
            documentos = list(coleccion.find())
            
            # Convertir a DataFrame
            df = pd.DataFrame(documentos)
            
            # Eliminar _id de MongoDB si existe
            if not df.empty and "_id" in df.columns:
                df = df.drop(columns="_id")
            
            # Registrar resultados
            cantidad = len(df)
            self.logger.info(f"✓ Colección '{nombre_coleccion}' extraída: {cantidad:,} registros")
            
            if df.empty:
                self.logger.warning(f"⚠️  La colección '{nombre_coleccion}' está vacía")
            
            return df
            
        except PyMongoError as e:
            self.logger.error(f"✗ Error al extraer '{nombre_coleccion}': {str(e)}")
            raise
        except Exception as e:
            self.logger.error(f"✗ Error inesperado en '{nombre_coleccion}': {str(e)}")
            raise

    def extraer_todas(self) -> tuple[DataFrame, DataFrame, DataFrame]:
        """
        Extraer todas las colecciones (listings, reviews, calendar) en una sola operación.
        Maneja automáticamente la conexión y cierre.
        
        Returns:
            Tupla con (df_listings, df_reviews, df_calendar)
        """
        self.logger.nueva_entrada("Inicio de extracción completa")
        
        try:
            # Conectar a la base de datos
            if not self.conectar():
                raise RuntimeError("No se pudo establecer conexión con MongoDB")
            
            # Extraer cada colección usando el método privado
            df_listings = self._extraer_coleccion("listings")
            df_reviews = self._extraer_coleccion("reviews")
            df_calendar = self._extraer_coleccion("calendar")
            
            # Registrar resumen
            total = len(df_listings) + len(df_reviews) + len(df_calendar)
            self.logger.nueva_entrada("Extracción completada exitosamente")
            self.logger.info(f"Total de registros extraídos: {total:,}")
            self.logger.info(f"  - Listings: {len(df_listings):,}")
            self.logger.info(f"  - Reviews: {len(df_reviews):,}")
            self.logger.info(f"  - Calendar: {len(df_calendar):,}")
            
            return df_listings, df_reviews, df_calendar
            
        except Exception as e:
            self.logger.error(f"\n✗ Error en la extracción completa: {str(e)}")
            raise
        
        finally:
            # Siempre cerrar la conexión
            if self._client is not None:
                self._client.close()
                self.logger.info("\n✓ Conexión a MongoDB cerrada correctamente")
                self._client = None
                self._db = None