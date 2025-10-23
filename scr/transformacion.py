"""
Módulo de transformación de datos.
Aplica transformaciones simples a los DataFrames extraídos.
"""
import os
from datetime import datetime
import pandas as pd


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


class Transformacion:
    """Clase para aplicar transformaciones a los DataFrames."""
    
    def __init__(self):
        """Inicializa el módulo de transformación."""
        self.logger = Logs()
        self.logger.info("Inicializando módulo de transformación")
    
    def transformar_listings(self, df):
        """
        Transforma el DataFrame de Listings.
        Selecciona columnas clave y normaliza tipos de datos.
        
        Args:
            df: DataFrame de Listings
            
        Returns:
            DataFrame transformado
        """
        try:
            self.logger.info(f"Iniciando transformación de Listings - Filas de entrada: {len(df)}")
            
            if df.empty:
                self.logger.warning("DataFrame de Listings vacío, no hay datos para transformar")
                return df
            
            # Definir columnas clave a mantener (si existen)
            columnas_clave = ['id', 'name', 'host_id', 'host_name', 'neighbourhood', 
                            'latitude', 'longitude', 'room_type', 'price', 
                            'minimum_nights', 'number_of_reviews', 'availability_365']
            
            # Filtrar solo las columnas que existen en el DataFrame
            columnas_existentes = [col for col in columnas_clave if col in df.columns]
            
            if columnas_existentes:
                df_transformado = df[columnas_existentes].copy()
            else:
                self.logger.warning("Ninguna columna clave encontrada, se mantienen todas las columnas")
                df_transformado = df.copy()
            
            # Normalizar tipos de datos
            if 'price' in df_transformado.columns:
                df_transformado['price'] = pd.to_numeric(df_transformado['price'], errors='coerce')
            
            if 'minimum_nights' in df_transformado.columns:
                df_transformado['minimum_nights'] = pd.to_numeric(df_transformado['minimum_nights'], errors='coerce')
            
            # Eliminar filas con valores nulos en columnas críticas
            if 'id' in df_transformado.columns:
                df_transformado = df_transformado.dropna(subset=['id'])
            
            self.logger.info(f"Transformación de Listings completada - Filas de salida: {len(df_transformado)}")
            return df_transformado
            
        except Exception as e:
            self.logger.error(f"Error en transformación de Listings: {str(e)}")
            return df
    
    def transformar_reviews(self, df):
        """
        Transforma el DataFrame de Reviews.
        Selecciona columnas clave y normaliza fechas.
        
        Args:
            df: DataFrame de Reviews
            
        Returns:
            DataFrame transformado
        """
        try:
            self.logger.info(f"Iniciando transformación de Reviews - Filas de entrada: {len(df)}")
            
            if df.empty:
                self.logger.warning("DataFrame de Reviews vacío, no hay datos para transformar")
                return df
            
            # Definir columnas clave a mantener (si existen)
            columnas_clave = ['listing_id', 'id', 'date', 'reviewer_id', 
                            'reviewer_name', 'comments']
            
            # Filtrar solo las columnas que existen en el DataFrame
            columnas_existentes = [col for col in columnas_clave if col in df.columns]
            
            if columnas_existentes:
                df_transformado = df[columnas_existentes].copy()
            else:
                self.logger.warning("Ninguna columna clave encontrada, se mantienen todas las columnas")
                df_transformado = df.copy()
            
            # Normalizar fechas
            if 'date' in df_transformado.columns:
                df_transformado['date'] = pd.to_datetime(df_transformado['date'], errors='coerce')
            
            # Eliminar filas con valores nulos en columnas críticas
            if 'id' in df_transformado.columns:
                df_transformado = df_transformado.dropna(subset=['id'])
            
            self.logger.info(f"Transformación de Reviews completada - Filas de salida: {len(df_transformado)}")
            return df_transformado
            
        except Exception as e:
            self.logger.error(f"Error en transformación de Reviews: {str(e)}")
            return df
    
    def transformar_calendar(self, df):
        """
        Transforma el DataFrame de calendar.
        Normaliza fechas y precios.
        
        Args:
            df: DataFrame de calendar
            
        Returns:
            DataFrame transformado
        """
        try:
            self.logger.info(f"Iniciando transformación de calendar - Filas de entrada: {len(df)}")
            
            if df.empty:
                self.logger.warning("DataFrame de calendar vacío, no hay datos para transformar")
                return df
            
            # Definir columnas clave a mantener (si existen)
            columnas_clave = ['listing_id', 'date', 'available', 'price', 
                            'minimum_nights', 'maximum_nights']
            
            # Filtrar solo las columnas que existen en el DataFrame
            columnas_existentes = [col for col in columnas_clave if col in df.columns]
            
            if columnas_existentes:
                df_transformado = df[columnas_existentes].copy()
            else:
                self.logger.warning("Ninguna columna clave encontrada, se mantienen todas las columnas")
                df_transformado = df.copy()
            
            # Normalizar fechas
            if 'date' in df_transformado.columns:
                df_transformado['date'] = pd.to_datetime(df_transformado['date'], errors='coerce')
            
            # Normalizar precios (remover símbolos de moneda y convertir a numérico)
            if 'price' in df_transformado.columns:
                if df_transformado['price'].dtype == 'object':
                    df_transformado['price'] = df_transformado['price'].str.replace('$', '', regex=False)
                    df_transformado['price'] = df_transformado['price'].str.replace(',', '', regex=False)
                df_transformado['price'] = pd.to_numeric(df_transformado['price'], errors='coerce')
            
            # Convertir available a booleano
            if 'available' in df_transformado.columns:
                df_transformado['available'] = df_transformado['available'].map({'t': True, 'f': False})
            
            self.logger.info(f"Transformación de calendar completada - Filas de salida: {len(df_transformado)}")
            return df_transformado
            
        except Exception as e:
            self.logger.error(f"Error en transformación de calendar: {str(e)}")
            return df


if __name__ == "__main__":
    # Prueba del módulo
    transformer = Transformacion()
    
    # Crear DataFrames de prueba
    df_test = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'price': ['100', '200', '300']
    })
    
    df_result = transformer.transformar_listings(df_test)
    print(f"Transformación completada: {len(df_result)} filas")

    