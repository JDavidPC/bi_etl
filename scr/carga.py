"""
Módulo de carga de datos.
Guarda los DataFrames transformados en archivos parquet.
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


class Carga:
    """Clase para cargar los DataFrames transformados a archivos."""
    
    def __init__(self, output_dir="scr/output"):
        """
        Inicializa el módulo de carga.
        
        Args:
            output_dir: Directorio donde se guardarán los archivos
        """
        self.logger = Logs()
        self.output_dir = output_dir
        
        # Crear directorio de salida si no existe
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.logger.info(f"Inicializando módulo de carga - Directorio: {self.output_dir}")
    
    def _guardar_parquet(self, df, nombre_archivo):
        """
        Guarda un DataFrame en formato parquet.
        
        Args:
            df: DataFrame a guardar
            nombre_archivo: Nombre del archivo (sin extensión)
        """
        try:
            if df.empty:
                self.logger.warning(f"DataFrame '{nombre_archivo}' está vacío, no se guardará")
                return False
            
            ruta_completa = os.path.join(self.output_dir, f"{nombre_archivo}.parquet")
            
            self.logger.info(f"Guardando {nombre_archivo}.parquet - {len(df)} filas")
            
            df.to_parquet(ruta_completa, index=False, engine='pyarrow')
            
            self.logger.info(f"Archivo guardado exitosamente: {ruta_completa}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error al guardar {nombre_archivo}.parquet: {str(e)}")
            return False
    
    def cargar_listings(self, df):
        """
        Guarda el DataFrame de Listings.
        
        Args:
            df: DataFrame de Listings transformado
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        return self._guardar_parquet(df, "listings")
    
    def cargar_reviews(self, df):
        """
        Guarda el DataFrame de Reviews.
        
        Args:
            df: DataFrame de Reviews transformado
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        return self._guardar_parquet(df, "reviews")
    
    def cargar_calendar(self, df):
        """
        Guarda el DataFrame de calendar.
        
        Args:
            df: DataFrame de calendar transformado
            
        Returns:
            True si se guardó correctamente, False en caso contrario
        """
        return self._guardar_parquet(df, "calendar")
    
    def cargar_todos(self, df_listings, df_reviews, df_calendar):
        """
        Guarda todos los DataFrames.
        
        Args:
            df_listings: DataFrame de Listings
            df_reviews: DataFrame de Reviews
            df_calendar: DataFrame de calendar
            
        Returns:
            Diccionario con el estado de cada carga
        """
        resultados = {
            'listings': self.cargar_listings(df_listings),
            'reviews': self.cargar_reviews(df_reviews),
            'calendar': self.cargar_calendar(df_calendar)
        }
        
        exitosos = sum(resultados.values())
        self.logger.info(f"Carga completada: {exitosos}/3 archivos guardados exitosamente")
        
        return resultados


if __name__ == "__main__":
    # Prueba del módulo
    cargador = Carga()
    
    # Crear DataFrame de prueba
    df_test = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [100, 200, 300]
    })
    
    resultado = cargador.cargar_listings(df_test)
    print(f"Carga completada: {resultado}")
