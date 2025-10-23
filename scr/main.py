"""
Script principal del ETL de Airbnb.
Orquesta el proceso completo: Extracción -> Transformación -> Carga.
"""
import os
import sys
from datetime import datetime

# Importar módulos del ETL
from extraccion import Extraccion, Logs as LogsExtraccion
from transformacion import Transformacion
from carga import Carga


def crear_directorios():
    """Crea los directorios necesarios si no existen."""
    os.makedirs("logs", exist_ok=True)
    os.makedirs("scr/output", exist_ok=True)


def ejecutar_etl():
    """Ejecuta el proceso ETL completo."""
    logger = LogsExtraccion()
    
    try:
        logger.info("="*60)
        logger.info("INICIANDO PROCESO ETL DE AIRBNB")
        logger.info("="*60)
        
        # Crear directorios necesarios
        crear_directorios()
        logger.info("Directorios de trabajo creados/verificados")
        
        # ===== EXTRACCIÓN =====
        logger.info("\n--- FASE 1: EXTRACCIÓN ---")
        
        extractor = Extraccion(uri="mongodb://localhost:27017", db_name="bi_mx")
        
        if not extractor.conectar():
            logger.error("No se pudo conectar a MongoDB. Abortando ETL.")
            return False
        
        # Extraer colecciones
        df_listings = extractor.obtener_listings()
        df_reviews = extractor.obtener_reviews()
        df_calendar = extractor.obtener_calendar()
        
        extractor.cerrar_conexion()
        
        logger.info(f"Extracción completada:")
        logger.info(f"  - Listings: {len(df_listings)} registros")
        logger.info(f"  - Reviews: {len(df_reviews)} registros")
        logger.info(f"  - Calendar: {len(df_calendar)} registros")
        
        # ===== TRANSFORMACIÓN =====
        logger.info("\n--- FASE 2: TRANSFORMACIÓN ---")
        
        transformer = Transformacion()
        
        df_listings_trans = transformer.transformar_listings(df_listings)
        df_reviews_trans = transformer.transformar_reviews(df_reviews)
        df_calendar_trans = transformer.transformar_calendar(df_calendar)
        
        logger.info(f"Transformación completada:")
        logger.info(f"  - Listings: {len(df_listings_trans)} registros procesados")
        logger.info(f"  - Reviews: {len(df_reviews_trans)} registros procesados")
        logger.info(f"  - Calendar: {len(df_calendar_trans)} registros procesados")
        
        # ===== CARGA =====
        logger.info("\n--- FASE 3: CARGA ---")
        
        cargador = Carga(output_dir="scr/output")
        
        resultados = cargador.cargar_todos(
            df_listings_trans,
            df_reviews_trans,
            df_calendar_trans
        )
        
        archivos_exitosos = sum(resultados.values())
        logger.info(f"Carga completada: {archivos_exitosos}/3 archivos guardados")
        
        # ===== FINALIZACIÓN =====
        logger.info("\n" + "="*60)
        logger.info("PROCESO ETL COMPLETADO EXITOSAMENTE")
        logger.info("="*60)
        
        return True
        
    except KeyboardInterrupt:
        logger.warning("Proceso ETL interrumpido por el usuario")
        return False
        
    except Exception as e:
        logger.error(f"Error crítico en el proceso ETL: {str(e)}")
        logger.error(f"Tipo de error: {type(e).__name__}")
        return False


if __name__ == "__main__":
    print("="*60)
    print("ETL DE AIRBNB - INICIO")
    print("="*60)
    print(f"Fecha y hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    exito = ejecutar_etl()
    
    print()
    print("="*60)
    if exito:
        print("ETL FINALIZADO CON ÉXITO")
        print("Revise el directorio 'scr/output/' para los archivos generados")
        print("Revise el directorio 'logs/' para el registro detallado")
    else:
        print("ETL FINALIZADO CON ERRORES")
        print("Revise el directorio 'logs/' para más detalles")
    print("="*60)