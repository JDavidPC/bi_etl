"""Script principal para ejecutar el pipeline ETL de Airbnb CDMX."""
from __future__ import annotations

from logs_manage import Logs
from extraccion import Extraccion
from transformacion import Transformacion
from carga import Carga


def run_pipeline() -> None:
    """Orquesta la extracción, transformación y carga de datos."""
    logger = Logs(header="LOG PIPELINE - AIRBNB CDMX")
    logger.info("Iniciando pipeline ETL")

    try:
        extractor = Extraccion(uri="mongodb://localhost:27017/", db_name="bi_mx", logger=logger)
        df_listings, df_reviews, df_calendar = extractor.extraer_todas()

        transformador = Transformacion(
            df_listings=df_listings,
            df_reviews_completo=df_reviews,
            df_calendar=df_calendar,
            logger=logger,
        )
        df_final = transformador.ejecutar_transformacion_completa()
        df_limpio, df_reviews_proc, df_calendar_agg, _ = transformador.obtener_resultados()

        cargador = Carga(
            df_final=df_final,
            df_limpio=df_limpio,
            df_reviews=df_reviews_proc,
            df_calendar_agregado=df_calendar_agg,
            logger=logger,
        )
        cargador.ejecutar_carga_completa()

        logger.info("Pipeline ETL finalizado con éxito")
    except Exception as exc:
        logger.error(f"Error crítico en el pipeline: {exc}")
        raise


if __name__ == "__main__":
    run_pipeline()
