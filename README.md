# Pipeline ETL Airbnb CDMX

## Descripción del proyecto
Este repositorio implementa un pipeline ETL end-to-end para preparar datos de Airbnb de la Ciudad de México. El flujo automatiza la extracción desde MongoDB, la transformación con cálculos de limpieza, agregaciones y análisis de sentimiento, y la carga final en formatos analíticos (SQLite y Excel). El objetivo es entregar datasets consolidados y documentados para análisis exploratorios y construcción de tableros.

Arquitectura principal:
- `src/extraccion.py`: conecta a MongoDB (colecciones `listings`, `reviews`, `calendar`) y devuelve DataFrames de pandas.
- `src/transformacion.py`: limpia y enriquece la información (imputaciones, normalización de métricas, agregación de disponibilidad, sentimiento en reseñas).
- `src/carga.py`: persiste los resultados en `src/output/` dividiendo automáticamente las hojas de Excel cuando superan el límite de filas de la herramienta.
- `src/main.py`: orquestador del pipeline con registro centralizado mediante `logs_manage.py`.
- `notebooks/exploracion_airbnb.ipynb`: resumen exploratorio y hallazgos clave.

## Instrucciones de instalación
1. **Clonar el repositorio**
   ```powershell
   git clone https://github.com/JDavidPC/bi_etl.git
   cd bi_etl
   ```
2. **Crear y activar un entorno virtual (Windows PowerShell)**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```
3. **Instalar dependencias**
   ```powershell
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
4. **Configurar variables de entorno** (si tu instancia de MongoDB no es `mongodb://localhost:27017/bi_mx`, actualiza `src/main.py` o exporta la URI correspondiente).

## Ejecución del ETL
1. Asegúrate de que la base de datos MongoDB esté accesible.
2. Corre el pipeline completo:
   ```powershell
   C:/Ruta/Al/Repo/venv/Scripts/python.exe src/main.py
   ```
3. Salidas principales:
   - `src/output/bi_mx.db`: tabla `listings_analitica` con los datos enriquecidos.
   - `src/output/datos_limpios.xlsx`: hojas segmentadas (`listings_limpio`, `reviews_analizados`, `calendar_limpio`, etc.) respetando el límite de 1,048,576 filas por hoja.
   - `logs/log_YYYYMMDD_HHMM.txt`: bitácora completa de la ejecución.

### Ejemplo de corrida
```
PS C:\Users\usuario\bi_etl> C:/Users/usuario/bi_etl/venv/Scripts/python.exe src/main.py
...
[INFO] ✓ Colección 'listings' extraída: 26,401 registros
[INFO] ✓ Colección 'reviews' extraída: 1,388,226 registros
[WARNING] ⚠️  'reviews_analizados' excede 1,048,576 filas; se dividirá en 2 hojas.
[INFO] Éxito: Se guardaron 5 hojas en el archivo 'datos_limpios.xlsx'
[INFO] Pipeline ETL finalizado con éxito
```

## Integrantes y responsabilidades
Actualiza la tabla con la información real del equipo.

| Integrante | Rol y responsabilidades |
|------------|-------------------------|
| _(Nombre)_ | _(Ej.: Diseño de la arquitectura ETL, desarrollo de `main.py`)_ |
| _(Nombre)_ | _(Ej.: Limpieza y enriquecimiento de datos en `transformacion.py`)_ |
