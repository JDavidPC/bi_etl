# ETL de Airbnb - Proyecto Mini-ETL

## Descripción del Proyecto

Este proyecto implementa un pipeline ETL (Extract, Transform, Load) mínimo y funcional que:

1. **Extrae** datos desde una base de datos MongoDB local con las colecciones:
   - `Listings` (listados de propiedades)
   - `Reviews` (reseñas de usuarios)
   - `calendar` (disponibilidad y precios por fecha)

2. **Transforma** los datos aplicando:
   - Selección de columnas clave
   - Normalización de tipos de datos
   - Conversión de fechas y precios
   - Limpieza de datos nulos

3. **Carga** los DataFrames transformados en archivos `.parquet` en el directorio `scr/output/`

4. **Registra** todas las operaciones en archivos de log con formato `log_YYYYMMDD_HHMM.txt` en el directorio `logs/`

---

## Instalación

### Prerrequisitos

- Python 3.8 o superior
- MongoDB instalado y ejecutándose localmente en `mongodb://localhost:27017`
- Base de datos `bi_mx` con las colecciones: `Listings`, `Reviews`, `calendar`

### Pasos de Instalación

#### Windows

```powershell
# Crear entorno virtual
python -m venv venv

# Activar entorno virtual
.\venv\Scripts\Activate.ps1

# Instalar dependencias
pip install -r requirements.txt
```

#### Mac/Linux

```bash
# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

---

## Configuración

### Parámetros por Defecto

El sistema utiliza los siguientes parámetros por defecto:

- **URI MongoDB**: `mongodb://localhost:27017`
- **Base de datos**: `bi_mx`
- **Colecciones**: `Listings`, `Reviews`, `calendar`
- **Formato de salida**: `.parquet`
- **Directorio de salida**: `scr/output/`
- **Directorio de logs**: `logs/`

Estos valores están configurados directamente en el código y pueden modificarse editando `scr/main.py` si es necesario.

---

## Ejecución

Para ejecutar el proceso ETL completo:

```bash
python scr/main.py
```

### Salida Esperada

```
============================================================
ETL DE AIRBNB - INICIO
============================================================
Fecha y hora: 2025-10-22 14:30:15

============================================================
ETL FINALIZADO CON ÉXITO
Revise el directorio 'scr/output/' para los archivos generados
Revise el directorio 'logs/' para el registro detallado
============================================================
```

---

## Estructura del Proyecto

```
etl_airbnb/
├── scr/
│   ├── extraccion.py       # Módulo de extracción desde MongoDB
│   ├── transformacion.py   # Módulo de transformación de datos
│   ├── carga.py           # Módulo de carga a archivos parquet
│   ├── main.py            # Orquestador principal del ETL
│   └── output/            # Directorio de archivos generados (.parquet)
├── notebooks/             # Directorio para notebooks de análisis
├── logs/                  # Archivos de log de cada ejecución
├── README.md              # Este archivo
└── requirements.txt       # Dependencias del proyecto
```

---

## Integrantes del Grupo y Responsabilidades

| Nombre | Rol | Responsabilidades |
|--------|-----|-------------------|
|        |     |                   |
|        |     |                   |
|        |     |                   |
|        |     |                   |

---

## Ejemplo de Salida

### Archivos Generados en `scr/output/`

```
scr/output/
├── listings.parquet
├── reviews.parquet
└── calendar.parquet
```

### Ejemplo de Log (`logs/log_20251022_1430.txt`)

```
[2025-10-22 14:30:15] [INFO] Sistema de logging inicializado
[2025-10-22 14:30:15] [INFO] ============================================================
[2025-10-22 14:30:15] [INFO] INICIANDO PROCESO ETL DE AIRBNB
[2025-10-22 14:30:15] [INFO] ============================================================
[2025-10-22 14:30:15] [INFO] Directorios de trabajo creados/verificados
[2025-10-22 14:30:15] [INFO] 
--- FASE 1: EXTRACCIÓN ---
[2025-10-22 14:30:15] [INFO] Inicializando módulo de extracción
[2025-10-22 14:30:15] [INFO] Intentando conectar a MongoDB: mongodb://localhost:27017
[2025-10-22 14:30:16] [INFO] Conexión exitosa a la base de datos: bi_mx
[2025-10-22 14:30:16] [INFO] Extrayendo colección: Listings
[2025-10-22 14:30:16] [INFO] Colección 'Listings' extraída: 5432 documentos
[2025-10-22 14:30:17] [INFO] Extrayendo colección: Reviews
[2025-10-22 14:30:18] [INFO] Colección 'Reviews' extraída: 12890 documentos
[2025-10-22 14:30:18] [INFO] Conexión a MongoDB cerrada
```

---

## Supuestos y Manejo de Errores

### Supuestos

1. **MongoDB local**: Se asume que MongoDB está instalado y ejecutándose en `localhost:27017`
2. **Base de datos**: La base de datos `bi_mx` debe existir previamente
3. **Colecciones**: Las colecciones `Listings`, `Reviews` y `calendar` deben existir (aunque pueden estar vacías)
4. **Estructura de datos**: Las colecciones pueden tener cualquier esquema; el ETL adapta las transformaciones a las columnas disponibles

### Manejo de Errores

El sistema maneja los siguientes escenarios de error:

- **Conexión fallida a MongoDB**: Se registra ERROR en el log y el proceso termina sin generar archivos
- **Colección no encontrada**: Se registra WARNING y se devuelve un DataFrame vacío (no se genera archivo)
- **Colección vacía**: Se registra WARNING y no se genera archivo de salida
- **Error en transformación**: Se registra ERROR y se devuelve el DataFrame original sin transformar
- **Error en carga**: Se registra ERROR y se continúa con las siguientes cargas

En todos los casos, **el sistema no se interrumpe abruptamente** y siempre genera un archivo de log con los detalles del error.

---

## Dependencias

Las dependencias mínimas del proyecto son:

- **pymongo**: Conexión y extracción desde MongoDB
- **pandas**: Manipulación de datos en DataFrames
- **pyarrow**: Serialización a formato Parquet

Versiones específicas en `requirements.txt`

---

## Notas Adicionales

- Los archivos `.parquet` son más eficientes en espacio y velocidad de lectura que CSV
- Cada ejecución genera un nuevo archivo de log con timestamp único
- El directorio `output/` y `logs/` se crean automáticamente si no existen
- Para modificar la conexión a MongoDB, edite los parámetros en `scr/main.py`

---

## Soporte

Para reportar problemas o consultas, revise primero el archivo de log más reciente en el directorio `logs/`
