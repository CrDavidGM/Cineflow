# CineFlow

Mini pipeline de datos con **MovieLens**: ingesta (raw en Mongo), validación, curado (Postgres), API (FastAPI) y dashboard (Streamlit).

## Quickstart

1) Copia `.env.example` a `.env.local` (para uso en tu host) y ajusta variables si es necesario.
   - El archivo `.env` se mantiene con los valores usados por Docker/CI (por ejemplo `postgres`, `mongo`).
   - Si quieres forzar otro archivo en cualquier entorno exporta `CINEFLOW_ENV_FILE=/ruta/a/tu/.env`.
2) Levanta servicios base:
```bash
docker compose up -d postgres mongo
```
3) (Opcional) Crea entorno local y deps:
```bash
pip install -e .
```
4) Descarga MovieLens 100K y deja CSVs en `data/samples/`:
   - ratings -> `ratings.csv` con columnas: userId,movieId,rating,timestamp
   - movies  -> `movies.csv`  con: movieId,title,genres
5) Ejecuta la primera ingesta a Mongo (raw):
```bash
python -m pipelines.ingest_raw
```
6) Carga curada a Postgres:
```bash
python -m pipelines.load_warehouse
```
7) API:
```bash
uvicorn api.main:app --reload
```
8) Dashboard:
```bash
streamlit run src/dashboard/app.py
```

## Tests locales (mismo flujo que CI)

1. Asegúrate de tener `.env.local` apuntando a `localhost` (copiar desde `.env.example` es suficiente).
2. Levanta las bases necesarias una sola vez:
   ```bash
   docker compose up -d postgres mongo
   ```
3. Carga datos de muestra en Postgres (si es la primera vez o cambiaste algo):
   ```bash
   poetry run python -m cineflow.runner --skip-validate
   ```
4. Ejecuta todos los tests tal cual se hace en GitHub Actions:
   ```bash
   poetry run pytest -q
   ```
   (Opcional) También puedes correrlos dentro del contenedor para emular CI:
   ```bash
   docker compose run --rm api poetry run pytest -q
   ```

## Estructura
- `src/pipelines`: ingestión y carga a warehouse
- `src/storage`: clientes Mongo/Postgres
- `src/api`: FastAPI con endpoints de métricas
- `src/dashboard`: Streamlit para KPIs
- `data/samples`: CSVs de entrada
