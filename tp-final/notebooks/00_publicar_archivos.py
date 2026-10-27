# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Publicación de archivos
# MAGIC
# MAGIC Primera tarea del Job. Prepara el entorno y simula la llegada de los archivos crudos:
# MAGIC
# MAGIC 1. Crea el schema y dos Volumes: `landing`, que contiene el conjunto completo de archivos del mes, y `raw`, que es la ubicación leída por la capa Bronze.
# MAGIC 2. Copia a `landing` los archivos versionados en el repositorio (`tp-final/datos/raw`) o los regenera con el script del generador.
# MAGIC 3. Publica en `raw` los archivos con fecha menor o igual a `hasta_fecha`.
# MAGIC
# MAGIC La ingesta incremental se demuestra ejecutando el Job primero con `hasta_fecha = 2026-01-15` y luego con `hasta_fecha = 2026-12-31`.

# COMMAND ----------

dbutils.widgets.text("catalogo", "workspace")
dbutils.widgets.text("schema", "tp_final_conciliacion")
dbutils.widgets.text("hasta_fecha", "2026-01-15")
dbutils.widgets.dropdown("origen", "repo", ["repo", "generar"])
dbutils.widgets.text("ruta_base", "")  # vacío = /Volumes/<catalogo>/<schema>

# COMMAND ----------

import os
import re
import shutil
import subprocess
import sys
from datetime import date
from pathlib import Path

catalogo = dbutils.widgets.get("catalogo")
schema = dbutils.widgets.get("schema")
hasta_fecha = dbutils.widgets.get("hasta_fecha").strip()
origen = dbutils.widgets.get("origen")
base = dbutils.widgets.get("ruta_base").strip() or f"/Volumes/{catalogo}/{schema}"

assert re.fullmatch(r"\d{4}-\d{2}-\d{2}", hasta_fecha), "hasta_fecha debe tener formato yyyy-mm-dd"
date.fromisoformat(hasta_fecha)
corte = hasta_fecha.replace("-", "")

landing = Path(base) / "landing" / "fuente"
raw = Path(base) / "raw"
print(f"landing = {landing}\nraw     = {raw}\ncorte   = {hasta_fecha}")

# COMMAND ----------

# MAGIC %md ## 1. Schema y Volumes

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema}.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalogo}.{schema}.raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Conjunto completo en `landing`
# MAGIC Se carga una única vez. Si `landing` ya contiene el ground truth, no se modifica.

# COMMAND ----------

tp_dir = Path(os.getcwd()).parent  # carpeta tp-final/ (el directorio de trabajo es el del notebook)
if not (tp_dir / "datos" / "raw").exists():
    try:  # alternativa: ruta del notebook en el workspace
        ruta_nb = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
        tp_dir = Path("/Workspace" + ruta_nb).parent.parent
    except Exception:
        pass
if origen == "repo":
    assert (tp_dir / "datos" / "raw").exists(), f"No se encontró datos/raw en {tp_dir}"
landing.mkdir(parents=True, exist_ok=True)

if not (landing / "_control" / "ground_truth.jsonl").exists():
    if origen == "repo":
        shutil.copytree(tp_dir / "datos" / "raw", landing, dirs_exist_ok=True)
    else:
        subprocess.run([sys.executable, str(tp_dir / "generador" / "generar_datos.py"),
                        "--salida", str(landing)], check=True)
    print("landing cargado")
else:
    print("landing ya estaba cargado")

print(sum(1 for p in landing.rglob("*") if p.is_file()), "archivos en landing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Publicación en `raw` hasta la fecha de corte
# MAGIC Los maestros se publican siempre. El resto de los archivos se publica según la fecha `yyyymmdd` incluida en su nombre. Los archivos de control (`_control/`) no se publican porque no forman parte de la ingesta.

# COMMAND ----------

raw.mkdir(parents=True, exist_ok=True)
nuevos, ya_estaban = 0, 0
for p in sorted(landing.rglob("*")):
    if not p.is_file() or "_control" in p.parts:
        continue
    m = re.search(r"_(\d{8})\.", p.name)
    if m and m.group(1) > corte:
        continue
    destino = raw / p.relative_to(landing)
    if destino.exists():
        ya_estaban += 1
        continue
    destino.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy(p, destino)
    nuevos += 1

print(f"Publicados en esta ejecución: {nuevos} · ya publicados: {ya_estaban}")

# COMMAND ----------

display(spark.createDataFrame(
    [(str(d.relative_to(raw)), sum(1 for f in d.iterdir() if f.is_file()))
     for d in sorted(raw.rglob("*")) if d.is_dir() and any(f.is_file() for f in d.iterdir())],
    "carpeta string, archivos int"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reinicio completo (opcional)
# MAGIC Elimina el schema con sus tablas y Volumes. La siguiente ejecución del Job reconstruye todo desde cero.

# COMMAND ----------

# spark.sql(f"DROP SCHEMA IF EXISTS {catalogo}.{schema} CASCADE")
