#!/usr/bin/env python3
"""
Ejecuta localmente, con PySpark, los notebooks del Job en el mismo orden que Databricks.

Simula tres ejecuciones del Job y luego el notebook de verificación:
  1. hasta_fecha = 2026-01-15  (primera quincena)
  2. hasta_fecha = 2026-12-31  (mes completo: Bronze carga solo los archivos nuevos)
  3. hasta_fecha = 2026-12-31  (reejecución: valida la idempotencia)
  4. 05_verificacion           (compara los resultados con el ground truth)

El código de los notebooks se ejecuta sin modificaciones. Solo se emulan las construcciones
propias de Databricks que no existen en Spark de código abierto:
  * COPY INTO          -> lectura de los archivos aún no cargados y append a la tabla
  * MERGE ... INSERT * -> append de las filas del origen sin coincidencia en el destino (LEFT ANTI JOIN)
  * USING DELTA        -> USING parquet;  CREATE OR REPLACE TABLE -> DROP + CREATE
  * CREATE VOLUME, USE CATALOG y dbutils.widgets
Esta ejecución valida la lógica de transformación; la evidencia de la entrega es la ejecución en Databricks.

Uso:
    pip install pyspark matplotlib    # requiere Java 17 o superior
    python tests/ejecutar_job_local.py

    # prueba de escala
    python generador/generar_datos.py --salida /tmp/raw500k --operaciones 500000
    python tests/ejecutar_job_local.py --datos /tmp/raw500k
"""

import argparse
import re
import shutil
import sys
import tempfile
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

RAIZ = Path(__file__).resolve().parents[1]
NOTEBOOKS = RAIZ / "notebooks"
TAREAS = ["00_publicar_archivos", "01_ingesta_bronze", "02_build_silver", "03_build_gold", "04_validate_pipeline"]


# -----------------------------------------------------------------------------
# Emulación de dbutils, display y de las sentencias propias de Databricks
# -----------------------------------------------------------------------------

class Widgets:
    def __init__(self, valores):
        self.valores, self.defaults = valores, {}

    def text(self, nombre, default, etiqueta=None):
        self.defaults[nombre] = default

    def dropdown(self, nombre, default, opciones, etiqueta=None):
        self.defaults[nombre] = default

    def get(self, nombre):
        return self.valores.get(nombre, self.defaults[nombre])


class DBUtils:
    def __init__(self, valores):
        self.widgets = Widgets(valores)


def display(obj):
    if obj is not None and hasattr(obj, "show"):
        obj.show(25, truncate=60)


class SparkLocal:
    """Envuelve la SparkSession y traduce las sentencias propias de Databricks."""

    def __init__(self, spark):
        self._s = spark
        self._cargados = {}  # tabla Bronze -> archivos ya cargados (el registro interno de COPY INTO)

    def __getattr__(self, nombre):
        return getattr(self._s, nombre)

    def sql(self, consulta):
        q = consulta.strip()
        if re.match(r"(?is)^CREATE\s+VOLUME\b", q):
            return None
        m = re.match(r"(?is)^USE\s+CATALOG\s+(\w+)$", q)
        if m:
            self._s.catalog.setCurrentCatalog(m.group(1))
            return None
        if re.match(r"(?is)^CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+\w+$", q):
            return None  # tabla vacía sin esquema: la crea el primer COPY INTO
        if re.match(r"(?is)^COPY\s+INTO\b", q):
            return self._copy_into(q)
        if re.match(r"(?is)^MERGE\s+INTO\b", q):
            return self._merge(q)
        m = re.match(r"(?is)^CREATE\s+OR\s+REPLACE\s+TABLE\s+(\w+)(.*)$", q)
        if m:
            self._s.sql(f"DROP TABLE IF EXISTS {m.group(1)}")
            return self._s.sql(f"CREATE TABLE {m.group(1)} USING parquet{m.group(2)}")
        return self._s.sql(re.sub(r"(?i)\bUSING\s+DELTA\b", "USING parquet", q))

    def _copy_into(self, q):
        tabla = re.search(r"(?i)COPY\s+INTO\s+(\w+)", q).group(1)
        ruta = Path(re.search(r"FROM\s+'([^']+)'", q).group(1))
        formato = re.search(r"(?i)FILEFORMAT\s*=\s*(\w+)", q).group(1).lower()
        bloque = re.search(r"(?is)FORMAT_OPTIONS\s*\((.*?)\)", q).group(1)
        opciones = dict(re.findall(r"'(\w+)'\s*=\s*'([^']*)'", bloque))

        cargados = self._cargados.setdefault(tabla, set())
        archivos = sorted(str(p) for p in ruta.iterdir() if p.is_file()) if ruta.exists() else []
        nuevos = [a for a in archivos if a not in cargados]
        if not nuevos:
            return None

        lector = self._s.read.format(formato)
        for k, v in opciones.items():
            lector = lector.option(k, v)
        df = lector.load(nuevos).select(
            "*",
            F.col("_metadata.file_path").alias("_archivo_origen"),
            F.col("_metadata.file_modification_time").alias("_archivo_modificado"),
            F.current_timestamp().alias("_fecha_ingesta"),
        )
        if self._s.catalog.tableExists(tabla):  # mergeSchema: se admiten columnas nuevas
            df = self._s.table(tabla).unionByName(df, allowMissingColumns=True)
        df = df.localCheckpoint()
        self._s.sql(f"DROP TABLE IF EXISTS {tabla}")
        df.write.format("parquet").saveAsTable(tabla)
        cargados.update(nuevos)
        return None

    def _merge(self, q):
        m = re.match(r"(?is)^MERGE\s+INTO\s+(\w+)\s+t\s+USING\s+(\w+)\s+s\s+ON\s+(.*?)\s+"
                     r"WHEN\s+NOT\s+MATCHED\s+THEN\s+INSERT\s+\*$", q)
        if not m:
            raise ValueError(f"MERGE no soportado por la emulación local:\n{q}")
        destino, origen, condicion = m.groups()
        nuevos = self._s.sql(
            f"SELECT s.* FROM {origen} s LEFT ANTI JOIN {destino} t ON {condicion}").localCheckpoint()
        nuevos.write.mode("append").saveAsTable(destino)
        self._s.catalog.refreshTable(destino)
        return None


# -----------------------------------------------------------------------------
# Ejecución de notebooks en formato fuente de Databricks
# -----------------------------------------------------------------------------

def celdas(ruta):
    texto = ruta.read_text(encoding="utf-8").replace("# Databricks notebook source\n", "", 1)
    for i, celda in enumerate(texto.split("# COMMAND ----------")):
        lineas = [l for l in celda.strip().splitlines() if l.strip()]
        if not lineas or all(l.startswith("# MAGIC") for l in lineas):
            continue
        yield i, "\n".join(l for l in celda.splitlines() if not l.startswith("%"))


def ejecutar_notebook(nombre, spark, widgets, salida):
    ruta = NOTEBOOKS / f"{nombre}.py"
    ns = {"spark": spark, "dbutils": DBUtils(widgets), "display": display, "__name__": "__notebook__"}
    contador = [0]

    def guardar_figura(*_, **__):
        contador[0] += 1
        plt.savefig(salida / f"{nombre}_grafico{contador[0]}.png", dpi=110)
        plt.close("all")

    plt.show = guardar_figura
    import os
    cwd = os.getcwd()
    os.chdir(NOTEBOOKS)  # en Databricks, el directorio de trabajo es la carpeta del notebook
    try:
        for i, codigo in celdas(ruta):
            exec(compile(codigo, f"{ruta.name} [celda {i}]", "exec"), ns)
    finally:
        os.chdir(cwd)
    return ns


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--salida", default=None, help="Carpeta para los gráficos (por defecto, una temporal)")
    ap.add_argument("--datos", default=None,
                    help="Carpeta con archivos generados a usar como landing (por defecto, datos/raw del repositorio)")
    args = ap.parse_args()

    tmp = Path(tempfile.mkdtemp(prefix="tp_final_job_"))
    salida = Path(args.salida) if args.salida else tmp / "graficos"
    salida.mkdir(parents=True, exist_ok=True)

    spark = (SparkSession.builder.master("local[*]").appName("tp-final-job-local")
             .config("spark.sql.ansi.enabled", "true")
             .config("spark.sql.session.timeZone", "UTC")          # igual que Databricks por defecto
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.warehouse.dir", str(tmp / "warehouse"))
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    spark_local = SparkLocal(spark)

    comunes = {"catalogo": "spark_catalog", "schema": "tp_final_conciliacion", "ruta_base": str(tmp / "volumes")}
    if args.datos:  # p. ej., un conjunto generado con --operaciones 500000
        shutil.copytree(args.datos, tmp / "volumes" / "landing" / "fuente")
    corridas = [("2026-01-15", "primera quincena"), ("2026-12-31", "mes completo"),
                ("2026-12-31", "reejecución (idempotencia)")]

    bronze = ["bronze_ventas", "bronze_eventos_mp", "bronze_eventos_prisma",
              "bronze_liquidaciones_mp", "bronze_liquidaciones_prisma", "bronze_banco"]
    filas_bronze = []
    for n, (hasta, etiqueta) in enumerate(corridas, 1):
        print(f"\n{'=' * 78}\nEJECUCIÓN {n} DEL JOB — hasta_fecha = {hasta} ({etiqueta})\n{'=' * 78}")
        widgets = {**comunes, "hasta_fecha": hasta, "job_run_id": f"local-{n}"}
        for tarea in TAREAS:
            print(f"\n--- tarea {tarea}")
            ejecutar_notebook(tarea, spark_local, widgets, salida)
        filas_bronze.append(sum(spark.table(f"tp_final_conciliacion.{t}").count() for t in bronze))

    print(f"\n{'=' * 78}\nNOTEBOOK 05_verificacion\n{'=' * 78}")
    ns = ejecutar_notebook("05_verificacion", spark_local, comunes, salida)

    comparacion = ns["comparacion"].collect()
    diferencias = [r for r in comparacion if r["control"] != "OK"]
    auditoria = spark.table("tp_final_conciliacion.pipeline_run_audit").orderBy("registrado_en").collect()

    print(f"\n{'=' * 78}\nRESUMEN\n{'=' * 78}")
    print("Filas totales en Bronze después de cada ejecución:", filas_bronze)
    print("Ground truth:", f"{len(comparacion) - len(diferencias)} de {len(comparacion)} métricas coinciden")
    for r in diferencias:
        print("   DIFERENCIA:", r.asDict())
    print("Auditoría:", [(r.hasta_fecha.isoformat(), r.filas_silver, r.idempotencia_comparada) for r in auditoria])
    print("Gráficos:", ", ".join(str(p) for p in sorted(salida.glob("*.png"))))

    ok = (not diferencias
          and filas_bronze[0] < filas_bronze[1] == filas_bronze[2]
          and len(auditoria) == 3 and auditoria[2].idempotencia_comparada
          and not auditoria[0].idempotencia_comparada)
    print("\nRESULTADO:", "OK — incremental, idempotente y coincidente con el ground truth" if ok else "HAY DIFERENCIAS")
    if not args.salida:
        shutil.rmtree(tmp / "warehouse", ignore_errors=True)
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
