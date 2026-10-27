#!/usr/bin/env python3
"""
Generador de archivos crudos para el TP Final de Big Data (Opción A — medallion).

Simula lo que una empresa recibe de cada sistema en un mes de operación:

  ventas/         Plataforma de ventas propia      JSON Lines, un archivo por día
  eventos_mp/     Reporte de eventos Mercado Pago  CSV ',' en inglés, fechas UTC
  eventos_prisma/ Reporte de eventos Prisma        CSV ';' en castellano, '12.345,67'
  liquidaciones/  Archivos de liquidación por PSP  CSV con registro trailer (T)
  banco/          Extractos bancarios por cuenta   CSV ';', débito/crédito separados
  maestros/       Datos de referencia              CSV

La lógica de negocio (mix de estados, fantasmas, diferencias de monto, capturas sin
liquidar, lotes sin acreditar) replica el script 02_populate.sql del TP de Almacenes
de Datos, para que los resultados sean comparables.

Sobre esa base se inyectan, en cantidades fijas y documentadas, errores de CALIDAD DE
DATOS (duplicados por reenvío, montos no numéricos, fechas imposibles, etc.). Los
errores de calidad se agregan como filas EXTRA: descartarlos en Silver no altera los
casos de conciliación, que deben llegar intactos a Gold.

Todo lo inyectado queda registrado en _control/ground_truth.jsonl, para comparar lo
esperado contra lo que detecta el pipeline.

Uso:
    python generar_datos.py --salida ../datos/raw
    python generar_datos.py --salida /tmp/raw --operaciones 500000   # prueba de escala
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import random
import shutil
from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

# -----------------------------------------------------------------------------
# Parámetros
# -----------------------------------------------------------------------------

SEMILLA = 2026
INICIO = date(2026, 1, 1)
DIA_SCHEMA_EVOLUTION = date(2026, 1, 16)  # desde acá MP agrega 'installments' y ventas 'coupon_code'
TZ_AR = timezone(timedelta(hours=-3))

# Errores de calidad (filas extra o copias), por cada 3.000 operaciones.
ERRORES_BASE = {
    "ventas_duplicadas": 45,          # reenvío: la misma orden llega en el archivo del día siguiente
    "ventas_monto_no_numerico": 12,   # total_amount = "N/A" o ""
    "ventas_usuario_nulo": 8,         # user_id = null
    "ventas_estado_desconocido": 6,   # status = "APPROVD"
    "mp_duplicados": 30,              # el mismo evento reenviado al día siguiente
    "mp_monto_vacio": 10,             # gross_amount vacío
    "prisma_duplicados": 25,
    "prisma_fecha_invalida": 6,       # 31/02/2026, 00/01/2026...
}
PROP_ESTADO_IRREGULAR = 0.10          # "APPROVED", " Approved " -> se normaliza, no se descarta

# -----------------------------------------------------------------------------
# Maestros (mismos valores que el DW original)
# -----------------------------------------------------------------------------

CATEGORIAS = {1: "Suscripciones", 2: "Cursos", 3: "Servicios digitales",
              4: "Software", 5: "Contenido premium", 6: "Consultoria"}

PRODUCTOS = [
    ("SUB-BAS-01", "Suscripcion Basica Mensual", 1), ("SUB-PRO-01", "Suscripcion Pro Mensual", 1),
    ("SUB-ENT-01", "Suscripcion Enterprise Mensual", 1), ("CUR-SQL-01", "Curso SQL Avanzado", 2),
    ("CUR-PY-01", "Curso Python Data Science", 2), ("CUR-DW-01", "Curso Data Warehouse", 2),
    ("SRV-API-01", "API Calls Pack 10k", 3), ("SRV-API-02", "API Calls Pack 100k", 3),
    ("SRV-STG-01", "Storage 100GB", 3), ("SOF-LIC-01", "Licencia Software Anual", 4),
    ("SOF-LIC-02", "Licencia Software Perpetua", 4), ("CON-VID-01", "Videos Premium Mes", 5),
    ("CON-VID-02", "Videos Premium Anio", 5), ("CON-ART-01", "Articulos Premium", 5),
    ("CNS-1H-01", "Hora Consultoria", 6), ("CNS-4H-01", "Pack 4h Consultoria", 6),
]

SEGMENTOS = {1: ("Alto valor", "26-35"), 2: ("Alto valor", "36-50"), 3: ("Recurrente", "26-35"),
             4: ("Recurrente", "36-50"), 5: ("Nuevo", "18-25"), 6: ("Nuevo", "26-35"),
             7: ("Ocasional", "36-50"), 8: ("Ocasional", "50+")}

CANALES = {1: ("web", "Web"), 2: ("mobile", "Mobile"), 3: ("pos", "POS"), 4: ("api", "API")}

ESTADOS = {1: ("approved", "Aprobado"), 2: ("rejected", "Rechazado"), 3: ("pending", "Pendiente"),
           4: ("refunded", "Devuelto"), 5: ("charged_back", "Contracargo")}

TIPOS_MEDIO = {1: "Tarjeta credito", 2: "Tarjeta debito", 3: "Wallet", 4: "Transferencia", 5: "QR"}

# id_medio: (nombre, marca, id_tipo, código en reporte MP, código en reporte Prisma)
MEDIOS = {
    1: ("Visa Credito", "Visa", 1, "visa", "VISA CREDITO"),
    2: ("Mastercard Credito", "Mastercard", 1, "master", "MASTERCARD CREDITO"),
    3: ("Amex Credito", "Amex", 1, "amex", "AMEX"),
    4: ("Visa Debito", "Visa", 2, "debvisa", "VISA DEBITO"),
    5: ("Mastercard Debito", "Mastercard", 2, "debmaster", "MASTERCARD DEBITO"),
    6: ("MercadoPago Wallet", "MP", 3, "account_money", "BILLETERA MP"),
    7: ("Modo", "Modo", 3, "modo", "MODO"),
    8: ("Transferencia CBU", "CVU", 4, "bank_transfer", "TRANSFERENCIA"),
    9: ("QR Interoperable", "QR", 5, "qr", "QR INTEROPERABLE"),
}

PSPS = {1: ("Mercado Pago", "mercadopago", "MP", "CTA-MP-001"),
        2: ("Prisma", "prisma", "PR", "CTA-PRISMA-001")}

# cod_evento: (descripción, código MP, código Prisma)
TIPOS_EVENTO = {"AUTH": ("Autorizacion", "authorization", "AUT"),
                "CAPTURE": ("Captura", "capture", "CAP"),
                "REFUND": ("Devolucion", "refund", "DEV"),
                "CHBK": ("Contracargo", "chargeback", "CTC")}

FEE_PCT = {1: Decimal("0.0799"), 2: Decimal("0.0650")}
PLAZO_LIQ = {1: 2, 2: 1}  # MP T+2, Prisma T+1

# -----------------------------------------------------------------------------
# Utilidades
# -----------------------------------------------------------------------------

CENT = Decimal("0.01")


def r2(x: Decimal) -> Decimal:
    """ROUND(x, 2) con la semántica de PostgreSQL (mitad lejos de cero)."""
    return x.quantize(CENT, rounding=ROUND_HALF_UP)


def fmt_punto(x: Decimal) -> str:
    return f"{x:.2f}"


def fmt_ar(x: Decimal) -> str:
    """12345.6 -> '12.345,60' (formato argentino)."""
    s = f"{abs(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" if x < 0 else "") + s


def ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def dmy(d: date) -> str:
    return d.strftime("%d/%m/%Y")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def escalar(base: int, n_ops: int) -> int:
    return max(1, round(base * n_ops / 3000))


# -----------------------------------------------------------------------------
# 1. Base de negocio (réplica de 02_populate.sql)
# -----------------------------------------------------------------------------

def generar_base(n_ops: int, rng: random.Random):
    skus = sorted(p[0] for p in PRODUCTOS)
    n_usuarios = max(500, n_ops // 6)
    n_fantasmas = escalar(100, n_ops)

    trx, eventos = [], []
    op_id, ev_id, chbk_id = 500000, 200000, 700000
    casos = Counter()

    def ts_local(d: date) -> datetime:
        # horario comercial 08:00-19:59 ART: en UTC nunca cruza la medianoche
        return datetime.combine(d, time(rng.randint(8, 19), rng.randint(0, 59), rng.randint(0, 59)), TZ_AR)

    def nuevo_evento(fecha_hora, psp, medio, op, ext_ref, cod, trx_id, bruto, neto, fee, iva, chbk=None):
        nonlocal ev_id
        ev_id += 1
        eventos.append(dict(id_evento=ev_id, fecha_hora=fecha_hora, id_psp=psp, id_medio=medio,
                            id_operacion=op, external_reference=ext_ref, cod_evento=cod,
                            id_trx=trx_id, monto_bruto=bruto, monto_neto=neto, fee=fee,
                            impuestos=iva, id_contracargo=chbk))

    for i in range(1, n_ops + 1):
        trx_id = 100000 + i
        fecha = INICIO + timedelta(days=(i * 13) % 31)
        caso = (i * 7) % 100
        psp = 1 + (i % 2)
        medio = 1 + (i * 3) % 9
        monto = Decimal(500 + (i * 137) % 99500)
        fee_pct = FEE_PCT[psp]
        estado = 2 if 82 <= caso <= 91 else 4 if 92 <= caso <= 96 else 5 if caso >= 97 else 1
        creado = ts_local(fecha)
        ext_ref = f"REF-{psp}-{trx_id}"

        trx.append(dict(id_trx=trx_id, creado=creado, fecha=fecha, sku=skus[i % len(skus)],
                        id_usuario=1 + (i * 11) % n_usuarios, id_canal=1 + (i * 5) % 4,
                        id_estado=estado, monto=monto, cantidad=1 + (i % 3),
                        descuento=r2(monto * Decimal("0.02")), id_psp=psp, ext_ref=ext_ref))

        t_auth = creado + timedelta(seconds=rng.randint(1, 4))
        t_cap = t_auth + timedelta(seconds=rng.randint(1, 20))

        if caso <= 2:
            casos["FANTASMA_CLIENTE"] += 1
            continue

        op_id += 1
        if 82 <= caso <= 91:
            casos["RECHAZO_OK"] += 1
            nuevo_evento(t_auth, psp, medio, op_id, ext_ref, "AUTH", trx_id, monto, Decimal(0), Decimal(0), Decimal(0))
            continue

        monto_psp = r2(monto * Decimal("0.95")) if caso <= 5 else monto
        fee = r2(monto_psp * fee_pct)
        iva = r2(fee * Decimal("0.21"))
        neto = monto_psp - fee - iva
        nuevo_evento(t_auth, psp, medio, op_id, ext_ref, "AUTH", trx_id, monto_psp, monto_psp, Decimal(0), Decimal(0))
        nuevo_evento(t_cap, psp, medio, op_id, ext_ref, "CAPTURE", trx_id, monto_psp, neto, fee, iva)

        if 92 <= caso <= 96:
            casos["DEVOLUCION_OK"] += 1
            nuevo_evento(ts_local(fecha + timedelta(days=3)), psp, medio, op_id, ext_ref, "REFUND",
                         trx_id, -monto, -neto, -fee, -iva)
        elif caso >= 97:
            casos["CONTRACARGO_OK"] += 1
            chbk_id += 1
            nuevo_evento(ts_local(fecha + timedelta(days=5)), psp, medio, op_id, ext_ref, "CHBK",
                         trx_id, -monto, -monto, r2(monto * Decimal("0.015")), Decimal(0), chbk=chbk_id)
        elif caso <= 5:
            casos["DIFERENCIA_MONTO"] += 1
        else:
            casos["OK_CONCILIADO"] += 1

    # Fantasmas PSP: capturas cuyo external_reference no existe en nuestra plataforma
    for i in range(1, n_fantasmas + 1):
        fecha = INICIO + timedelta(days=(i * 7) % 31)
        psp = 1 + (i % 2)
        monto = Decimal(500 + (i * 197) % 49500)
        fee = r2(monto * FEE_PCT[psp])
        iva = r2(fee * Decimal("0.21"))
        op_id += 1
        nuevo_evento(ts_local(fecha), psp, 1 + (i * 3) % 9, op_id, f"GHOST-{psp}-{i}", "CAPTURE",
                     None, monto, monto - fee - iva, fee, iva)
    casos["FANTASMA_PSP"] = n_fantasmas

    usuarios = [dict(id_usuario=n, hash_email=sha256(f"email_{n}"), hash_doc=sha256(f"doc_{n}"),
                     edad=18 + (n * 7) % 55, id_segmento=1 + (n % 8)) for n in range(1, n_usuarios + 1)]
    return trx, eventos, usuarios, casos


def generar_liquidaciones(eventos, n_ops):
    max_sin_liquidar = escalar(37, n_ops)
    liqs, sin_liquidar = [], 0
    liq_id, n_cap = 800000, 0
    for e in sorted(eventos, key=lambda x: x["id_evento"]):
        if e["cod_evento"] == "AUTH":
            continue
        if e["cod_evento"] == "CAPTURE":
            n_cap += 1
            if n_cap % 73 == 0 and sin_liquidar < max_sin_liquidar:
                sin_liquidar += 1
                continue
        psp = e["id_psp"]
        fecha_liq = e["fecha_hora"].date() + timedelta(days=PLAZO_LIQ[psp])
        lote = f"LOTE-{PSPS[psp][2]}-{ymd(fecha_liq)}-{1 + e['id_evento'] % 2:02d}"
        ret = r2(abs(e["monto_bruto"]) * Decimal("0.005"))
        liq_id += 1
        liqs.append(dict(
            id_liquidacion=liq_id, fecha=fecha_liq, id_psp=psp, cod_evento=e["cod_evento"],
            id_evento=e["id_evento"], lote=lote,
            monto=e["monto_neto"] + ret if e["cod_evento"] == "REFUND" else e["monto_neto"] - ret,
            fee=e["fee"], retencion=ret,
            contracargo=abs(e["monto_bruto"]) if e["cod_evento"] == "CHBK" else Decimal(0)))
    return liqs, sin_liquidar


def generar_banco(liqs):
    lotes = sorted({l["lote"] for l in liqs})
    excluidos = {lotes[8], lotes[24]}  # lotes que nunca se acreditan (como en el original)
    grupos = defaultdict(list)
    for l in liqs:
        grupos[(l["fecha"], l["id_psp"])].append(l)

    movs, mov_id, fecha_distinta = [], 900000, 0
    for (fecha_liq, psp), ls in sorted(grupos.items()):
        incluidas = [l for l in ls if l["lote"] not in excluidos]
        if not incluidas:
            continue
        lotes_dia = sorted({l["lote"] for l in incluidas})
        corrida = fecha_liq >= date(2026, 1, 28)  # fin de mes: el banco acredita al día siguiente
        if corrida:
            fecha_distinta += len(lotes_dia)
        mov_id += 1
        movs.append(dict(id_mov=mov_id, fecha_mov=fecha_liq + timedelta(days=1 if corrida else 0),
                         fecha_valor=fecha_liq, importe=sum(l["monto"] for l in incluidas),
                         concepto=f"ACREDITACION {PSPS[psp][0].upper()} LOTES: {','.join(lotes_dia)}",
                         cuenta=PSPS[psp][3]))

    # Movimientos que no son acreditaciones de PSP: comisión mensual de cada cuenta
    for psp in PSPS:
        mov_id += 1
        movs.append(dict(id_mov=mov_id, fecha_mov=date(2026, 1, 31), fecha_valor=date(2026, 1, 31),
                         importe=Decimal("-15000.00"), concepto="COMISION MANTENIMIENTO CUENTA",
                         cuenta=PSPS[psp][3]))

    lotes_por_estado = Counter()
    for lote in lotes:
        if lote in excluidos:
            lotes_por_estado["SIN_ACREDITAR"] += 1
        elif datetime.strptime(lote.split("-")[2], "%Y%m%d").date() >= date(2026, 1, 28):
            lotes_por_estado["FECHA_ACREDITACION_DISTINTA"] += 1
        else:
            lotes_por_estado["OK_ACREDITADO"] += 1
    lotes_por_estado["MOVIMIENTO_SIN_LOTE"] = len(PSPS)
    assert lotes_por_estado["FECHA_ACREDITACION_DISTINTA"] == fecha_distinta
    return movs, lotes_por_estado


# -----------------------------------------------------------------------------
# 2. Escritura de archivos crudos (con errores de calidad inyectados)
# -----------------------------------------------------------------------------

def escribir_csv(path: Path, header, filas, sep=","):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=sep, lineterminator="\n")
        w.writerow(header)
        w.writerows(filas)


def escribir_ventas(trx, salida: Path, n_ops, rng, gt):
    por_dia = defaultdict(list)
    estados_irregulares = 0
    for t in trx:
        status = ESTADOS[t["id_estado"]][0]
        if rng.random() < PROP_ESTADO_IRREGULAR:
            status = rng.choice([status.upper(), f" {status.capitalize()} "])
            estados_irregulares += 1
        reg = {
            "order_id": t["id_trx"],
            "created_at": t["creado"].isoformat(),
            "user_id": t["id_usuario"],
            "channel": CANALES[t["id_canal"]][0],
            "status": status,
            "currency": "ARS",
            "items": [{"sku": t["sku"], "quantity": t["cantidad"], "unit_price": float(t["monto"])}],
            "total_amount": float(t["monto"]),
            "discount": float(t["descuento"]),
            "payment": {"provider": PSPS[t["id_psp"]][1], "reference": t["ext_ref"]},
        }
        if t["fecha"] >= DIA_SCHEMA_EVOLUTION:
            reg["coupon_code"] = rng.choice([None, None, None, "VERANO10", "BIENVENIDA"])
        por_dia[t["fecha"]].append(reg)

    # Duplicados por reenvío: la misma orden vuelve a llegar en el archivo del día siguiente
    n_dup = escalar(ERRORES_BASE["ventas_duplicadas"], n_ops)
    por_id = {r["order_id"]: r for regs in por_dia.values() for r in regs}
    for t in rng.sample(trx, n_dup):
        por_dia[t["fecha"] + timedelta(days=1)].append(dict(por_id[t["id_trx"]]))

    # Filas inválidas extra (ids fuera del rango real, sin contraparte en el PSP)
    invalidas, sig_id = Counter(), 9_000_000
    for tipo in ("ventas_monto_no_numerico", "ventas_usuario_nulo", "ventas_estado_desconocido"):
        for _ in range(escalar(ERRORES_BASE[tipo], n_ops)):
            sig_id += 1
            d = INICIO + timedelta(days=rng.randint(0, 30))
            psp = rng.choice([1, 2])
            reg = {"order_id": sig_id,
                   "created_at": datetime.combine(d, time(rng.randint(8, 19), 0), TZ_AR).isoformat(),
                   "user_id": rng.randint(1, 500), "channel": "web", "status": "approved",
                   "currency": "ARS",
                   "items": [{"sku": "SUB-BAS-01", "quantity": 1, "unit_price": 1000.0}],
                   "total_amount": 1000.0, "discount": 0.0,
                   "payment": {"provider": PSPS[psp][1], "reference": f"REF-{psp}-{sig_id}"}}
            if tipo == "ventas_monto_no_numerico":
                reg["total_amount"] = rng.choice(["N/A", ""])
            elif tipo == "ventas_usuario_nulo":
                reg["user_id"] = None
            else:
                reg["status"] = "APPROVD"
            por_dia[d].append(reg)
            invalidas[tipo] += 1

    total = 0
    for d, regs in sorted(por_dia.items()):
        p = salida / "ventas" / f"ventas_{ymd(d)}.json"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            for r in regs:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
        total += len(regs)

    gt.append(("calidad", "ventas", "filas_crudas", total))
    gt.append(("calidad", "ventas", "duplicados", n_dup))
    for k, v in invalidas.items():
        gt.append(("calidad", "ventas", k.replace("ventas_", "invalidas_"), v))
    gt.append(("calidad", "ventas", "cuarentena", sum(invalidas.values())))
    gt.append(("calidad", "ventas", "estado_normalizado", estados_irregulares))
    gt.append(("calidad", "ventas", "filas_silver", len(trx)))


def escribir_eventos(eventos, salida: Path, n_ops, rng, gt):
    for psp in PSPS:
        evs = [e for e in eventos if e["id_psp"] == psp]
        nombre = "mp" if psp == 1 else "prisma"
        filas_por_dia = defaultdict(list)

        def fila(e, invalido=None):
            cod = TIPOS_EVENTO[e["cod_evento"]][1 if psp == 1 else 2]
            medio = MEDIOS[e["id_medio"]][3 if psp == 1 else 4]
            if psp == 1:
                fecha = e["fecha_hora"].astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
                bruto = "" if invalido == "monto_vacio" else fmt_punto(e["monto_bruto"])
                cuotas = str((1, 3, 6)[e["id_operacion"] % 3]) if MEDIOS[e["id_medio"]][2] == 1 else "1"
                return [e["id_evento"], fecha, e["id_operacion"], e["external_reference"], cod, medio,
                        "ARS", bruto, fmt_punto(e["monto_neto"]), fmt_punto(e["fee"]),
                        fmt_punto(e["impuestos"]), e["id_contracargo"] or "", cuotas]
            fecha = invalido or dmy(e["fecha_hora"].date())
            return [e["id_evento"], fecha, e["id_operacion"], e["external_reference"], cod, medio,
                    "ARS", fmt_ar(e["monto_bruto"]), fmt_ar(e["monto_neto"]), fmt_ar(e["fee"]),
                    fmt_ar(e["impuestos"]), e["id_contracargo"] or ""]

        for e in evs:
            filas_por_dia[e["fecha_hora"].date()].append(fila(e))

        n_dup = escalar(ERRORES_BASE[f"{nombre}_duplicados"], n_ops)
        for e in rng.sample(evs, n_dup):
            filas_por_dia[e["fecha_hora"].date() + timedelta(days=1)].append(fila(e))

        tipo_inv = "mp_monto_vacio" if psp == 1 else "prisma_fecha_invalida"
        n_inv = escalar(ERRORES_BASE[tipo_inv], n_ops)
        for k in range(n_inv):
            d = INICIO + timedelta(days=rng.randint(0, 30))
            ev = dict(id_evento=9_500_000 + psp * 10_000 + k,
                      fecha_hora=datetime.combine(d, time(12, 0), TZ_AR), id_psp=psp,
                      id_medio=1, id_operacion=9_600_000 + psp * 10_000 + k,
                      external_reference=f"REF-{psp}-{9_700_000 + psp * 10_000 + k}",
                      cod_evento="CAPTURE", monto_bruto=Decimal("1000.00"), monto_neto=Decimal("910.00"),
                      fee=Decimal("70.00"), impuestos=Decimal("20.00"), id_contracargo=None)
            inv = "monto_vacio" if psp == 1 else rng.choice(["31/02/2026", "00/01/2026", "32/01/2026"])
            filas_por_dia[d].append(fila(ev, inv))

        total = 0
        for d, filas in sorted(filas_por_dia.items()):
            if psp == 1:
                header = ["id", "date_created", "operation_id", "external_reference", "event_type",
                          "payment_method_id", "currency_id", "gross_amount", "net_amount",
                          "fee_amount", "tax_amount", "chargeback_id"]
                if d >= DIA_SCHEMA_EVOLUTION:  # evolución de esquema: columna nueva al final
                    header = header + ["installments"]
                else:
                    filas = [f[:-1] for f in filas]
                escribir_csv(salida / "eventos_mp" / f"mp_events_{ymd(d)}.csv", header, filas, ",")
            else:
                header = ["id_evento", "fecha_mov", "nro_operacion", "ref_comercio", "tipo_mov", "medio",
                          "moneda", "importe_bruto", "importe_neto", "arancel", "iva_arancel",
                          "id_contracargo"]
                escribir_csv(salida / "eventos_prisma" / f"prisma_movimientos_{ymd(d)}.csv", header, filas, ";")
            total += len(filas)

        fuente = f"eventos_{nombre}"
        gt.append(("calidad", fuente, "filas_crudas", total))
        gt.append(("calidad", fuente, "duplicados", n_dup))
        gt.append(("calidad", fuente, "cuarentena", n_inv))
        gt.append(("calidad", fuente, "filas_silver", len(evs)))


def escribir_liquidaciones(liqs, salida: Path, gt):
    archivos = 0
    for psp in PSPS:
        por_dia = defaultdict(list)
        for l in liqs:
            if l["id_psp"] == psp:
                por_dia[l["fecha"]].append(l)
        for d, ls in sorted(por_dia.items()):
            total = sum(l["monto"] for l in ls)
            if psp == 1:
                header = ["record_type", "settlement_id", "batch_id", "settlement_date", "source_event_id",
                          "transaction_type", "settled_amount", "fee_amount", "withholding_amount",
                          "chargeback_amount"]
                filas = [["D", l["id_liquidacion"], l["lote"], d.isoformat(), l["id_evento"],
                          TIPOS_EVENTO[l["cod_evento"]][1], fmt_punto(l["monto"]), fmt_punto(l["fee"]),
                          fmt_punto(l["retencion"]), fmt_punto(l["contracargo"])] for l in ls]
                filas.append(["T", "", "", d.isoformat(), "", "", fmt_punto(total), "", "", ""])
                escribir_csv(salida / "liquidaciones" / "mp" / f"settlement_{ymd(d)}.csv", header, filas, ",")
            else:
                header = ["tipo_registro", "id_liquidacion", "nro_lote", "fecha_liquidacion", "id_evento",
                          "tipo_mov", "importe_liquidado", "arancel", "retenciones", "contracargos"]
                filas = [["D", l["id_liquidacion"], l["lote"], dmy(d), l["id_evento"],
                          TIPOS_EVENTO[l["cod_evento"]][2], fmt_ar(l["monto"]), fmt_ar(l["fee"]),
                          fmt_ar(l["retencion"]), fmt_ar(l["contracargo"])] for l in ls]
                filas.append(["T", "", "", dmy(d), "", "", fmt_ar(total), "", "", ""])
                escribir_csv(salida / "liquidaciones" / "prisma" / f"liquidacion_{ymd(d)}.csv", header, filas, ";")
            archivos += 1
    gt.append(("calidad", "liquidaciones", "filas_crudas", len(liqs) + archivos))
    gt.append(("calidad", "liquidaciones", "registros_trailer", archivos))
    gt.append(("calidad", "liquidaciones", "cuarentena", 0))
    gt.append(("calidad", "liquidaciones", "filas_silver", len(liqs)))


def escribir_banco(movs, salida: Path, gt):
    saldos = {c: Decimal("2500000.00") for c in (p[3] for p in PSPS.values())}
    por_archivo = defaultdict(list)
    for m in sorted(movs, key=lambda x: (x["fecha_mov"], x["id_mov"])):
        saldos[m["cuenta"]] += m["importe"]
        debito = fmt_ar(-m["importe"]) if m["importe"] < 0 else ""
        credito = fmt_ar(m["importe"]) if m["importe"] >= 0 else ""
        por_archivo[(m["cuenta"], m["fecha_mov"])].append(
            [dmy(m["fecha_mov"]), dmy(m["fecha_valor"]), m["id_mov"], m["concepto"], debito, credito,
             fmt_ar(saldos[m["cuenta"]]), m["cuenta"]])
    for (cuenta, d), filas in sorted(por_archivo.items()):
        escribir_csv(salida / "banco" / f"extracto_{cuenta}_{ymd(d)}.csv",
                     ["fecha_operacion", "fecha_valor", "nro_movimiento", "concepto", "debito",
                      "credito", "saldo", "cuenta"], filas, ";")
    gt.append(("calidad", "banco", "filas_crudas", len(movs)))
    gt.append(("calidad", "banco", "cuarentena", 0))
    gt.append(("calidad", "banco", "filas_silver", len(movs)))


def escribir_maestros(usuarios, salida: Path):
    m = salida / "maestros"
    escribir_csv(m / "productos.csv", ["sku", "nombre", "id_categoria", "categoria", "iso_moneda"],
                 [[s, n, c, CATEGORIAS[c], "ARS"] for s, n, c in PRODUCTOS])
    escribir_csv(m / "usuarios.csv", ["id_usuario", "hash_email", "hash_doc", "edad", "id_segmento",
                                      "segmento", "grupo_etario"],
                 [[u["id_usuario"], u["hash_email"], u["hash_doc"], u["edad"], u["id_segmento"],
                   *SEGMENTOS[u["id_segmento"]]] for u in usuarios])
    escribir_csv(m / "canales.csv", ["id_canal", "codigo", "tipo_canal"],
                 [[k, c, n] for k, (c, n) in CANALES.items()])
    escribir_csv(m / "estados_pago.csv", ["id_estado", "codigo", "estado_pago"],
                 [[k, c, n] for k, (c, n) in ESTADOS.items()])
    escribir_csv(m / "medios_pago.csv", ["id_medio", "nombre", "marca", "id_tipo", "tipo_medio",
                                         "id_comercio", "razon_social", "cuit"],
                 [[k, n, mc, t, TIPOS_MEDIO[t], 1, "Digital Services SA", "30712345678"]
                  for k, (n, mc, t, _, _) in MEDIOS.items()])
    escribir_csv(m / "mapeo_medios_psp.csv", ["id_psp", "codigo_psp", "id_medio"],
                 [[1, v[3], k] for k, v in MEDIOS.items()] + [[2, v[4], k] for k, v in MEDIOS.items()])
    escribir_csv(m / "psp.csv", ["id_psp", "nombre", "codigo", "pais_op", "plazo_liquidacion_dias",
                                 "cuenta_bancaria"],
                 [[k, n, c, "AR", PLAZO_LIQ[k], cta] for k, (n, c, _, cta) in PSPS.items()])
    escribir_csv(m / "tipos_evento.csv", ["cod_evento", "tipo_evento", "codigo_mp", "codigo_prisma"],
                 [[k, d, mp, pr] for k, (d, mp, pr) in TIPOS_EVENTO.items()])


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--salida", required=True, help="Carpeta destino (se recrea)")
    ap.add_argument("--operaciones", type=int, default=3000, help="Cantidad de órdenes (default 3000)")
    ap.add_argument("--semilla", type=int, default=SEMILLA)
    args = ap.parse_args()

    salida = Path(args.salida)
    if salida.exists():
        shutil.rmtree(salida)
    salida.mkdir(parents=True)
    rng = random.Random(args.semilla)
    gt: list[tuple] = []

    trx, eventos, usuarios, casos = generar_base(args.operaciones, rng)
    liqs, sin_liquidar = generar_liquidaciones(eventos, args.operaciones)
    movs, lotes_estado = generar_banco(liqs)

    escribir_ventas(trx, salida, args.operaciones, rng, gt)
    escribir_eventos(eventos, salida, args.operaciones, rng, gt)
    escribir_liquidaciones(liqs, salida, gt)
    escribir_banco(movs, salida, gt)
    escribir_maestros(usuarios, salida)

    for k, v in sorted(casos.items()):
        gt.append(("conciliacion", "OPERATIVA", k, v))
    eventos_liquidables = sum(1 for e in eventos if e["cod_evento"] != "AUTH")
    gt.append(("conciliacion", "FINANCIERA", "PENDIENTE_LIQUIDACION", sin_liquidar))
    gt.append(("conciliacion", "FINANCIERA", "OK_LIQUIDADO", eventos_liquidables - sin_liquidar))
    for k, v in sorted(lotes_estado.items()):
        gt.append(("conciliacion", "BANCARIA", k, v))

    ctrl = salida / "_control"
    ctrl.mkdir()
    with (ctrl / "ground_truth.jsonl").open("w", encoding="utf-8") as f:
        for tipo, ambito, metrica, valor in gt:
            f.write(json.dumps({"tipo": tipo, "ambito": ambito, "metrica": metrica, "esperado": valor}) + "\n")

    n_archivos = sum(1 for p in salida.rglob("*") if p.is_file())
    print(f"OK: {n_archivos} archivos en {salida}")
    for tipo, ambito, metrica, valor in gt:
        print(f"  {tipo:13s} {ambito:16s} {metrica:30s} {valor:>8}")


if __name__ == "__main__":
    main()
