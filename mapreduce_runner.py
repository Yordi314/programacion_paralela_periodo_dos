#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
mapreduce_runner.py — Simulador fiel del runtime de Hadoop MapReduce
en Python, usando multiprocessing para paralelismo real.

Emula las cinco fases canónicas del framework Hadoop:

    1. INPUT SPLITS   → divide los archivos en fragmentos por línea
    2. MAP            → N mappers en paralelo (multiprocessing.Pool)
    3. PARTITION      → hash(key) mod num_reducers
    4. SHUFFLE + SORT → agrupa por partición y ordena por clave
    5. REDUCE         → R reducers en paralelo escribiendo part-r-XXXXX

Uso:
    python3 mapreduce_runner.py --input data/corpus/ --output output_hadoop/ \\
                                --mappers 4 --reducers 2 [--combiner]

Los archivos de salida siguen la convención Hadoop:
    output_hadoop/
        _SUCCESS
        part-r-00000
        part-r-00001
        ...
"""

import argparse
import hashlib
import multiprocessing as mp
import os
import re
import shutil
import sys
import time
from pathlib import Path
from collections import defaultdict

# Fuerza UTF-8 en stdin/stdout (crítico en Windows)
sys.stdin.reconfigure(encoding="utf-8", errors="replace")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TOKEN_RE = re.compile(r"\w+", re.UNICODE)


# ============================================================
# FASE 1: INPUT SPLITS
# ============================================================
def make_splits(input_dir: Path, num_splits: int) -> list:
    """
    Emula el InputSplit de Hadoop: distribuye las líneas de todos los
    archivos de entrada en `num_splits` fragmentos de tamaño balanceado.
    Cada split será procesado por un Mapper distinto en paralelo.
    """
    files = sorted(input_dir.glob("*.txt"))
    if not files:
        raise FileNotFoundError(f"No hay archivos .txt en {input_dir}")

    # Leemos todas las líneas de todos los archivos
    all_lines = []
    for f in files:
        with f.open("r", encoding="utf-8", errors="replace") as fp:
            all_lines.extend(fp.readlines())

    # Repartimos las líneas en num_splits fragmentos de tamaño similar
    total = len(all_lines)
    chunk_size = (total + num_splits - 1) // num_splits
    splits = [
        all_lines[i:i + chunk_size]
        for i in range(0, total, chunk_size)
    ]
    return splits, total


# ============================================================
# FASE 2: MAP (ejecutado en paralelo por multiprocessing.Pool)
# ============================================================
def mapper(split_lines):
    """
    Procesa un split completo. Es la función Map: por cada línea,
    tokeniza y emite pares (palabra, 1).
    Retorna una lista de tuplas (palabra, 1) equivalente al STDOUT
    del mapper.py de Hadoop Streaming.
    """
    output = []
    for line in split_lines:
        line = line.strip().lower()
        if not line:
            continue
        for token in TOKEN_RE.findall(line):
            if token.isdigit():
                continue
            output.append((token, 1))
    return output


# ============================================================
# FASE 2.5 (opcional): COMBINER (mini-reduce local en el mapper)
# ============================================================
def combiner(pairs):
    """
    Agrega localmente pares (palabra, 1) del mismo Mapper antes del
    shuffle. Reduce dramáticamente el volumen de datos que cruza la red
    en un clúster real. Solo se aplica cuando la operación es
    asociativa y conmutativa (como la suma del WordCount).
    """
    local = defaultdict(int)
    for word, count in pairs:
        local[word] += count
    return list(local.items())


def map_task(args):
    """Wrapper para multiprocessing: aplica mapper + (opcional) combiner."""
    split_lines, use_combiner = args
    pairs = mapper(split_lines)
    if use_combiner:
        pairs = combiner(pairs)
    return pairs


# ============================================================
# FASE 3: PARTITION (hash-based, replica de HashPartitioner de Hadoop)
# ============================================================
def partition(key: str, num_reducers: int) -> int:
    """
    Emula HashPartitioner: partition = (key.hashCode() & MAX_INT) % R
    Usamos MD5 truncado en vez de Java hashCode para determinismo
    cross-platform. La distribución estadística es equivalente.
    """
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(h[:8], 16) % num_reducers


# ============================================================
# FASE 4: SHUFFLE + SORT
# ============================================================
def shuffle_and_sort(all_map_output: list, num_reducers: int) -> list:
    """
    Recoge la salida de todos los mappers, particiona cada par por su
    clave y ordena cada partición alfabéticamente por clave.
    Retorna una lista de longitud num_reducers: buckets[r] contiene
    todos los pares (palabra, valor) destinados al reducer r.
    """
    buckets = [[] for _ in range(num_reducers)]
    for pairs in all_map_output:
        for word, count in pairs:
            buckets[partition(word, num_reducers)].append((word, count))
    # Cada reducer recibe su bucket ORDENADO por clave (así lo hace Hadoop)
    for r in range(num_reducers):
        buckets[r].sort(key=lambda kv: kv[0])
    return buckets


# ============================================================
# FASE 5: REDUCE (paralelo)
# ============================================================
def reducer(bucket_and_path):
    """
    Recibe todos los pares (palabra, count) ordenados por clave para
    UNA partición y escribe la salida en un archivo part-r-XXXXX.
    Aprovecha el orden para agregar con memoria O(1) por clave activa.
    """
    bucket, out_path = bucket_and_path
    current_word = None
    current_count = 0
    lines_written = 0

    with open(out_path, "w", encoding="utf-8") as out:
        for word, count in bucket:
            if word == current_word:
                current_count += count
            else:
                if current_word is not None:
                    out.write(f"{current_word}\t{current_count}\n")
                    lines_written += 1
                current_word = word
                current_count = count
        if current_word is not None:
            out.write(f"{current_word}\t{current_count}\n")
            lines_written += 1

    return lines_written


# ============================================================
# ORCHESTRATOR — equivalente al JobTracker/ApplicationMaster de Hadoop
# ============================================================
def run_job(input_dir: Path, output_dir: Path,
            num_mappers: int, num_reducers: int, use_combiner: bool):
    """Coordina las cinco fases y reporta métricas por cada una."""

    # Limpiamos el directorio de salida (Hadoop también falla si existe)
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    print("=" * 70)
    print(f"  MapReduce Job: WordCount")
    print(f"  Mappers: {num_mappers}   Reducers: {num_reducers}   "
          f"Combiner: {'ON' if use_combiner else 'OFF'}")
    print("=" * 70)

    t0 = time.time()

    # FASE 1 — INPUT SPLITS
    print("\n[FASE 1/5] Generando input splits...")
    splits, total_lines = make_splits(input_dir, num_mappers)
    t_split = time.time() - t0
    print(f"    → {len(splits)} splits generados de {total_lines:,} líneas totales "
          f"({t_split:.2f}s)")

    # FASE 2 — MAP (en paralelo con multiprocessing.Pool)
    print(f"\n[FASE 2/5] Ejecutando {num_mappers} tareas Map en paralelo...")
    t_map_start = time.time()
    with mp.Pool(processes=num_mappers) as pool:
        map_outputs = pool.map(
            map_task,
            [(s, use_combiner) for s in splits]
        )
    t_map = time.time() - t_map_start
    total_map_records = sum(len(o) for o in map_outputs)
    print(f"    → {total_map_records:,} pares intermedios emitidos "
          f"({'combiner activo' if use_combiner else 'sin combiner'}) "
          f"({t_map:.2f}s)")

    # FASE 3 + 4 — PARTITION + SHUFFLE + SORT
    print(f"\n[FASE 3-4/5] Particionando (hash) y ordenando por clave...")
    t_shuffle_start = time.time()
    buckets = shuffle_and_sort(map_outputs, num_reducers)
    t_shuffle = time.time() - t_shuffle_start
    print(f"    → {num_reducers} particiones creadas")
    for r, b in enumerate(buckets):
        print(f"    → Partición {r}: {len(b):,} pares")
    print(f"    ({t_shuffle:.2f}s)")

    # FASE 5 — REDUCE (paralelo)
    print(f"\n[FASE 5/5] Ejecutando {num_reducers} tareas Reduce en paralelo...")
    t_reduce_start = time.time()
    out_paths = [
        str(output_dir / f"part-r-{i:05d}")
        for i in range(num_reducers)
    ]
    with mp.Pool(processes=num_reducers) as pool:
        lines_per_reducer = pool.map(
            reducer,
            list(zip(buckets, out_paths))
        )
    t_reduce = time.time() - t_reduce_start
    total_unique = sum(lines_per_reducer)
    print(f"    → {total_unique:,} palabras únicas escritas en {num_reducers} archivos "
          f"part-r-* ({t_reduce:.2f}s)")

    # Bandera _SUCCESS (igual que Hadoop)
    (output_dir / "_SUCCESS").touch()

    # RESUMEN
    t_total = time.time() - t0
    print("\n" + "=" * 70)
    print("  JOB COMPLETED SUCCESSFULLY")
    print("=" * 70)
    print(f"  Tiempo total:         {t_total:.2f}s")
    print(f"    ├─ Splits:          {t_split:.2f}s")
    print(f"    ├─ Map:             {t_map:.2f}s")
    print(f"    ├─ Shuffle+Sort:    {t_shuffle:.2f}s")
    print(f"    └─ Reduce:          {t_reduce:.2f}s")
    print(f"  Registros Map input:  {total_lines:,} líneas")
    print(f"  Registros Map output: {total_map_records:,} pares (k,v)")
    print(f"  Palabras únicas:      {total_unique:,}")
    print(f"  Output:               {output_dir}/")
    print("=" * 70)

    return {
        "num_mappers": num_mappers,
        "num_reducers": num_reducers,
        "combiner": use_combiner,
        "total_time": round(t_total, 2),
        "map_time": round(t_map, 2),
        "shuffle_time": round(t_shuffle, 2),
        "reduce_time": round(t_reduce, 2),
        "map_records_in": total_lines,
        "map_records_out": total_map_records,
        "unique_words": total_unique,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Simulador de Hadoop MapReduce en Python"
    )
    parser.add_argument("--input", required=True, type=Path,
                        help="Directorio con archivos .txt de entrada")
    parser.add_argument("--output", required=True, type=Path,
                        help="Directorio de salida (se sobrescribe)")
    parser.add_argument("--mappers", type=int, default=4,
                        help="Número de tareas Map paralelas (default: 4)")
    parser.add_argument("--reducers", type=int, default=1,
                        help="Número de tareas Reduce paralelas (default: 1)")
    parser.add_argument("--combiner", action="store_true",
                        help="Activa el combiner (agregación local en Map)")
    args = parser.parse_args()

    run_job(args.input, args.output,
            args.mappers, args.reducers, args.combiner)


if __name__ == "__main__":
    main()
