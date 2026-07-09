#!/usr/bin/env bash
# run_all_experiments.sh — Ejecuta el job WordCount con distintas
# configuraciones y genera un CSV comparativo para el informe.
#
# Uso:  bash scripts/run_all_experiments.sh

set -euo pipefail

INPUT="data/corpus"
CSV="output_sample/performance_summary.csv"

mkdir -p output_sample

echo "num_mappers,num_reducers,combiner,tiempo_total,tiempo_map,tiempo_shuffle,tiempo_reduce,map_output_records" > "${CSV}"

run_and_record() {
  local M=$1 R=$2 COMB=$3 LABEL=$4

  local FLAG=""
  local COMB_TXT="OFF"
  if [ "$COMB" = "1" ]; then
    FLAG="--combiner"
    COMB_TXT="ON"
  fi

  echo ""
  echo "############################################################"
  echo "### CORRIDA: ${LABEL} (M=${M}, R=${R}, combiner=${COMB_TXT})"
  echo "############################################################"

  # Ejecutamos y capturamos las métricas del resumen final
  local OUT_DIR="output_sample/output-m${M}-r${R}-c${COMB}"
  python3 src/mapreduce_runner.py \
    --input "${INPUT}" \
    --output "${OUT_DIR}" \
    --mappers "${M}" \
    --reducers "${R}" \
    ${FLAG} \
    | tee /tmp/last_run.log

  # Extraemos los tiempos del log
  local T_TOTAL=$(grep "Tiempo total:"    /tmp/last_run.log | awk '{print $3}' | tr -d 's')
  local T_MAP=$(grep -E "Map:\s"          /tmp/last_run.log | awk '{print $3}' | tr -d 's')
  local T_SHUF=$(grep "Shuffle+Sort:"     /tmp/last_run.log | awk '{print $3}' | tr -d 's')
  local T_RED=$(grep -E "Reduce:\s"       /tmp/last_run.log | awk '{print $3}' | tr -d 's')
  local RECORDS=$(grep "Map output"       /tmp/last_run.log | awk '{print $4}' | tr -d ',')

  echo "${M},${R},${COMB_TXT},${T_TOTAL},${T_MAP},${T_SHUF},${T_RED},${RECORDS}" >> "${CSV}"
}

# ==== EXPERIMENTOS ====
# Escenario A: variar reducers, combiner ON, mappers fijos
run_and_record 4 1 1 "Baseline_1R"
run_and_record 4 2 1 "Escala_2R"
run_and_record 4 4 1 "Escala_4R"

# Escenario B: efecto del combiner (mismo M y R)
run_and_record 4 2 0 "Sin_combiner"

echo ""
echo "############################################################"
echo "### RESUMEN COMPARATIVO"
echo "############################################################"
column -t -s',' "${CSV}"

echo ""
echo "Resultados guardados en: ${CSV}"
echo "Archivos part-r-* en:    output_sample/output-m*-r*-c*/"
