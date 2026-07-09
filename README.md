# WordCount con Hadoop MapReduce (Python Streaming)

**Actividad Semana 10 — Big Data**
Universidad Iberoamericana (UNIBE) · 2026
Autor: **Yordi**

Implementación del clásico algoritmo *WordCount* sobre Hadoop MapReduce
usando **Python Streaming**, ejecutado en un clúster Hadoop 3.3.6
pseudo-distribuido levantado con Docker.

---

## 📁 Estructura del repositorio

```
wordcount-hadoop/
├── README.md                       ← este archivo
├── src/
│   ├── mapper.py                   ← Mapper: (línea) → (palabra, 1)
│   └── reducer.py                  ← Reducer: (palabra, [1,1,1,...]) → (palabra, total)
├── scripts/
│   ├── setup_docker.sh             ← Levanta el clúster Hadoop en Docker
│   ├── run_local_test.sh           ← Prueba mapper/reducer con UNIX pipes
│   ├── run_hadoop.sh               ← Ejecuta el job en el clúster
│   ├── performance_test.sh         ← Corre con 1, 2 y 4 reducers
│   └── convert_pdf.py              ← Utilitario para convertir PDFs a .txt
├── data/
│   ├── download_corpus.sh          ← Descarga corpus de Project Gutenberg
│   └── corpus/                     ← Aquí van los .txt de entrada
├── output_sample/                  ← Resultados de ejecución (para el informe)
└── docs/
    └── informe.docx                ← Informe formal UNIBE
```

## 🔧 Requisitos

- **Docker Desktop** (o Docker Engine) — [Descargar](https://www.docker.com/products/docker-desktop)
- **Python 3.8+** (solo para pruebas locales; el contenedor ya lo trae)
- ~2 GB de RAM libres para el contenedor Hadoop

## 🚀 Cómo ejecutar (paso a paso)

### 1. Preparar el corpus

Descargar el corpus de prueba (obras clásicas en español):

```bash
bash data/download_corpus.sh
```

O convertir tus propios PDFs:

```bash
pip install pdfplumber
python3 scripts/convert_pdf.py ruta/a/tus/pdfs/*.pdf
```

### 2. Probar el pipeline localmente (sin Hadoop)

```bash
bash scripts/run_local_test.sh data/corpus/*.txt
```

Esto simula el flujo completo `mapper → sort → reducer` usando pipes de UNIX
y muestra las 30 palabras más frecuentes.

### 3. Levantar el clúster Hadoop

```bash
bash scripts/setup_docker.sh
```

Al terminar, verás las UIs de Hadoop en:

- **NameNode / HDFS:** http://localhost:9870
- **YARN ResourceManager:** http://localhost:8088

### 4. Ejecutar el job MapReduce

Entra al contenedor y ejecuta:

```bash
docker exec -it hadoop-wordcount bash
# Ya adentro del contenedor:
cd /wc
bash scripts/run_hadoop.sh 1     # 1 reducer
```

### 5. Comparativa de rendimiento (1, 2, 4 reducers)

Dentro del contenedor:

```bash
bash scripts/performance_test.sh
```

Los resultados se guardan en `output_sample/performance_summary.csv`.

## 🧠 Flujo MapReduce en este proyecto

```
┌──────────────┐    ┌──────────┐    ┌───────────────┐    ┌───────────┐    ┌───────────┐
│ Archivos     │───►│  Splits  │───►│    MAPPER     │───►│  SHUFFLE  │───►│  REDUCER  │
│ .txt en HDFS │    │ (bloques)│    │ (palabra, 1)  │    │  + SORT   │    │ (palabra, │
└──────────────┘    └──────────┘    └───────────────┘    └───────────┘    │   total)  │
                                          │                                └───────────┘
                                          ▼                                       │
                                    ┌───────────┐                                 ▼
                                    │ COMBINER  │                          ┌───────────┐
                                    │ (opcional:│                          │ HDFS      │
                                    │ reduce    │                          │ part-*    │
                                    │ local)    │                          └───────────┘
                                    └───────────┘
```

- **Mapper:** cada línea del texto se tokeniza y por cada palabra se emite `(palabra, 1)`.
- **Combiner:** reutilizamos el mismo reducer localmente en cada nodo para reducir tráfico de red.
- **Shuffle & Sort:** Hadoop agrupa por clave y ordena — es transparente para nosotros.
- **Reducer:** suma todos los 1s por palabra y emite `(palabra, total)`.

## 📊 Explicación línea por línea

Ver el informe `docs/informe.docx` — cada línea del `mapper.py` y `reducer.py`
está comentada en el código y explicada en el informe.

## 📹 Video de presentación

Ver enlace incluido en el informe (`docs/informe.docx`).

## 📚 Referencias

- Apache Hadoop MapReduce Tutorial — [WordCount v1.0](https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html)
- Dean, J. & Ghemawat, S. (2008). *MapReduce: Simplified Data Processing on Large Clusters*. Communications of the ACM, 51(1), 107–113.
- White, T. (2015). *Hadoop: The Definitive Guide* (4ª ed.). O'Reilly Media.

## 📄 Licencia

Trabajo académico. Corpus de prueba: dominio público (Project Gutenberg).
