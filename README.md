# Actividad Semana 9 — MPI Avanzado: Comunicaciones Colectivas

**Curso:** Programación Paralela y Distribuida — Sección 02
**Estudiante:** Yordi Polanco Pujols — 24-0937
**Docente:** Joerlyn Morfe
**Fecha de entrega:** Lunes 6 de julio de 2026
**Universidad:** Universidad Iberoamericana (UNIBE)

---

## Descripción

Programa en C que calcula el promedio de un conjunto de números generados aleatoriamente por cada proceso MPI. La coordinación entre procesos se resuelve con dos primitivas colectivas:

- **`MPI_Bcast`** — difunde el tamaño `N` desde el rank 0 y, más tarde, el promedio calculado.
- **`MPI_Reduce`** — agrega con `MPI_SUM` las sumas parciales de todos los procesos en el rank 0.

`MPI_Scatter` y `MPI_Gather` se discuten en el informe pero no se emplean, porque los valores se generan localmente en cada proceso (no viven en la raíz) y la agregación necesaria es un escalar por proceso.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `promedio_colectivo.c` | Código fuente del programa MPI. |
| `Informe_Semana9_MPI_Colectivas.docx` | Informe en formato APA/UNIBE. |
| `salida_run_N1000.txt` | Salida de una ejecución de referencia con `N=1000`, `P=4`. |
| `salida_run_N8_P6.txt` | Salida con `N=8`, `P=6` para validar la difusión del promedio. |
| `README.md` | Este archivo. |

## Requisitos

- Compilador C (probado con `gcc 13.3.0`).
- OpenMPI 4.x (probado con `4.1.6`).
- Ubuntu 24.04 o cualquier distribución Linux con soporte para OpenMPI.

Instalación en Ubuntu / Debian:

```bash
sudo apt-get update
sudo apt-get install -y openmpi-bin openmpi-common libopenmpi-dev
```

## Compilación

```bash
mpicc -O2 -Wall -o promedio_colectivo promedio_colectivo.c
```

## Ejecución

### Modo interactivo

```bash
mpirun --oversubscribe -np 4 ./promedio_colectivo
```

El programa pedirá el valor de `N` por la entrada estándar y luego imprimirá las sumas parciales, la suma total, el promedio y el mismo promedio recibido por cada proceso.

### Modo automatizado (por pipe)

```bash
echo "1000" | mpirun --oversubscribe -np 4 ./promedio_colectivo
```

## Ejemplo de salida

```
=====================================================
 Calculo distribuido del promedio de N*P numeros
 Procesos activos: 4
=====================================================
Ingrese la cantidad N de valores por proceso:
[Rank 0] Genero 1000 valores  |  suma parcial =   50802.5034
[Rank 1] Genero 1000 valores  |  suma parcial =   49679.6350
[Rank 3] Genero 1000 valores  |  suma parcial =   50866.2949
[Rank 2] Genero 1000 valores  |  suma parcial =   48677.1283
-----------------------------------------------------
Suma total   =    200025.5617
Elementos    = 4 procesos x 1000 valores = 4000
Promedio     =      50.006390
-----------------------------------------------------
[Rank 0..3] Promedio global recibido = 50.006390
```

## Validación

Los valores se generan uniformemente en `[0, 100]`, por lo que el promedio teórico esperado es 50. En corridas con `N × P ≥ 4000` el error absoluto respecto al valor teórico se mantiene por debajo de 0.05, consistente con la ley de los grandes números.

## Estructura del programa

1. `MPI_Init` / `MPI_Comm_rank` / `MPI_Comm_size` — inicialización.
2. Lectura de `N` en el rank 0.
3. `MPI_Bcast` de `N` a todos los procesos.
4. Generación local de `N` valores en `[0, 100]` y cálculo de la suma parcial.
5. `MPI_Reduce` con `MPI_SUM` para agregar las sumas parciales en el rank 0.
6. El rank 0 calcula `promedio = suma_total / (N * size)`.
7. `MPI_Bcast` del promedio hacia todos los procesos.
8. Cada proceso imprime el promedio recibido junto con su rank.
9. `MPI_Finalize`.

## Notas sobre sincronización

Todas las llamadas colectivas de MPI bloquean hasta que todos los procesos del comunicador entran en la primitiva. Este comportamiento se aprovecha para razonar sobre la corrección del algoritmo, pero exige cuidar tres puntos:

- El **balance de carga** entre procesos, para que uno lento no bloquee a los demás.
- La **simetría en los argumentos** (raíz, tipo, cantidad, operación) en todos los procesos.
- El **manejo de errores** antes de las colectivas: cualquier `MPI_Abort` en la raíz debe hacerse antes de bloquear al resto.

## Licencia

Trabajo académico. Uso educativo permitido con atribución.
