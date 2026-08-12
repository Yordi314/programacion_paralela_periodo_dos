# Actividad Semana 11 — Tolerancia a Fallos: Checkpoint y Rollback Recovery

## Descripción

Prototipo en C con MPI que implementa **checkpointing coordinado** y **rollback recovery** para tolerar fallos en un cómputo distribuido. Tres procesos MPI ejecutan un bucle iterativo, toman puntos de rescate sincronizados cada cierto número de iteraciones y, ante un fallo simulado, la re-ejecución detecta los checkpoints en disco y reanuda desde el último instante consistente global.

## Objetivos cubiertos

- Detección de checkpoint previo antes de arrancar el trabajo.
- Restauración del estado guardado en disco.
- Bucle computacional (actualización incremental sobre un vector local + acumulador).
- Simulación de fallo con `exit(EXIT_FAILURE)`.
- **Checkpoint coordinado** usando `MPI_Barrier` antes y después de la escritura.
- Sincronización global de la iteración de rescate con `MPI_Allreduce` + `MPI_LAND` y `MPI_MIN`.
- Escritura atómica (`.tmp` + `rename()`) para evitar archivos corruptos.

## Contenido del repositorio

| Archivo | Descripción |
|---|---|
| `checkpoint_recovery.c` | Código fuente del prototipo. |
| `Informe_Semana11_Checkpoint_Rollback.docx` | Informe en formato APA/UNIBE. |
| `run1_falla.txt` | Salida capturada de la ejecución con fallo. |
| `run2_recovery.txt` | Salida capturada de la ejecución tras recuperación. |
| `checkpoints/` | Directorio con los archivos `.dat` de estado (se genera al ejecutar). |
| `README.md` | Este archivo. |

## Requisitos

- Compilador C (probado con `gcc 13.3.0`).
- OpenMPI 4.x (probado con `4.1.6`).
- Sistema POSIX (Linux / macOS) para `rename()` atómico y `fsync()`.

Instalación en Ubuntu / Debian:

```bash
sudo apt-get update
sudo apt-get install -y openmpi-bin openmpi-common libopenmpi-dev
```

## Compilación

```bash
mpicc -O2 -Wall -o checkpoint_recovery checkpoint_recovery.c
```

## Cómo simular el fallo y la recuperación

### Paso 1 — Ejecución inicial (fallo simulado)

Con el directorio de checkpoints limpio:

```bash
rm -rf checkpoints
mpirun --oversubscribe -np 3 ./checkpoint_recovery
```

- Los tres procesos parten desde cero.
- Se toma checkpoint coordinado en las iteraciones 5 y 10.
- En la iteración 12, el `rank 1` ejecuta `exit(EXIT_FAILURE)`.
- El runtime OpenMPI derriba a los otros procesos.
- En `checkpoints/` quedan los archivos `ckpt_rank_0.dat`, `ckpt_rank_1.dat`, `ckpt_rank_2.dat` con el estado de la iteración 10.

### Paso 2 — Recuperación

**Sin borrar el directorio `checkpoints/`**, se ejecuta el mismo comando:

```bash
mpirun --oversubscribe -np 3 ./checkpoint_recovery
```

- Cada proceso lee su archivo local y se recupera al iter 10.
- `MPI_Allreduce` con `MPI_LAND` verifica que los tres tienen checkpoint.
- `MPI_Allreduce` con `MPI_MIN` fija la iteración global consistente.
- El fallo simulado **no se dispara** porque la bandera `all_recovered` vale 1.
- La ejecución continúa hasta la iteración 20 y termina con éxito.
- Se imprime la suma total: **11520.0000**.

### Paso 3 — Limpieza

```bash
rm -rf checkpoints
```

## Parámetros configurables

Están al inicio del código como `#define`:

| Constante | Valor por defecto | Descripción |
|---|---|---|
| `N_ITER` | 20 | Total de iteraciones. |
| `VEC_LEN` | 8 | Tamaño del vector local por proceso. |
| `CKPT_INTERVAL` | 5 | Cada cuántas iteraciones se toma checkpoint. |
| `FAIL_AT_ITER` | 12 | Iteración del fallo simulado. |
| `FAIL_RANK` | 1 | Rank que aborta. |

## Validación del resultado

El resultado teórico se puede calcular en cerrado. El rank *r* inicia con `vec[i] = 10*r + i` para `i = 0..7`, así que la suma parcial de la iteración *k* es `28 + 80*r + 8*k`. Sumando de *k = 1* a *20*:

| Rank | Acumulador final teórico | Reportado |
|---|---|---|
| 0 | 2240 | 2240 ✅ |
| 1 | 3840 | 3840 ✅ |
| 2 | 5440 | 5440 ✅ |
| **Total** | **11520** | **11520.0000 ✅** |

Coincidencia bit a bit → el rollback no introduce inconsistencias.

## Checkpoint coordinado vs. no coordinado (resumen)

| Aspecto | Coordinado (este prototipo) | No coordinado |
|---|---|---|
| Sincronización | `MPI_Barrier` global. | Cada proceso decide autónomamente. |
| Consistencia global | Garantizada por construcción. | Requiere logs de mensajes y reconstrucción causal. |
| Efecto dominó | Imposible. | Puede ocurrir. |
| Complejidad de recuperación | Baja. | Alta. |
| Escalabilidad | Limitada por la barrera. | Mejor en sistemas muy grandes. |
