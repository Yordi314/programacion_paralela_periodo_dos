# programacion_paralela_periodo_dos# Suma de vectores híbrida: CPU (OpenMP) + GPU (CUDA)

Actividad Semana 8 — Computación Paralela y Distribuida (UNIBE).

Programa que suma dos vectores de **N = 1 048 576** elementos (`C[i] = A[i] + B[i]`)
en tres fases: CPU paralela con OpenMP, GPU con CUDA, y comparación de rendimiento
(*speedup*).

## Archivo
- `suma_vectores.cu` — código fuente completo (kernel CUDA + versión OpenMP + medición + verificación).

## Requisitos
- GPU NVIDIA con CUDA Toolkit instalado (o Google Colab con GPU T4).
- Compilador con soporte OpenMP (gcc).

## Compilar y ejecutar (equipo local)
```bash
nvcc -O3 -Xcompiler -fopenmp suma_vectores.cu -o suma_vectores
./suma_vectores
```
El flag `-Xcompiler -fopenmp` reenvía `-fopenmp` al compilador de host para habilitar OpenMP.

## Ejecutar en Google Colab
1. Entorno de ejecución → Cambiar tipo de entorno de ejecución → **GPU (T4)**.
2. Subir `suma_vectores.cu`.
3. En una celda:
```python
!nvcc -O3 -Xcompiler -fopenmp suma_vectores.cu -o suma_vectores
!./suma_vectores
```

## Salida esperada
El programa imprime el tiempo de la CPU (OpenMP), el tiempo del kernel de la GPU,
el tiempo total de la GPU (con transferencias), la verificación de resultados y los
*speedups*. Los tiempos dependen del hardware.
