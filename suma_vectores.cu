/* ============================================================================
 *  suma_vectores.cu
 *  ---------------------------------------------------------------------------
 *  Programa hibrido CPU (OpenMP) + GPU (CUDA) para la suma de dos vectores
 *  grandes:  C[i] = A[i] + B[i]  con  N = 1 M elementos (1 048 576).
 *
 *  Fases:
 *    1) Version CPU paralela con OpenMP  -> tiempo medido con omp_get_wtime()
 *    2) Version GPU con CUDA             -> tiempo total (incluye transferencias)
 *       medido con eventos CUDA, ademas del tiempo del kernel aislado.
 *    3) Comparacion de rendimiento       -> speedup = t_CPU / t_GPU
 *
 *  Autor : Yordi
 *  Curso : Computacion Paralela y Distribuida - UNIBE
 *  Semana: 8 - Programacion en GPU con CUDA y OpenMP Avanzado
 *
 *  Compilacion (equipo con GPU NVIDIA y toolkit CUDA):
 *      nvcc -O3 -Xcompiler -fopenmp suma_vectores.cu -o suma_vectores
 *  Ejecucion:
 *      ./suma_vectores
 *
 *  Compilacion en Google Colab (runtime con GPU T4):
 *      !nvcc -O3 -Xcompiler -fopenmp suma_vectores.cu -o suma_vectores
 *      !./suma_vectores
 * ==========================================================================*/

#include <cstdio>       // printf
#include <cstdlib>      // malloc, free, rand, exit
#include <cmath>        // fabs
#include <omp.h>        // API de OpenMP (pragmas, omp_get_wtime, omp_get_max_threads)
#include <cuda_runtime.h> // API de runtime de CUDA (cudaMalloc, cudaMemcpy, eventos)

/* --------------------------------------------------------------------------
 *  Parametros del problema
 * ------------------------------------------------------------------------*/
#define N (1 << 20)             // 2^20 = 1 048 576 elementos (1 M)
#define THREADS_PER_BLOCK 256   // Hilos por bloque CUDA (multiplo de 32 = warp)

/* --------------------------------------------------------------------------
 *  Macro de verificacion de errores CUDA.
 *  Toda llamada al runtime de CUDA devuelve un cudaError_t; envolverla en
 *  CUDA_CHECK detiene el programa con un mensaje claro si algo falla, en
 *  lugar de continuar con datos corruptos silenciosamente.
 * ------------------------------------------------------------------------*/
#define CUDA_CHECK(call)                                                     \
    do {                                                                     \
        cudaError_t _err = (call);                                           \
        if (_err != cudaSuccess) {                                           \
            fprintf(stderr, "[CUDA ERROR] %s:%d -> %s\n",                    \
                    __FILE__, __LINE__, cudaGetErrorString(_err));           \
            exit(EXIT_FAILURE);                                              \
        }                                                                    \
    } while (0)

/* --------------------------------------------------------------------------
 *  KERNEL CUDA: se ejecuta en la GPU. Cada hilo calcula UN elemento de C.
 *  __global__ indica que es codigo lanzado desde la CPU y corrido en la GPU.
 * ------------------------------------------------------------------------*/
__global__ void add_vectors(const float *A, const float *B, float *C, int n)
{
    // Indice global unico del hilo dentro de toda la malla (grid):
    //   blockIdx.x  = numero de bloque
    //   blockDim.x  = hilos por bloque (256)
    //   threadIdx.x = numero de hilo dentro del bloque
    int i = blockIdx.x * blockDim.x + threadIdx.x;

    // Guarda de frontera: si N no es multiplo exacto de blockDim.x, los
    // hilos sobrantes no deben escribir fuera del arreglo.
    if (i < n) {
        C[i] = A[i] + B[i];
    }
}

int main(void)
{
    const size_t bytes = (size_t)N * sizeof(float); // bytes por vector

    printf("==============================================================\n");
    printf(" Suma de vectores hibrida  CPU (OpenMP) + GPU (CUDA)\n");
    printf(" N = %d elementos  |  %.2f MB por vector\n", N, bytes / (1024.0 * 1024.0));
    printf(" Hilos OpenMP disponibles : %d\n", omp_get_max_threads());
    printf(" Config. CUDA             : %d hilos/bloque\n", THREADS_PER_BLOCK);
    printf("==============================================================\n\n");

    /* ======================================================================
     *  Reserva de memoria en el HOST (CPU) e inicializacion de datos
     * ====================================================================*/
    float *h_A     = (float *)malloc(bytes); // vector A
    float *h_B     = (float *)malloc(bytes); // vector B
    float *h_C_cpu = (float *)malloc(bytes); // resultado calculado en CPU
    float *h_C_gpu = (float *)malloc(bytes); // resultado traido desde la GPU

    if (!h_A || !h_B || !h_C_cpu || !h_C_gpu) {
        fprintf(stderr, "Error: no se pudo reservar memoria en el host.\n");
        return EXIT_FAILURE;
    }

    // Inicializacion determinista: valores conocidos para poder verificar.
    // A[i] = i          ->  suma esperada C[i] = 3*i
    // B[i] = 2*i
    for (int i = 0; i < N; i++) {
        h_A[i] = (float)i;
        h_B[i] = (float)(2 * i);
    }

    /* ======================================================================
     *  FASE 1 - VERSION CPU PARALELA CON OpenMP
     * ====================================================================*/
    double t0_cpu = omp_get_wtime();          // marca de tiempo inicial

    #pragma omp parallel for                   // reparte el bucle entre hilos
    for (int i = 0; i < N; i++) {
        h_C_cpu[i] = h_A[i] + h_B[i];
    }

    double t_cpu = omp_get_wtime() - t0_cpu;   // tiempo transcurrido (segundos)
    printf("[CPU  OpenMP] Tiempo = %.6f ms\n", t_cpu * 1000.0);

    /* ======================================================================
     *  FASE 2 - VERSION GPU CON CUDA
     * ====================================================================*/

    // 2.1  Reserva de memoria en el DEVICE (GPU)
    float *d_A, *d_B, *d_C;
    CUDA_CHECK(cudaMalloc((void **)&d_A, bytes));
    CUDA_CHECK(cudaMalloc((void **)&d_B, bytes));
    CUDA_CHECK(cudaMalloc((void **)&d_C, bytes));

    // 2.2  Configuracion de lanzamiento: cuantos bloques necesitamos para
    //      cubrir N elementos con THREADS_PER_BLOCK hilos cada uno.
    //      (N + T - 1) / T  es la division entera "hacia arriba" (ceil).
    int blocks = (N + THREADS_PER_BLOCK - 1) / THREADS_PER_BLOCK;
    printf("[GPU  CUDA ] Malla: %d bloques x %d hilos = %d hilos totales\n",
           blocks, THREADS_PER_BLOCK, blocks * THREADS_PER_BLOCK);

    // 2.3  "Warm-up": un lanzamiento previo que NO se cronometra. Sirve para
    //      pagar por adelantado la inicializacion del contexto CUDA y no
    //      contaminar la medicion real con ese costo unico.
    add_vectors<<<blocks, THREADS_PER_BLOCK>>>(d_A, d_B, d_C, N);
    CUDA_CHECK(cudaDeviceSynchronize());

    // 2.4  Eventos CUDA para cronometrar con precision en la GPU.
    cudaEvent_t ev_ini, ev_fin, k_ini, k_fin;
    CUDA_CHECK(cudaEventCreate(&ev_ini)); // inicio del bloque GPU completo
    CUDA_CHECK(cudaEventCreate(&ev_fin)); // fin del bloque GPU completo
    CUDA_CHECK(cudaEventCreate(&k_ini));  // inicio del kernel aislado
    CUDA_CHECK(cudaEventCreate(&k_fin));  // fin del kernel aislado

    // ---- Cronometro del TOTAL GPU (incluye transferencias H2D + kernel + D2H) ----
    CUDA_CHECK(cudaEventRecord(ev_ini));

    //   Transferencia Host -> Device (copiamos A y B a la GPU)
    CUDA_CHECK(cudaMemcpy(d_A, h_A, bytes, cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_B, h_B, bytes, cudaMemcpyHostToDevice));

    //   Kernel (cronometrado tambien de forma aislada)
    CUDA_CHECK(cudaEventRecord(k_ini));
    add_vectors<<<blocks, THREADS_PER_BLOCK>>>(d_A, d_B, d_C, N);
    CUDA_CHECK(cudaEventRecord(k_fin));

    //   Transferencia Device -> Host (traemos el resultado C)
    CUDA_CHECK(cudaMemcpy(h_C_gpu, d_C, bytes, cudaMemcpyDeviceToHost));

    CUDA_CHECK(cudaEventRecord(ev_fin));
    CUDA_CHECK(cudaEventSynchronize(ev_fin)); // esperar a que todo termine

    // Chequeo de errores del lanzamiento del kernel
    CUDA_CHECK(cudaGetLastError());

    // 2.5  Lectura de los tiempos medidos (en milisegundos)
    float t_gpu_total_ms = 0.0f, t_kernel_ms = 0.0f;
    CUDA_CHECK(cudaEventElapsedTime(&t_gpu_total_ms, ev_ini, ev_fin));
    CUDA_CHECK(cudaEventElapsedTime(&t_kernel_ms,    k_ini,  k_fin));

    printf("[GPU  CUDA ] Tiempo kernel (solo computo) = %.6f ms\n", t_kernel_ms);
    printf("[GPU  CUDA ] Tiempo TOTAL (con transferencias) = %.6f ms\n", t_gpu_total_ms);

    /* ======================================================================
     *  VERIFICACION DE RESULTADOS: C[i] debe ser igual a A[i] + B[i]
     * ====================================================================*/
    int errores = 0;
    for (int i = 0; i < N; i++) {
        float esperado = h_A[i] + h_B[i];
        if (fabs(h_C_gpu[i] - esperado) > 1e-3f) {
            errores++;
            if (errores <= 5) { // mostrar solo los primeros 5 fallos
                printf("  Discrepancia en i=%d: GPU=%.1f esperado=%.1f\n",
                       i, h_C_gpu[i], esperado);
            }
        }
    }
    if (errores == 0)
        printf("\n[VERIFICACION] OK: los %d elementos coinciden con A[i]+B[i].\n", N);
    else
        printf("\n[VERIFICACION] FALLO: %d elementos incorrectos.\n", errores);

    /* ======================================================================
     *  FASE 3 - COMPARACION DE RENDIMIENTO (SPEEDUP)
     * ====================================================================*/
    double t_cpu_ms      = t_cpu * 1000.0;
    double speedup_total = t_cpu_ms / t_gpu_total_ms; // usando total GPU
    double speedup_kernel= t_cpu_ms / t_kernel_ms;    // usando solo el kernel

    printf("\n---------------------- RESUMEN -------------------------------\n");
    printf(" Tiempo CPU (OpenMP)            : %8.4f ms\n", t_cpu_ms);
    printf(" Tiempo GPU total (con copias)  : %8.4f ms\n", t_gpu_total_ms);
    printf(" Tiempo GPU kernel (solo GPU)   : %8.4f ms\n", t_kernel_ms);
    printf(" Speedup (CPU / GPU total)      : %8.2fx\n", speedup_total);
    printf(" Speedup (CPU / GPU kernel)     : %8.2fx\n", speedup_kernel);
    printf("--------------------------------------------------------------\n");

    /* ======================================================================
     *  Liberacion de recursos
     * ====================================================================*/
    CUDA_CHECK(cudaEventDestroy(ev_ini));
    CUDA_CHECK(cudaEventDestroy(ev_fin));
    CUDA_CHECK(cudaEventDestroy(k_ini));
    CUDA_CHECK(cudaEventDestroy(k_fin));
    CUDA_CHECK(cudaFree(d_A));
    CUDA_CHECK(cudaFree(d_B));
    CUDA_CHECK(cudaFree(d_C));
    free(h_A); free(h_B); free(h_C_cpu); free(h_C_gpu);

    return EXIT_SUCCESS;
}
