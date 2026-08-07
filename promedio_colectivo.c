/* =====================================================================
 * promedio_colectivo.c
 * ---------------------------------------------------------------------
 * Actividad Semana 9: MPI Avanzado - Comunicaciones Colectivas
 *   MPI_Bcast, MPI_Reduce
 *
 * Descripcion:
 *   Cada proceso genera N valores aleatorios y calcula su suma parcial.
 *   La suma total se agrega en el proceso raiz mediante MPI_Reduce y el
 *   promedio calculado se distribuye a todos los procesos con MPI_Bcast.
 *
 * Compilacion:  mpicc -O2 -Wall -o promedio_colectivo promedio_colectivo.c
 * Ejecucion:    mpirun -np 4 ./promedio_colectivo
 *
 * Autor:  Yordi Polanco Pujols (24-0937)
 * Curso:  Programacion Paralela y Distribuida - Seccion 02
 * Docente: Joerlyn Morfe
 * Fecha:  Lunes 6 de julio de 2026
 * =====================================================================
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#define VALOR_MIN 0.0
#define VALOR_MAX 100.0

int main(int argc, char *argv[])
{
    int rank, size;
    int N = 0;                  /* cantidad de valores por proceso   */
    double suma_local = 0.0;    /* suma parcial calculada por cada proceso */
    double suma_total = 0.0;    /* suma agregada en el proceso raiz  */
    double promedio = 0.0;      /* promedio global distribuido a todos */

    /* ---------- 1. Inicializacion del entorno MPI ---------------- */
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    /* ---------- 2. Solicitud y difusion de N (MPI_Bcast) --------- */
    if (rank == 0) {
        printf("=====================================================\n");
        printf(" Calculo distribuido del promedio de N*P numeros\n");
        printf(" Procesos activos: %d\n", size);
        printf("=====================================================\n");
        printf("Ingrese la cantidad N de valores por proceso: ");
        fflush(stdout);
        if (scanf("%d", &N) != 1 || N <= 0) {
            fprintf(stderr, "[ERROR] Entrada invalida. Se requiere N > 0.\n");
            MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
        }
    }

    /* El proceso raiz difunde el valor de N a todos los demas
     * procesos del comunicador MPI_COMM_WORLD. La llamada bloquea
     * a cada proceso hasta que todos participen en la operacion. */
    MPI_Bcast(&N, 1, MPI_INT, 0, MPI_COMM_WORLD);

    /* ---------- 3. Generacion local y suma parcial --------------- */
    /* Cada proceso usa una semilla distinta combinando el tiempo actual
     * y su rank, para que las secuencias pseudoaleatorias no coincidan. */
    srand((unsigned) time(NULL) + rank * 1000);

    double *valores = (double *) malloc(sizeof(double) * (size_t) N);
    if (valores == NULL) {
        fprintf(stderr, "[Rank %d] Fallo al reservar memoria.\n", rank);
        MPI_Abort(MPI_COMM_WORLD, EXIT_FAILURE);
    }

    for (int i = 0; i < N; ++i) {
        double r = (double) rand() / (double) RAND_MAX;   /* [0,1] */
        valores[i] = VALOR_MIN + r * (VALOR_MAX - VALOR_MIN);
        suma_local += valores[i];
    }

    printf("[Rank %d] Genero %d valores  |  suma parcial = %12.4f\n",
           rank, N, suma_local);
    fflush(stdout);

    /* Barrera opcional para ordenar la salida de las lineas de log
     * antes de que aparezca el resultado global. No es necesaria para
     * la correccion del calculo. */
    MPI_Barrier(MPI_COMM_WORLD);

    /* ---------- 4. Reduccion de la suma (MPI_Reduce) ------------- */
    /* Todos los procesos aportan su suma_local; el resultado agregado
     * mediante MPI_SUM queda unicamente en suma_total del proceso 0. */
    MPI_Reduce(&suma_local, &suma_total, 1, MPI_DOUBLE,
               MPI_SUM, 0, MPI_COMM_WORLD);

    /* ---------- 5. Calculo del promedio y difusion --------------- */
    if (rank == 0) {
        promedio = suma_total / ((double) N * (double) size);
        printf("-----------------------------------------------------\n");
        printf("Suma total   = %14.4f\n", suma_total);
        printf("Elementos    = %d procesos x %d valores = %d\n",
               size, N, size * N);
        printf("Promedio     = %14.6f\n", promedio);
        printf("-----------------------------------------------------\n");
        fflush(stdout);
    }

    /* Se difunde el promedio calculado desde el rank 0 hacia todos
     * los procesos del comunicador. */
    MPI_Bcast(&promedio, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    /* ---------- 6. Cada proceso imprime el promedio recibido ----- */
    MPI_Barrier(MPI_COMM_WORLD);   /* solo para ordenar la salida */
    printf("[Rank %d] Promedio global recibido = %.6f\n", rank, promedio);
    fflush(stdout);

    /* ---------- 7. Liberacion y finalizacion --------------------- */
    free(valores);
    MPI_Finalize();
    return EXIT_SUCCESS;
}
