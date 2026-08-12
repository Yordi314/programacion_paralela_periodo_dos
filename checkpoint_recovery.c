/* =====================================================================
 * checkpoint_recovery.c
 * ---------------------------------------------------------------------
 * Actividad Semana 11: Tolerancia a Fallos - Checkpoint y Rollback
 *
 * Prototipo MPI que:
 *   1) Detecta la existencia de un checkpoint local al arrancar.
 *   2) Restaura el estado si lo encuentra; en caso contrario, inicia
 *      desde cero.
 *   3) Ejecuta un bucle computacional (suma incremental sobre un
 *      vector local por proceso).
 *   4) Cada CKPT_INTERVAL iteraciones toma un checkpoint coordinado
 *      (todos los procesos entran a MPI_Barrier antes y despues de
 *      escribir a disco, garantizando consistencia global).
 *   5) Simula un fallo abortando el proceso FAIL_RANK en la iteracion
 *      FAIL_AT_ITER (solo la primera vez; tras la recuperacion no se
 *      vuelve a disparar el fallo).
 *
 * Compilacion:  mpicc -O2 -Wall -o checkpoint_recovery checkpoint_recovery.c
 * Ejecucion:    mpirun -np 3 ./checkpoint_recovery
 *   - Primera ejecucion: sufre fallo simulado y aborta.
 *   - Segunda ejecucion: recupera el estado y termina el trabajo.
 *
 * Limpieza:     rm -rf checkpoints/
 *
 * Autor:  Yordi Polanco Pujols (24-0937)
 * Curso:  Programacion Paralela y Distribuida - Seccion 02
 * Docente: Joerlyn Morfe
 * Fecha:  Lunes 20 de julio de 2026
 * =====================================================================
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define N_ITER         20    /* iteraciones totales del bucle           */
#define VEC_LEN         8    /* tamanio del vector local por proceso    */
#define CKPT_INTERVAL   5    /* frecuencia del checkpoint coordinado    */
#define FAIL_AT_ITER   12    /* iteracion en la que se simula el fallo  */
#define FAIL_RANK       1    /* rank que sufre el fallo simulado        */
#define CKPT_DIR   "checkpoints"

/* --------- Estructura del estado que se persiste al checkpoint ------ */
typedef struct {
    int    rank;                /* identidad para verificacion             */
    int    iteration;           /* siguiente iteracion a ejecutar          */
    double accumulator;         /* acumulador global del proceso           */
    double vec[VEC_LEN];        /* vector local con datos de trabajo       */
} CheckpointState;

/* --------- Utilidades de ruta y I/O --------------------------------- */
static void ckpt_path(int rank, char *buf, size_t n)
{
    snprintf(buf, n, "%s/ckpt_rank_%d.dat", CKPT_DIR, rank);
}

/* Devuelve 1 si se cargo un checkpoint valido, 0 en caso contrario.   */
static int load_checkpoint(int rank, CheckpointState *st)
{
    char path[256];
    ckpt_path(rank, path, sizeof(path));

    FILE *f = fopen(path, "rb");
    if (f == NULL) return 0;

    size_t r = fread(st, sizeof(*st), 1, f);
    fclose(f);

    if (r != 1 || st->rank != rank) {
        fprintf(stderr, "[Rank %d] Checkpoint corrupto en %s\n", rank, path);
        return 0;
    }
    return 1;
}

/* Guarda el checkpoint de forma atomica: primero a un .tmp y luego
 * se renombra sobre el archivo definitivo. Asi, si el proceso muere
 * mientras se escribe, no queda un archivo a medio guardar.          */
static void save_checkpoint(int rank, const CheckpointState *st)
{
    char path[256], tmp[300];
    ckpt_path(rank, path, sizeof(path));
    snprintf(tmp, sizeof(tmp), "%s.tmp", path);

    FILE *f = fopen(tmp, "wb");
    if (f == NULL) {
        fprintf(stderr, "[Rank %d] No pude abrir %s para escribir\n", rank, tmp);
        return;
    }
    fwrite(st, sizeof(*st), 1, f);
    fflush(f);
    fsync(fileno(f));           /* fuerza el vaciado a disco */
    fclose(f);
    rename(tmp, path);          /* reemplazo atomico */
}

/* --------- Programa principal --------------------------------------- */
int main(int argc, char *argv[])
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 3) {
        if (rank == 0) {
            fprintf(stderr,
                    "[ERROR] Este prototipo requiere al menos 3 procesos MPI.\n");
        }
        MPI_Finalize();
        return EXIT_FAILURE;
    }

    /* El rank 0 crea el directorio de checkpoints. Los demas esperan
     * en la barrera para asegurar que el directorio existe antes de
     * cualquier intento de lectura o escritura.                       */
    if (rank == 0) {
        mkdir(CKPT_DIR, 0755);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    /* ---------- Fase de recuperacion ------------------------------- */
    CheckpointState state;
    int local_recovered = load_checkpoint(rank, &state);

    /* Consistencia global: solo si TODOS los procesos tienen checkpoint
     * la ejecucion se considera "recuperada". Esto evita un estado
     * mixto en el que un proceso reanuda y otro parte de cero.        */
    int all_recovered;
    MPI_Allreduce(&local_recovered, &all_recovered, 1,
                  MPI_INT, MPI_LAND, MPI_COMM_WORLD);

    if (all_recovered) {
        /* Todos rescataron su estado local. Ademas, para el modelo
         * coordinado se toma como iteracion global el minimo de las
         * iteraciones almacenadas por cada proceso: asi el sistema
         * retrocede a un instante consistente entre todos los ranks. */
        int global_iter;
        MPI_Allreduce(&state.iteration, &global_iter, 1,
                      MPI_INT, MPI_MIN, MPI_COMM_WORLD);
        state.iteration = global_iter;

        if (rank == 0) {
            printf("\n===============================================\n");
            printf(" [RECOVERY] Todos los procesos recuperaron.\n");
            printf(" Iteracion global consistente = %d\n", global_iter);
            printf("===============================================\n");
        }
        printf("[Rank %d] Recuperado -> iter=%d, acc=%.4f\n",
               rank, state.iteration, state.accumulator);
    } else {
        /* Inicio limpio: se descartan estados parciales para evitar
         * inconsistencias.                                            */
        if (local_recovered && rank == 0) {
            printf("[Rank %d] Habia checkpoint local pero no todos, "
                   "se reinicia desde cero.\n", rank);
        }
        state.rank        = rank;
        state.iteration   = 0;
        state.accumulator = 0.0;
        for (int i = 0; i < VEC_LEN; ++i) {
            state.vec[i] = (double)(rank * 10 + i);
        }
        if (rank == 0) {
            printf("\n===============================================\n");
            printf(" [START] Inicio limpio, sin checkpoint previo.\n");
            printf("===============================================\n");
        }
        printf("[Rank %d] Inicio desde cero -> iter=0\n", rank);
    }

    fflush(stdout);
    MPI_Barrier(MPI_COMM_WORLD);

    /* ---------- Bucle computacional -------------------------------- */
    for (int it = state.iteration; it < N_ITER; ++it) {

        /* Trabajo local: incrementa cada componente del vector y
         * suma al acumulador. Nada en particular, es solo para
         * ejercitar el mecanismo de checkpoint.                       */
        double partial = 0.0;
        for (int i = 0; i < VEC_LEN; ++i) {
            state.vec[i] += 1.0;
            partial += state.vec[i];
        }
        state.accumulator += partial;
        state.iteration    = it + 1;

        printf("[Rank %d] iter %2d  parcial=%8.2f  acc=%10.2f\n",
               rank, state.iteration, partial, state.accumulator);
        fflush(stdout);

        /* ------- Simulacion de fallo ------------------------------ */
        /* Solo se dispara si NO estamos en modo recuperado. Asi la
         * segunda ejecucion termina normalmente.                     */
        if (!all_recovered &&
            state.iteration == FAIL_AT_ITER &&
            rank == FAIL_RANK)
        {
            fprintf(stderr,
                "\n############################################\n"
                "  [Rank %d] *** FALLO SIMULADO en iter %d ***\n"
                "  Abortando con exit(EXIT_FAILURE). El runtime\n"
                "  MPI derribara a los demas procesos.\n"
                "############################################\n\n",
                rank, state.iteration);
            fflush(stderr);
            exit(EXIT_FAILURE);
        }

        /* ------- Checkpoint coordinado ---------------------------- */
        /* Cada CKPT_INTERVAL iteraciones todos los procesos entran
         * a una barrera para sincronizarse, cada uno vuelca su
         * estado local a disco y vuelven a sincronizar. Este
         * patron es el checkpoint coordinado clasico y garantiza
         * que en disco existe una linea consistente global de
         * puntos de rescate.                                        */
        if (state.iteration % CKPT_INTERVAL == 0) {
            MPI_Barrier(MPI_COMM_WORLD);       /* pre-ckpt sync   */
            save_checkpoint(rank, &state);
            MPI_Barrier(MPI_COMM_WORLD);       /* post-ckpt sync  */

            if (rank == 0) {
                printf("=== CHECKPOINT COORDINADO en iter %d ===\n",
                       state.iteration);
                fflush(stdout);
            }
        }
    }

    /* ---------- Agregacion final ---------------------------------- */
    double total_global = 0.0;
    MPI_Reduce(&state.accumulator, &total_global, 1,
               MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("\n-----------------------------------------------\n");
        printf(" Ejecucion completada correctamente\n");
        printf(" Suma total acumulada por todos los procesos = %.4f\n",
               total_global);
        printf("-----------------------------------------------\n");
    }

    MPI_Finalize();
    return EXIT_SUCCESS;
}
