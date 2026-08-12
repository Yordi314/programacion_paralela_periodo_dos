# Serverless Actors — Semana 14

Microservicio basado en **Modelo de Actores (Akka Classic)** desplegado como función **Serverless (AWS Lambda)** con endpoint HTTP.

**Curso:** Programación Paralela y Distribuida — Sección 02
**Estudiante:** Yordi Polanco Pujols — 24-0937
**Docente:** Joerlyn Morfe
**Fecha de entrega:** Lunes 10 de agosto de 2026
**Universidad:** Universidad Iberoamericana (UNIBE)

---

## Objetivos cubiertos

- ✅ Actor **supervisor** que gestiona un pool de actores **worker**.
- ✅ Patrón de **supervisión con reinicio automático** (`OneForOneStrategy` → `restart`).
- ✅ Encapsulado en una **función Lambda** que expone un endpoint **HTTP POST /task**.
- ✅ Recibe **JSON**, despacha trabajo a actores, devuelve **JSON**.
- ✅ **Ejecución asíncrona** (`Patterns.ask` retorna `CompletionStage`) con timeout.
- ✅ **Tolerancia a fallos**: worker que lanza excepción → supervisor lo reinicia sin afectar a los demás.
- ✅ **Warm-start reuse** del `ActorSystem` para amortizar el arranque entre invocaciones.

## Arquitectura

```
Cliente HTTP  →  API Gateway (HTTP API)  →  Lambda (Handler)
                                                   │
                                                   ▼
                                     ┌── SupervisorActor ──┐
                                     │  OneForOneStrategy  │
                                     │   (restart on RTE)  │
                                     └─┬──────┬──────┬─────┘
                                       │      │      │
                                     worker-0  worker-1  worker-2
```

## Estructura

```
serverless-actors/
├── pom.xml                              Build Maven + shade plugin
├── template.yaml                        AWS SAM (Lambda + HTTP API)
├── deploy.sh                            Script build + deploy
├── README.md                            Este archivo
├── src/main/java/com/yordi/serverless/
│   ├── Handler.java                     Entry point Lambda (API Gateway)
│   ├── ActorSystemHolder.java           Singleton ActorSystem para reuso warm
│   ├── LocalRunner.java                 Runner local para pruebas sin AWS
│   ├── actors/
│   │   ├── SupervisorActor.java         Supervisor + estrategia + routing
│   │   └── WorkerActor.java             Worker que procesa Task
│   └── messages/
│       ├── Task.java                    Mensaje entrante (inmutable)
│       └── TaskResult.java              Mensaje saliente (inmutable)
└── src/main/resources/
    ├── application.conf                 Configuración Akka
    └── simplelogger.properties          Configuración logs
```

## Requisitos

- **JDK 11+** (probado con OpenJDK 21).
- **Maven 3.6+**.
- **AWS CLI** y **AWS SAM CLI** (solo para desplegar en AWS).

## Compilación

```bash
mvn clean package
```

Produce `target/serverless-actors.jar` (fat jar listo para Lambda).

## Ejecución local (sin AWS)

```bash
java -cp target/serverless-actors.jar com.yordi.serverless.LocalRunner
```

`LocalRunner` simula 6 invocaciones consecutivas:

1. `SUM` con `[10,20,30,40]` → `100` (cold start ~1s)
2. `TRANSFORM` con `"Yordi"` → `"IDROY"` (warm ~2ms)
3. `FAIL_ME` → 500 + supervisor reinicia el worker
4. `TRANSFORM` con `"Akka"` → `"AKKA"` (tras el restart)
5. `SUM` con `[100,200,300]` → `600`
6. Bad request (falta `numbers`) → 400

## Despliegue en AWS

Primera vez (interactivo):

```bash
./deploy.sh --guided
```

Redepliegue:

```bash
./deploy.sh
```

Eliminar todo:

```bash
./deploy.sh --delete
```

El script imprime la URL del endpoint al final. Ejemplo de petición:

```bash
curl -X POST "$URL" \
     -H 'Content-Type: application/json' \
     -d '{"op":"SUM","numbers":[1,2,3,4,5]}'
```

Respuesta:

```json
{
  "taskId":"...",
  "success":true,
  "worker":"worker-0",
  "attempts":1,
  "result":"15"
}
```

## API

### `POST /task`

**Request body:**

```json
{
  "op": "SUM | TRANSFORM | FAIL_ME",
  "numbers": [1, 2, 3],
  "text": "hola",
  "id": "opcional-uuid"
}
```

**Operaciones:**

| Op | Campo requerido | Comportamiento |
|---|---|---|
| `SUM` | `numbers` | Devuelve la suma de los enteros. |
| `TRANSFORM` | `text` | Devuelve `text` invertido y en mayúsculas. Si `text` contiene `"FAIL"`, el worker falla y es reiniciado. |
| `FAIL_ME` | — | Simulación pura de fallo. El worker lanza `RuntimeException` → supervisor lo reinicia. |

**Respuestas:**

- `200` con `{success:true, result:"…"}` en éxito.
- `500` con `{success:false, error:"…"}` cuando el worker falla.
- `400` con `{error:"bad_request", message:"…"}` si el JSON es inválido.

## Costos estimados en AWS

Con la capa gratuita de Lambda (1 M requests/mes, 400 000 GB-s) y HTTP API (1 M requests/mes), el prototipo cuesta **US$0/mes** hasta agotar la capa. Fuera de ella, aproximadamente **US$0,20 por millón de invocaciones**.
