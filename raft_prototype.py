"""
Prototipo de consenso distribuido con Raft.
------------------------------------------------
Simula 3 nodos que se coordinan para:
  1) Elegir un líder.
  2) Replicar el valor 'A=1' en la mayoría del cluster.
  3) Recuperar el consenso tras la caída del líder.

Autor: Yordi Polanco Pujols
Curso: Computación Distribuida - UNIBE
Actividad Semana 12
"""

import threading
import time
import random
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict


# ---------------------------------------------------------------
# Configuración general del prototipo
# ---------------------------------------------------------------
LOG_LOCK = threading.Lock()          # Serializa los prints para logs limpios
T0 = time.time()                     # Origen de tiempo relativo

def log(node_id: str, msg: str) -> None:
    """Imprime una línea de log con timestamp relativo y nodo."""
    with LOG_LOCK:
        print(f"[t={time.time()-T0:6.2f}s] [Nodo {node_id}] {msg}")


# ---------------------------------------------------------------
# Modelo del algoritmo Raft
# ---------------------------------------------------------------
class Estado(Enum):
    SEGUIDOR = "SEGUIDOR"
    CANDIDATO = "CANDIDATO"
    LIDER = "LIDER"


@dataclass
class EntradaLog:
    """Entrada del log replicado: (termino, comando)."""
    termino: int
    comando: str


@dataclass
class Nodo:
    """Nodo Raft con estado persistente y volátil."""
    id: str
    # Estado persistente
    termino_actual: int = 0
    voto_por: Optional[str] = None
    log_entradas: List[EntradaLog] = field(default_factory=list)
    # Estado volátil
    estado: Estado = Estado.SEGUIDOR
    indice_commit: int = -1
    lider_conocido: Optional[str] = None
    # Control de tiempos
    ultimo_latido: float = field(default_factory=time.time)
    timeout_eleccion: float = 0.0
    # Simulación de fallos
    vivo: bool = True
    # Referencia al cluster para enviar RPCs
    cluster: Dict[str, "Nodo"] = field(default_factory=dict, repr=False)
    # Bloqueo por nodo para consistencia
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self):
        self.timeout_eleccion = self._nuevo_timeout()

    @staticmethod
    def _nuevo_timeout() -> float:
        # Timeout aleatorio dentro del rango recomendado por Raft
        # para reducir la probabilidad de elecciones divididas.
        return random.uniform(1.5, 3.0)

    # -----------------------------------------------------------
    # RPC: RequestVote
    # -----------------------------------------------------------
    def rpc_pedir_voto(self, termino: int, candidato_id: str,
                       ultimo_indice_log: int, ultimo_termino_log: int):
        """Otro nodo pide nuestro voto para el termino indicado."""
        if not self.vivo:
            return None  # nodo caido, no responde
        with self.lock:
            if termino < self.termino_actual:
                return (self.termino_actual, False)
            if termino > self.termino_actual:
                self.termino_actual = termino
                self.voto_por = None
                self.estado = Estado.SEGUIDOR
            log_ok = (ultimo_termino_log, ultimo_indice_log) >= \
                     (self._ultimo_termino_log(), len(self.log_entradas) - 1)
            if (self.voto_por is None or self.voto_por == candidato_id) and log_ok:
                self.voto_por = candidato_id
                self.ultimo_latido = time.time()
                log(self.id, f"Voto CONCEDIDO a {candidato_id} en termino {termino}")
                return (self.termino_actual, True)
            log(self.id, f"Voto RECHAZADO a {candidato_id} en termino {termino}")
            return (self.termino_actual, False)

    # -----------------------------------------------------------
    # RPC: AppendEntries (latido y replicacion de log)
    # -----------------------------------------------------------
    def rpc_append_entries(self, termino: int, lider_id: str,
                           entradas: List[EntradaLog], indice_commit_lider: int):
        """El lider nos envia latido y, opcionalmente, entradas nuevas."""
        if not self.vivo:
            return None
        with self.lock:
            if termino < self.termino_actual:
                return (self.termino_actual, False)
            self.termino_actual = termino
            self.estado = Estado.SEGUIDOR
            self.lider_conocido = lider_id
            self.ultimo_latido = time.time()
            if entradas:
                for e in entradas:
                    self.log_entradas.append(e)
                log(self.id, f"Replica log del lider {lider_id}: {[e.comando for e in entradas]}")
            if indice_commit_lider > self.indice_commit:
                self.indice_commit = min(indice_commit_lider, len(self.log_entradas) - 1)
                if self.indice_commit >= 0:
                    aplicado = self.log_entradas[self.indice_commit].comando
                    log(self.id, f"COMMIT confirmado -> aplica '{aplicado}'")
            return (self.termino_actual, True)

    def _ultimo_termino_log(self) -> int:
        return self.log_entradas[-1].termino if self.log_entradas else 0

    # -----------------------------------------------------------
    # Bucle principal del nodo
    # -----------------------------------------------------------
    def bucle(self):
        while True:
            if not self.vivo:
                time.sleep(0.1)
                continue
            if self.estado == Estado.SEGUIDOR:
                self._pasar_seguidor()
            elif self.estado == Estado.CANDIDATO:
                self._pasar_candidato()
            elif self.estado == Estado.LIDER:
                self._pasar_lider()
            time.sleep(0.05)

    # -----------------------------------------------------------
    # Rol: seguidor
    # -----------------------------------------------------------
    def _pasar_seguidor(self):
        if time.time() - self.ultimo_latido > self.timeout_eleccion:
            log(self.id, f"Timeout de eleccion vencido -> pasa a CANDIDATO")
            self.estado = Estado.CANDIDATO

    # -----------------------------------------------------------
    # Rol: candidato
    # -----------------------------------------------------------
    def _pasar_candidato(self):
        with self.lock:
            self.termino_actual += 1
            self.voto_por = self.id
            votos = 1
            termino = self.termino_actual
        log(self.id, f"Inicia eleccion para termino {termino}, se vota a si mismo")
        for peer_id, peer in self.cluster.items():
            if peer_id == self.id:
                continue
            resp = peer.rpc_pedir_voto(
                termino, self.id,
                len(self.log_entradas) - 1,
                self._ultimo_termino_log())
            if resp is None:
                log(self.id, f"Peer {peer_id} no responde (posiblemente caido)")
                continue
            t_resp, concedido = resp
            if t_resp > self.termino_actual:
                with self.lock:
                    self.termino_actual = t_resp
                    self.estado = Estado.SEGUIDOR
                    self.voto_por = None
                return
            if concedido:
                votos += 1
        mayoria = (len(self.cluster) // 2) + 1
        if votos >= mayoria:
            log(self.id, f"Gana la eleccion con {votos}/{len(self.cluster)} votos -> LIDER del termino {termino}")
            self.estado = Estado.LIDER
            self.lider_conocido = self.id
        else:
            log(self.id, f"No obtiene mayoria ({votos}/{len(self.cluster)}), reintenta")
            self.timeout_eleccion = self._nuevo_timeout()
            self.ultimo_latido = time.time()
            self.estado = Estado.SEGUIDOR

    # -----------------------------------------------------------
    # Rol: lider
    # -----------------------------------------------------------
    def _pasar_lider(self):
        self._enviar_latidos([])
        time.sleep(0.5)

    def _enviar_latidos(self, entradas: List[EntradaLog]):
        confirmaciones = 1  # el propio lider ya "acepta"
        for peer_id, peer in self.cluster.items():
            if peer_id == self.id:
                continue
            resp = peer.rpc_append_entries(
                self.termino_actual, self.id, entradas, self.indice_commit)
            if resp is None:
                continue
            t_resp, ok = resp
            if t_resp > self.termino_actual:
                log(self.id, f"Detecta termino mayor {t_resp} -> vuelve a SEGUIDOR")
                self.estado = Estado.SEGUIDOR
                self.termino_actual = t_resp
                self.voto_por = None
                return
            if ok:
                confirmaciones += 1
        # Si se replico una entrada nueva y hay mayoria, se hace commit.
        if entradas and confirmaciones >= (len(self.cluster) // 2) + 1:
            self.indice_commit = len(self.log_entradas) - 1
            log(self.id, f"Mayoria confirma. COMMIT del indice {self.indice_commit} "
                         f"= '{self.log_entradas[self.indice_commit].comando}'")

    # -----------------------------------------------------------
    # API de cliente: proponer un comando
    # -----------------------------------------------------------
    def proponer(self, comando: str) -> bool:
        if self.estado != Estado.LIDER or not self.vivo:
            log(self.id, f"Propuesta '{comando}' rechazada (no es lider)")
            return False
        entrada = EntradaLog(termino=self.termino_actual, comando=comando)
        self.log_entradas.append(entrada)
        log(self.id, f"Recibe propuesta de cliente: '{comando}'. Replica a seguidores...")
        self._enviar_latidos([entrada])
        return True


# ---------------------------------------------------------------
# Escenario de simulacion
# ---------------------------------------------------------------
def main():
    random.seed(7)  # reproducibilidad de los timeouts
    ids = ["N1", "N2", "N3"]
    nodos: Dict[str, Nodo] = {i: Nodo(id=i) for i in ids}
    for n in nodos.values():
        n.cluster = nodos

    print("=" * 70)
    print("PROTOTIPO RAFT - 3 NODOS")
    print("=" * 70)

    hilos = [threading.Thread(target=n.bucle, daemon=True) for n in nodos.values()]
    for h in hilos:
        h.start()

    # Fase 1: eleccion de lider
    print("\n--- FASE 1: Eleccion de lider ---")
    time.sleep(4)
    lider = next((n for n in nodos.values() if n.estado == Estado.LIDER), None)
    if lider is None:
        print("!! Ningun lider elegido; se aborta la simulacion.")
        return
    print(f"\n>> Lider actual: {lider.id} (termino {lider.termino_actual})")

    # Fase 2: propuesta de valor
    print("\n--- FASE 2: Propuesta y replicacion del valor 'A=1' ---")
    lider.proponer("A=1")
    time.sleep(2)

    # Fase 3: caida del lider
    print(f"\n--- FASE 3: Se simula la caida del lider {lider.id} ---")
    lider.vivo = False
    log(lider.id, "!! NODO CAIDO !!")

    # Fase 4: reeleccion
    print("\n--- FASE 4: Los seguidores detectan ausencia de latidos y eligen nuevo lider ---")
    time.sleep(6)
    nuevo_lider = next(
        (n for n in nodos.values() if n.estado == Estado.LIDER and n.vivo), None)
    if nuevo_lider is None:
        print("!! No hubo reeleccion tras la caida.")
    else:
        print(f"\n>> Nuevo lider: {nuevo_lider.id} (termino {nuevo_lider.termino_actual})")

    # Fase 5: nueva propuesta bajo el nuevo lider
    if nuevo_lider is not None:
        print("\n--- FASE 5: Nueva propuesta 'B=2' bajo el nuevo lider ---")
        nuevo_lider.proponer("B=2")
        time.sleep(2)

    # Estado final
    print("\n" + "=" * 70)
    print("ESTADO FINAL DEL CLUSTER")
    print("=" * 70)
    for n in nodos.values():
        estado = "CAIDO" if not n.vivo else n.estado.value
        entradas = [e.comando for e in n.log_entradas]
        print(f"  {n.id}: estado={estado:10} termino={n.termino_actual} "
              f"commit_idx={n.indice_commit} log={entradas}")


if __name__ == "__main__":
    main()
