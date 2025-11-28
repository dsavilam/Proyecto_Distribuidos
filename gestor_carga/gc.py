import argparse
import json
import queue
import threading
import time
from dataclasses import dataclass, field
from typing import List, Tuple, Optional

import zmq


@dataclass
class ActorInfo:
    nombre: str
    topico: str  # "DEVOLUCION" o "RENOVACION"
    hc_addr: str  # tcp://IP:PORT del REP health del actor
    vivo: bool = False
    ultimo_ok: float = 0.0
    req: Optional[zmq.Socket] = None  # socket REQ reutilizable para health
    backlog: list = field(default_factory=list)  # mensajes pendientes cuando está DOWN


def publicador_worker(ctx: zmq.Context, bind_pub: str, cola_pub: "queue.Queue[Tuple[str, dict]]"):
    """
    Hilo único dueño del socket PUB.
    Lee (topico, msg) de la cola y publica.
    """
    pub = ctx.socket(zmq.PUB)
    pub.bind(bind_pub)
    print(f"[GC] PUB en {bind_pub}")
    try:
        while True:
            topico, msg = cola_pub.get()
            pub.send_multipart([topico.encode("utf-8"), json.dumps(msg).encode("utf-8")])
            print(f"[GC] Publicado tópico {topico}")
    except KeyboardInterrupt:
        pass
    finally:
        pub.close(0)


def health_loop(
    ctx: zmq.Context,
    actores: List[ActorInfo],
    cola_pub: "queue.Queue[Tuple[str, dict]]",
    intervalo: float,
    timeout_ms: int,
):
    """
    Ping periódico a cada actor. Imprime VIVO/DOWN y hace flush del backlog al volver VIVO.
    """
    while True:
        for a in actores:
            ok = False
            try:
                if a.req is None:
                    s = ctx.socket(zmq.REQ)
                    s.connect(a.hc_addr)
                    s.setsockopt(zmq.RCVTIMEO, timeout_ms)
                    s.setsockopt(zmq.SNDTIMEO, timeout_ms)
                    a.req = s

                a.req.send_json({"type": "health"})
                resp = a.req.recv_json()
                ok = resp.get("type") == "health_ok"
            except zmq.Again:
                ok = False
                try:
                    if a.req is not None:
                        a.req.setsockopt(zmq.LINGER, 0)
                        a.req.close(0)
                except Exception:
                    pass
                a.req = None
            except Exception:
                ok = False
                try:
                    if a.req is not None:
                        a.req.setsockopt(zmq.LINGER, 0)
                        a.req.close(0)
                except Exception:
                    pass
                a.req = None

            previo = a.vivo
            a.vivo = ok
            estado = "VIVO" if ok else "DOWN"
            print(f"[salud] {a.nombre} {estado}")

            if ok and not previo and a.backlog:
                # flush backlog: mover a la cola del publicador
                print(f"[GC] {a.nombre} volvió VIVO → enviando backlog ({len(a.backlog)} msg)")
                while a.backlog:
                    cola_pub.put((a.topico, a.backlog.pop(0)))

        time.sleep(intervalo)


def main():
    ap = argparse.ArgumentParser(description="Gestor de Carga con HealthChecker, backlog y PRESTAMO síncrono")
    ap.add_argument(
        "--rep",
        default="tcp://*:5555",
        help="Bind REP para PS (default tcp://*:5555)",
    )
    ap.add_argument(
        "--pub",
        default="tcp://*:5560",
        help="Bind PUB para Actores (default tcp://*:5560)",
    )
    # Endpoints de health de los actores (el GC se conecta a ellos)
    ap.add_argument(
        "--hc-dev",
        dest="hc_dev",
        default="tcp://127.0.0.1:5601",
        help="Health REP de actor DEVOLUCION",
    )
    ap.add_argument(
        "--hc-ren",
        dest="hc_ren",
        default="tcp://127.0.0.1:5602",
        help="Health REP de actor RENOVACION",
    )
    # Actor PRESTAMO (síncrono)
    ap.add_argument(
        "--prestamo-addr",
        dest="prestamo_addr",
        default="tcp://127.0.0.1:5585",
        help="Dirección REP del actor PRESTAMO (el GC se conecta por REQ).",
    )
    # Timings del health checker
    ap.add_argument(
        "--health-interval",
        type=float,
        default=3.0,
        help="Intervalo entre pings (s)",
    )
    ap.add_argument(
        "--health-timeout-ms",
        type=int,
        default=1500,
        help="Timeout de health (ms)",
    )
    # Timeout para actor PRESTAMO
    ap.add_argument(
        "--prestamo-timeout-ms",
        type=int,
        default=5000,
        help="Timeout de actor PRESTAMO (ms)",
    )

    args = ap.parse_args()

    ctx = zmq.Context.instance()

    # REP para PS (solo este hilo)
    rep = ctx.socket(zmq.REP)
    rep.bind(args.rep)
    print(f"[GC] REP en {args.rep}")

    # Cola y publicador (hilo dueño del PUB)
    cola_pub: "queue.Queue[Tuple[str, dict]]" = queue.Queue()
    threading.Thread(
        target=publicador_worker,
        args=(ctx, args.pub, cola_pub),
        daemon=True,
    ).start()

    # Lista de actores a monitorear (solo DEV y REN para el patrón Pub/Sub)
    actores = [
        ActorInfo(nombre="ACTOR-DEV", topico="DEVOLUCION", hc_addr=args.hc_dev),
        ActorInfo(nombre="ACTOR-REN", topico="RENOVACION", hc_addr=args.hc_ren),
    ]

    # Hilo de health
    threading.Thread(
        target=health_loop,
        args=(ctx, actores, cola_pub, args.health_interval, args.health_timeout_ms),
        daemon=True,
    ).start()

    # Helper para hallar Actor por topic
    def get_actor_por_topico(topico: str) -> Optional[ActorInfo]:
        for a in actores:
            if a.topico == topico:
                return a
        return None

    # Socket REQ dedicado para el actor PRESTAMO (síncrono)
    prest_sock = ctx.socket(zmq.REQ)
    prest_sock.connect(args.prestamo_addr)
    prest_sock.setsockopt(zmq.RCVTIMEO, args.prestamo_timeout_ms)
    prest_sock.setsockopt(zmq.SNDTIMEO, args.prestamo_timeout_ms)
    print(f"[GC] Actor PRESTAMO vía {args.prestamo_addr}")

    print("[GC] Esperando mensajes...")
    try:
        while True:
            raw = rep.recv()
            try:
                msg = json.loads(raw.decode("utf-8"))
            except Exception as e:
                rep.send_string(json.dumps({"ok": False, "msg": f"payload no-JSON: {e}"}))
                continue

            op = (msg.get("op") or "").upper()
            if op not in ("DEVOLUCION", "RENOVACION", "PRESTAMO"):
                rep.send_string(json.dumps({"ok": False, "msg": "op no soportada (DEV/REN/PREST)"}))
                print(f"[GC] op desconocida: {op} payload={msg}")
                continue

            if op == "PRESTAMO":
                # Patrón síncrono PS→GC→Actor PREST→GA→Actor PREST→GC→PS
                try:
                    prest_sock.send_json(msg)
                    resp_actor = prest_sock.recv_json()
                    rep.send_string(json.dumps(resp_actor))
                    print(f"[GC] PRESTAMO id={msg.get('idSolicitud')} → {resp_actor}")
                except zmq.Again:
                    resp = {"ok": False, "msg": "Actor PRESTAMO no responde (timeout)."}
                    rep.send_string(json.dumps(resp))
                    print(f"[GC][WARN] PRESTAMO timeout con actor PRESTAMO")
                except Exception as e:
                    resp = {"ok": False, "msg": f"Error hablando con actor PRESTAMO: {e}"}
                    rep.send_string(json.dumps(resp))
                    print(f"[GC][ERROR] PRESTAMO fallo: {e}")
                continue

            # DEVOLUCION / RENOVACION (patrón asíncrono con Pub/Sub)
            # Responder inmediato al PS
            rep.send_string(json.dumps({"ok": True, "msg": "Recibido y (re)publicado si hay actor VIVO"}))

            actor = get_actor_por_topico(op)
            if actor and actor.vivo:
                cola_pub.put((op, msg))
            else:
                if actor:
                    actor.backlog.append(msg)
                    print(f"[GC] {actor.nombre} DOWN → backlog {len(actor.backlog)} (tópico {op})")
                else:
                    print(f"[GC] No hay actor configurado para tópico {op}")
    except KeyboardInterrupt:
        print("\n[GC] Saliendo...")
    finally:
        try:
            rep.close(0)
            prest_sock.close(0)
            ctx.term()
        except Exception:
            pass


if __name__ == "__main__":
    main()
