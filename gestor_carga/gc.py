import json, time, threading, queue, zmq, argparse
from dataclasses import dataclass, field

@dataclass
class ActorInfo:
    nombre: str
    topico: str                # "DEVOLUCION" o "RENOVACION"
    hc_addr: str               # tcp://IP:PORT del REP health del actor
    vivo: bool = False
    ultimo_ok: float = 0.0
    req: zmq.Socket = None     # socket REQ reutilizable para health, solo en este hilo lol
    backlog: list = field(default_factory=list)  # mensajes pendientes cuando está DOWN

def publicador_worker(ctx: zmq.Context, bind_pub: str, cola_pub: "queue.Queue[tuple[str,dict]]"):
    """Hilo único dueño del socket PUB. Lee (topico, msg) de la cola y publica."""
    pub = ctx.socket(zmq.PUB); pub.bind(bind_pub)
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

def health_loop(ctx: zmq.Context, actores: list[ActorInfo], cola_pub: "queue.Queue[tuple[str,dict]]",
                intervalo: float, timeout_ms: int):
    """Ping periódico a cada actor. Imprime VIVO/DOWN y hace flush del backlog al volver VIVO."""
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
                ok = (resp.get("type") == "health_ok")
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
    ap = argparse.ArgumentParser(description="Gestor de Carga con HealthChecker y backlog")
    ap.add_argument("--rep", default="tcp://*:5555", help="Bind REP para PS (default tcp://*:5555)")
    ap.add_argument("--pub", default="tcp://*:5560", help="Bind PUB para Actores (default tcp://*:5560)")

    # Endpoints de health de los actores (el GC se conecta a ellos)
    ap.add_argument("--hc-dev", default="tcp://127.0.0.1:5601", help="Health REP de actor DEVOLUCION")
    ap.add_argument("--hc-ren", default="tcp://127.0.0.1:5602", help="Health REP de actor RENOVACION")

    # Timings del health checker
    ap.add_argument("--health-interval", type=float, default=3.0, help="Intervalo entre pings (s)")
    ap.add_argument("--health-timeout-ms", type=int, default=1500, help="Timeout de health (ms)")
    args = ap.parse_args()

    ctx = zmq.Context.instance()

    # REP para PS (solo este hilo)
    rep = ctx.socket(zmq.REP); rep.bind(args.rep)
    print(f"[GC] REP en {args.rep}")

    # Cola y publicador (hilo dueño del PUB)
    cola_pub: "queue.Queue[tuple[str,dict]]" = queue.Queue()
    threading.Thread(target=publicador_worker, args=(ctx, args.pub, cola_pub), daemon=True).start()

    # Lista de actores a monitorear
    actores = [
        ActorInfo(nombre="ACTOR-DEV", topico="DEVOLUCION",  hc_addr=args.hc-dev if hasattr(args, "hc-dev") else args.hc_dev),
        ActorInfo(nombre="ACTOR-REN", topico="RENOVACION", hc_addr=args.hc-ren if hasattr(args, "hc-ren") else args.hc_ren),
    ]

    # Hilo de health
    threading.Thread(
        target=health_loop,
        args=(ctx, actores, cola_pub, args.health_interval, args.health_timeout_ms),
        daemon=True
    ).start()

    # Helper para hallar Actor por topic
    def get_actor_por_topico(topico: str) -> ActorInfo | None:
        for a in actores:
            if a.topico == topico: return a
        return None

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
            if op not in ("DEVOLUCION", "RENOVACION"):
                rep.send_string(json.dumps({"ok": False, "msg": "op no soportada (Ent1+HC)"}))
                print(f"[GC] op desconocida: {op} payload={msg}")
                continue

            # Responder inmediato al PS (no bloquea la cola de publicación)
            rep.send_string(json.dumps({"ok": True, "msg": "Recibido y (re)publicado si hay actor VIVO"}))

            # Publicar o encolar según salud del actor
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
            ctx.term()
        except Exception:
            pass

if __name__ == "__main__":
    main()
