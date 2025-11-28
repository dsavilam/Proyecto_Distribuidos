import argparse
import json
import time
import threading
from typing import Optional

import zmq


def servir_health(ctx: zmq.Context, bind_addr: str, nombre: str):
    """
    REP de health: responde a {'type':'health'} con {'type':'health_ok'}.
    """
    rep = ctx.socket(zmq.REP)
    rep.bind(bind_addr)
    print(f"[{nombre}] Health REP en {bind_addr}")
    try:
        while True:
            try:
                req = rep.recv_json()
                if req.get("type") == "health":
                    rep.send_json({"type": "health_ok", "actor": nombre})
                else:
                    rep.send_json({"type": "error", "error": "unknown"})
            except zmq.ContextTerminated:
                break
            except Exception as e:
                try:
                    rep.send_json({"type": "error", "error": str(e)})
                except Exception:
                    pass
    finally:
        rep.close(0)


def llamar_ga_con_failover(
    ctx: zmq.Context,
    data: dict,
    primary_ep: str,
    backup_ep: Optional[str] = None,
    timeout_ms: int = 5000,
) -> dict:
    """
    Envía 'data' a GA usando REQ/REP. Intenta primero primary_ep;
    si hay timeout o error, intenta backup_ep (si existe).
    """
    endpoints = [primary_ep] + ([backup_ep] if backup_ep else [])

    for ep in endpoints:
        if not ep:
            continue
        sock = ctx.socket(zmq.REQ)
        sock.connect(ep)
        sock.setsockopt(zmq.RCVTIMEO, timeout_ms)
        try:
            sock.send_json(data)
            resp = sock.recv_json()
            print(f"[ACTOR-REN] GA {ep} → {resp}")
            sock.close(0)
            return resp
        except zmq.Again:
            print(f"[ACTOR-REN][WARN] Timeout hablando con GA {ep}, probando siguiente si existe...")
        except Exception as e:
            print(f"[ACTOR-REN][ERROR] Falla hablando con GA {ep}: {e}")
        finally:
            try:
                sock.close(0)
            except Exception:
                pass

    return {"ok": False, "msg": "Ningún GA respondió (ni primario ni backup)."}


def main():
    ap = argparse.ArgumentParser(description="Actor RENOVACION (SUB + REQ->GA + Health)")
    ap.add_argument(
        "--sub",
        required=True,
        help="Dirección del PUB del GC (p.ej. tcp://127.0.0.1:5560)",
    )
    # Compatibilidad hacia atrás: --ga (un solo endpoint) o --ga-primary/--ga-backup
    ap.add_argument(
        "--ga",
        dest="ga",
        default=None,
        help="Endpoint único del GA (legacy, se asume primario)",
    )
    ap.add_argument(
        "--ga-primary",
        dest="ga_primary",
        default=None,
        help="Dirección REP del GA primario (p.ej. tcp://127.0.0.1:5570)",
    )
    ap.add_argument(
        "--ga-backup",
        dest="ga_backup",
        default=None,
        help="Dirección REP del GA de respaldo (p.ej. tcp://127.0.0.1:5571)",
    )
    ap.add_argument(
        "--hc",
        default="tcp://*:5602",
        help="Bind REP health del actor (default tcp://*:5602)",
    )
    ap.add_argument(
        "--name",
        default="ACTOR-REN",
        help="Nombre del actor para logs",
    )
    args = ap.parse_args()

    ga_primary = args.ga_primary or args.ga
    ga_backup = args.ga_backup

    if not ga_primary:
        raise SystemExit("Debe especificar --ga-primary o --ga (endpoint del GA).")

    ctx = zmq.Context.instance()

    # Health REP en hilo aparte
    threading.Thread(
        target=servir_health,
        args=(ctx, args.hc, args.name),
        daemon=True,
    ).start()

    # SUB al GC
    sub = ctx.socket(zmq.SUB)
    sub.connect(args.sub)
    sub.setsockopt_string(zmq.SUBSCRIBE, "RENOVACION")
    print(f"[{args.name}] SUB a {args.sub} (tópico RENOVACION)")

    print(f"[{args.name}] GA primario: {ga_primary} | GA backup: {ga_backup or '-'}")

    try:
        while True:
            topic, payload = sub.recv_multipart()
            data = json.loads(payload.decode("utf-8"))
            print(f"[{args.name}] Recibí {topic.decode()}: {data}")

            # Enviar a GA para aplicar renovación con failover
            resp = llamar_ga_con_failover(ctx, data, ga_primary, ga_backup)
            print(f"[{args.name}] Respuesta GA → {resp}")
            time.sleep(0.01)
    except KeyboardInterrupt:
        print(f"\n[{args.name}] Saliendo...")
    finally:
        sub.close(0)
        ctx.term()


if __name__ == "__main__":
    main()
