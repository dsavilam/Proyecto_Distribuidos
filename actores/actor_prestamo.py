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
            print(f"[ACTOR-PREST] GA {ep} → {resp}")
            sock.close(0)
            return resp
        except zmq.Again:
            print(f"[ACTOR-PREST][WARN] Timeout hablando con GA {ep}, probando siguiente si existe...")
        except Exception as e:
            print(f"[ACTOR-PREST][ERROR] Falla hablando con GA {ep}: {e}")
        finally:
            try:
                sock.close(0)
            except Exception:
                pass

    return {"ok": False, "msg": "Ningún GA respondió (ni primario ni backup)."}


def main():
    ap = argparse.ArgumentParser(description="Actor PRESTAMO (REP←GC, REQ→GA, Health)")
    ap.add_argument(
        "--bind",
        default="tcp://*:5585",
        help="Bind REP para que el GC se conecte (default tcp://*:5585)",
    )
    ap.add_argument(
        "--ga-primary",
        dest="ga_primary",
        required=True,
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
        default="tcp://*:5603",
        help="Bind REP health del actor (default tcp://*:5603)",
    )
    ap.add_argument(
        "--name",
        default="ACTOR-PREST",
        help="Nombre del actor para logs",
    )
    args = ap.parse_args()

    ctx = zmq.Context.instance()

    # Health REP en hilo aparte
    threading.Thread(
        target=servir_health,
        args=(ctx, args.hc, args.name),
        daemon=True,
    ).start()

    # REP para PRESTAMO (desde GC)
    rep = ctx.socket(zmq.REP)
    rep.bind(args.bind)
    print(f"[{args.name}] REP PRESTAMO en {args.bind}")
    print(f"[{args.name}] GA primario: {args.ga_primary} | GA backup: {args.ga_backup or '-'}")

    try:
        while True:
            data = rep.recv_json()
            op = (data.get("op") or "").upper()
            print(f"[{args.name}] Recibí solicitud de GC: {op} id={data.get('idSolicitud')} data={data}")

            if op != "PRESTAMO":
                resp = {"ok": False, "msg": f"op no soportada por actor PRESTAMO: {op}"}
            else:
                resp = llamar_ga_con_failover(ctx, data, args.ga_primary, args.ga_backup)

            rep.send_json(resp)
            time.sleep(0.01)
    except KeyboardInterrupt:
        print(f"\n[{args.name}] Saliendo...")
    finally:
        rep.close(0)
        ctx.term()


if __name__ == "__main__":
    main()
