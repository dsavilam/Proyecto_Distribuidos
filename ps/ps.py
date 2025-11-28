import argparse
import json
import sys
import time
import uuid
import hashlib
from datetime import datetime, timezone

import zmq

ALLOWED_OPS = {"DEVOLUCION", "RENOVACION", "PRESTAMO"}


def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def ensure_message_contract(msg: dict) -> dict:
    if "op" not in msg or msg["op"] not in ALLOWED_OPS:
        raise ValueError(f"op inválida: {msg.get('op')}. Debe ser una de {ALLOWED_OPS}")

    if "idSolicitud" not in msg:
        msg["idSolicitud"] = f"S-{uuid.uuid4()}"

    for k in ("idUsuario", "idLibro", "sede"):
        if k not in msg:
            raise ValueError(f"falta campo obligatorio: {k}")

    if "timestamp" not in msg:
        msg["timestamp"] = iso_now()

    if "idempotencyKey" not in msg:
        base = f"{msg['op']}:{msg['idSolicitud']}:{msg['idLibro']}"
        msg["idempotencyKey"] = hashlib.sha256(base.encode()).hexdigest()[:16]

    return msg


def main():
    parser = argparse.ArgumentParser(description="Procesos Solicitantes (PS) - ZeroMQ REQ")
    parser.add_argument("--file", required=True, help="Ruta al archivo (JSON por línea)")
    parser.add_argument(
        "--endpoint",
        default="tcp://127.0.0.1:5555",
        help="Endpoint del GC REP (p.e., tcp://gc:5555)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=0.2,
        help="Intervalo entre envíos (s)",
    )
    parser.add_argument(
        "--timeout_ms",
        type=int,
        default=2000,
        help="Timeout de respuesta (ms)",
    )
    parser.add_argument(
        "--label",
        default="",
        help="Etiqueta opcional para los experimentos (ej: sede1-ps1-4hilos)",
    )
    args = parser.parse_args()

    label = f"[{args.label}] " if args.label else ""

    print(f"{label}[PS] Enviando solicitudes a {args.endpoint} desde archivo {args.file}")
    ctx = zmq.Context.instance()

    total, ok, fail = 0, 0, 0
    lat_sum, lat_min, lat_max = 0.0, None, None
    t_global_start = None
    t_global_end = None

    with open(args.file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            total += 1
            try:
                raw = json.loads(line)
                msg = ensure_message_contract(raw)
            except Exception as e:
                fail += 1
                print(f"{label}[PS][ERROR] Línea {total} inválida: {e}")
                continue

            sock = ctx.socket(zmq.REQ)
            sock.connect(args.endpoint)
            sock.setsockopt(zmq.RCVTIMEO, args.timeout_ms)
            sock.setsockopt(zmq.LINGER, 0)

            try:
                if t_global_start is None:
                    t_global_start = time.perf_counter()

                t0 = time.perf_counter()
                sock.send_json(msg)
                reply = sock.recv_json()
                t1 = time.perf_counter()

                dt = t1 - t0
                lat_sum += dt
                lat_min = dt if lat_min is None or dt < lat_min else lat_min
                lat_max = dt if lat_max is None or dt > lat_max else lat_max

                ok += 1
                print(f"{label}[PS][OK] {msg['op']} id={msg['idSolicitud']} → {reply} (lat={dt:.4f}s)")
            except zmq.Again:
                fail += 1
                print(f"{label}[PS][WARN] Timeout para id={msg['idSolicitud']}")
            except Exception as e:
                fail += 1
                print(f"{label}[PS][ERROR] id={msg['idSolicitud']} fallo: {e}")
            finally:
                sock.close(0)

            time.sleep(max(0.0, args.interval))

    t_global_end = time.perf_counter() if t_global_start is not None else None

    print(f"{label}[PS] Terminado. total={total} ok={ok} fail={fail}")

    if t_global_start is not None and t_global_end is not None:
        elapsed = t_global_end - t_global_start
        throughput = (total / elapsed) if elapsed > 0 else 0.0
        avg_lat = (lat_sum / ok) if ok > 0 else 0.0

        lat_min_str = f"{lat_min:.4f}s" if lat_min is not None else "N/A"
        lat_max_str = f"{lat_max:.4f}s" if lat_max is not None else "N/A"

        print(f"{label}[METRICAS] Duración total = {elapsed:.4f}s")
        print(f"{label}[METRICAS] Throughput = {throughput:.2f} ops/s (total={total})")
        print(f"{label}[METRICAS] Latencia media = {avg_lat:.4f}s, min = {lat_min_str}, max = {lat_max_str}")

    ctx.term()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[PS] Interrumpido")
        sys.exit(130)
