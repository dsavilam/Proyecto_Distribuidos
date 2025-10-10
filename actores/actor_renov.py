import zmq, json, argparse, time
from datetime import datetime, timedelta, timezone
from common.config import GC_PUB_CONNECT, GA_REP_CONNECT, TOPIC_RENOV

def iso_plus_7days(base_iso: str | None) -> str:
    try:
        if base_iso:
            # "2025-10-07T21:00:00Z"
            dt = datetime.strptime(base_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        else:
            dt = datetime.now(timezone.utc)
    except Exception:
        dt = datetime.now(timezone.utc)
    return (dt + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    ap = argparse.ArgumentParser(description="Actor RENOVACION")
    ap.add_argument("--sub", default=GC_PUB_CONNECT, help="Endpoint SUB (GC PUB connect)")
    ap.add_argument("--ga",  default=GA_REP_CONNECT,  help="Endpoint REQ a GA")
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB); sub.connect(args.sub)
    sub.setsockopt_string(zmq.SUBSCRIBE, TOPIC_RENOV)

    req = ctx.socket(zmq.REQ); req.connect(args.ga)
    req.setsockopt(zmq.RCVTIMEO, 4000)

    print(f"[ACTOR-REN] SUB a {args.sub} (tópico {TOPIC_RENOV})")
    print(f"[ACTOR-REN] REQ a GA {args.ga}")

    try:
        while True:
            topic, payload = sub.recv_multipart()
            data = json.loads(payload.decode("utf-8"))
            print(f"[ACTOR-REN] {topic.decode()}: {data}")

            nueva_entrega = iso_plus_7days(data.get("timestamp"))

            ga_msg = {
                "op": "RENOVACION",
                "idempotencyKey": data.get("idempotencyKey"),
                "idSolicitud":    data.get("idSolicitud"),
                "idUsuario":      data.get("idUsuario"),
                "idLibro":        data.get("idLibro"),
                "sede":           data.get("sede"),
                "timestamp":      data.get("timestamp"),
                "nuevaFechaEntrega": nueva_entrega
            }

            req.send_string(json.dumps(ga_msg))
            try:
                resp = req.recv_string()
                print(f"[ACTOR-REN] GA → {resp}")
            except zmq.Again:
                print("[ACTOR-REN] ⚠ Timeout esperando GA")
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("\n[ACTOR-REN] Saliendo...")
    finally:
        sub.close(0); req.close(0); ctx.term()

if __name__ == "__main__":
    main()
