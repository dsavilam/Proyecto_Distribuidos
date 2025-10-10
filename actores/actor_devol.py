import zmq, json, argparse, time
from common.config import GC_PUB_CONNECT, GA_REP_CONNECT, TOPIC_DEVOL

def main():
    ap = argparse.ArgumentParser(description="Actor DEVOLUCION")
    ap.add_argument("--sub", default=GC_PUB_CONNECT, help="Endpoint SUB (GC PUB connect)")
    ap.add_argument("--ga",  default=GA_REP_CONNECT,  help="Endpoint REQ a GA")
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB); sub.connect(args.sub)
    sub.setsockopt_string(zmq.SUBSCRIBE, TOPIC_DEVOL)

    req = ctx.socket(zmq.REQ); req.connect(args.ga)
    req.setsockopt(zmq.RCVTIMEO, 4000)

    print(f"[ACTOR-DEV] SUB a {args.sub} (tópico {TOPIC_DEVOL})")
    print(f"[ACTOR-DEV] REQ a GA {args.ga}")

    try:
        while True:
            topic, payload = sub.recv_multipart()
            data = json.loads(payload.decode("utf-8"))
            print(f"[ACTOR-DEV] {topic.decode()}: {data}")

            # Armar request a GA
            ga_msg = {
                "op": "DEVOLUCION",
                "idempotencyKey": data.get("idempotencyKey"),
                "idSolicitud":    data.get("idSolicitud"),
                "idUsuario":      data.get("idUsuario"),
                "idLibro":        data.get("idLibro"),
                "sede":           data.get("sede"),
                "timestamp":      data.get("timestamp"),
            }

            req.send_string(json.dumps(ga_msg))
            try:
                resp = req.recv_string()
                print(f"[ACTOR-DEV] GA → {resp}")
            except zmq.Again:
                print("[ACTOR-DEV] ⚠ Timeout esperando GA")
            time.sleep(0.01)
    except KeyboardInterrupt:
        print("\n[ACTOR-DEV] Saliendo...")
    finally:
        sub.close(0); req.close(0); ctx.term()

if __name__ == "__main__":
    main()
