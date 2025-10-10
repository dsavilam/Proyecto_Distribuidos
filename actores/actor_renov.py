import argparse, json, time, threading, zmq

def servir_health(ctx: zmq.Context, bind_addr: str, nombre: str):
    """REP de health: responde a {'type':'health'} con {'type':'health_ok'}."""
    rep = ctx.socket(zmq.REP); rep.bind(bind_addr)
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
                try: rep.send_json({"type":"error","error":str(e)})
                except: pass
    finally:
        rep.close(0)

def main():
    ap = argparse.ArgumentParser(description="Actor RENOVACION (SUB + REQ->GA + Health)")
    ap.add_argument("--sub", required=True, help="Dirección del PUB del GC (p.ej. tcp://127.0.0.1:5560)")
    ap.add_argument("--ga",  required=True, help="Dirección REP del GA (p.ej. tcp://127.0.0.1:5570)")
    ap.add_argument("--hc",  default="tcp://*:5602", help="Bind REP health del actor (default tcp://*:5602)")
    ap.add_argument("--name", default="ACTOR-REN", help="Nombre del actor para logs")
    args = ap.parse_args()

    ctx = zmq.Context.instance()

    # Health REP en hilo aparte
    threading.Thread(target=servir_health, args=(ctx, args.hc, args.name), daemon=True).start()

    # SUB al GC
    sub = ctx.socket(zmq.SUB); sub.connect(args.sub)
    sub.setsockopt_string(zmq.SUBSCRIBE, "RENOVACION")
    print(f"[{args.name}] SUB a {args.sub} (tópico RENOVACION)")

    # REQ al GA
    req = ctx.socket(zmq.Req);  # o zmq.REQ
    req = ctx.socket(zmq.REQ); req.connect(args.ga)
    req.setsockopt(zmq.RCVTIMEO, 5000)
    print(f"[{args.name}] REQ->GA {args.ga}")

    try:
        while True:
            topic, payload = sub.recv_multipart()
            data = json.loads(payload.decode("utf-8"))
            print(f"[{args.name}] Recibí {topic.decode()}: {data}")

            # Enviar a GA para aplicar renovación
            req.send_json({"type": "RENOVACION", "payload": data})
            resp = req.recv_json()
            print(f"[{args.name}] GA→ {resp}")
            time.sleep(0.01)
    except KeyboardInterrupt:
        print(f"\n[{args.name}] Saliendo...")
    finally:
        sub.close(0); req.close(0); ctx.term()

if __name__ == "__main__":
    main()
