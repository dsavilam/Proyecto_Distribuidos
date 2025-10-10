import json, argparse
import zmq
from common.config import GC_REP_ADDR, GC_PUB_ADDR, TOPIC_DEVOL, TOPIC_RENOV

def main():
    ap = argparse.ArgumentParser(description="Gestor de Carga (GC)")
    ap.add_argument("--rep", default=GC_REP_ADDR, help="Endpoint REP bind (p.e., tcp://*:5555)")
    ap.add_argument("--pub", default=GC_PUB_ADDR, help="Endpoint PUB bind (p.e., tcp://*:5560)")
    args = ap.parse_args()

    ctx = zmq.Context.instance()
    rep = ctx.socket(zmq.REP); rep.bind(args.rep)
    pub = ctx.socket(zmq.PUB); pub.bind(args.pub)

    print(f"[GC] REP en {args.rep}")
    print(f"[GC] PUB en {args.pub}")
    print("[GC] Esperando mensajes... (Ctrl+C para salir)")

    try:
        while True:
            raw = rep.recv()                       # bytes
            msg = json.loads(raw.decode("utf-8"))  # dict
            op  = (msg.get("op") or "").upper()

            if op not in (TOPIC_DEVOL, TOPIC_RENOV):
                rep.send_string(json.dumps({"ok": False, "msg": "op no soportada (Ent1)"}))
                print(f"[GC] op desconocida: {op} payload={msg}")
                continue

            print(f"[GC] Recibí {op}: {msg}")

            # Respuesta inmediata a PS
            rep.send_string(json.dumps({"ok": True, "msg": "Recibido y publicado"}))

            # Publicación al tópico
            pub.send_multipart([op.encode("utf-8"), json.dumps(msg).encode("utf-8")])
            print(f"[GC] Publicado tópico {op}")

    except KeyboardInterrupt:
        print("\n[GC] Saliendo...")
    finally:
        rep.close(0); pub.close(0); ctx.term()

if __name__ == "__main__":
    main()
