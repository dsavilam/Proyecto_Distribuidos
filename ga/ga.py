import argparse, json, sqlite3, os
from datetime import datetime, timedelta, timezone
import zmq
from common.config import GA_REP_ADDR, DB_PATH

def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def connect(db_path: str):
    con = sqlite3.connect(db_path, timeout=10, isolation_level=None)  # autocommit OFF -> usaremos BEGIN
    con.execute("PRAGMA foreign_keys = ON")
    return con

def apply_idempotency(con: sqlite3.Connection, key: str, op: str, idSolicitud: str, ts: str) -> bool:
    cur = con.execute("SELECT 1 FROM applied_ops WHERE idempotencyKey = ?", (key,))
    if cur.fetchone():
        return True
    con.execute(
        "INSERT INTO applied_ops(idempotencyKey, op, idSolicitud, timestamp) VALUES (?,?,?,?)",
        (key, op, idSolicitud, ts)
    )
    return False

def op_devolucion(con: sqlite3.Connection, data: dict) -> dict:
    idLibro   = data["idLibro"]; idUsuario = data["idUsuario"]; sede = data["sede"]
    ahora = data.get("timestamp") or iso_now()

    # Encontrar préstamo ACTIVO
    cur = con.execute("""
        SELECT idPrestamo FROM prestamos
         WHERE idLibro=? AND idUsuario=? AND sede=? AND estado='ACTIVO'
         ORDER BY idPrestamo DESC LIMIT 1
    """, (idLibro, idUsuario, sede))
    row = cur.fetchone()
    if not row:
        return {"ok": True, "msg": "No había préstamo activo (idempotente)."}

    idp = row[0]

    # Marcar DEVUELTO y liberar ejemplar
    con.execute("UPDATE prestamos SET estado='DEVUELTO', fecha_entrega=? WHERE idPrestamo=?",
                (ahora, idp))
    con.execute("""
        UPDATE libros
           SET ejemplares_disponibles = MIN(ejemplares_totales, ejemplares_disponibles + 1)
         WHERE idLibro=?
    """, (idLibro,))
    return {"ok": True, "msg": f"Devolución aplicada sobre préstamo {idp}"}

def op_renovacion(con: sqlite3.Connection, data: dict) -> dict:
    idLibro   = data["idLibro"]; idUsuario = data["idUsuario"]; sede = data["sede"]
    nueva = data.get("nuevaFechaEntrega")
    if not nueva:
        # por si el actor no la calculó, +7 días
        try:
            base_ts = data.get("timestamp")
            dt = datetime.strptime(base_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) if base_ts else datetime.now(timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
        nueva = (dt + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur = con.execute("""
        SELECT idPrestamo FROM prestamos
         WHERE idLibro=? AND idUsuario=? AND sede=? AND estado='ACTIVO'
         ORDER BY idPrestamo DESC LIMIT 1
    """, (idLibro, idUsuario, sede))
    row = cur.fetchone()
    if not row:
        return {"ok": False, "msg": "No hay préstamo activo para renovar."}

    idp = row[0]
    con.execute("UPDATE prestamos SET fecha_entrega=? WHERE idPrestamo=?", (nueva, idp))
    return {"ok": True, "msg": f"Renovación aplicada sobre préstamo {idp} nueva_entrega={nueva}"}

def main():
    ap = argparse.ArgumentParser(description="Gestor de Almacenamiento (GA)")
    ap.add_argument("--rep", default=GA_REP_ADDR, help="Endpoint REP bind (p.e., tcp://*:5570)")
    ap.add_argument("--db",  default=DB_PATH,     help="Ruta a la BD SQLite (biblioteca.db)")
    args = ap.parse_args()

    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    con = connect(args.db)

    ctx = zmq.Context.instance()
    rep = ctx.socket(zmq.REP); rep.bind(args.rep)

    print(f"[GA] REP en {args.rep}")
    print(f"[GA] Usando BD: {args.db}")
    print("[GA] Esperando operaciones... (Ctrl+C para salir)")

    try:
        while True:
            raw = rep.recv()
            try:
                data = json.loads(raw.decode("utf-8"))
            except Exception as e:
                rep.send_string(json.dumps({"ok": False, "msg": f"JSON inválido: {e}"}))
                continue

            op = (data.get("op") or "").upper()
            idem = data.get("idempotencyKey")
            idsol = data.get("idSolicitud") or "?"
            ts = data.get("timestamp") or iso_now()

            # Transacción
            try:
                con.execute("BEGIN")
                if not idem:
                    idem = f"NOIDEMP-{op}-{idsol}"

                ya = apply_idempotency(con, idem, op, idsol, ts)
                if ya:
                    con.execute("COMMIT")
                    rep.send_string(json.dumps({"ok": True, "msg": "Ya aplicado (idempotente)."}))
                    print(f"[GA] {op} id={idsol} → ya aplicado (idempotente)")
                    continue

                if op == "DEVOLUCION":
                    res = op_devolucion(con, data)
                elif op == "RENOVACION":
                    res = op_renovacion(con, data)
                else:
                    con.execute("ROLLBACK")
                    rep.send_string(json.dumps({"ok": False, "msg": "op no soportada (Ent1)"}))
                    print(f"[GA] op desconocida: {op} data={data}")
                    continue

                con.execute("COMMIT")
                rep.send_string(json.dumps(res))
                print(f"[GA] {op} id={idsol} → {res}")
            except Exception as e:
                try:
                    con.execute("ROLLBACK")
                except Exception:
                    pass
                rep.send_string(json.dumps({"ok": False, "msg": f"Error aplicando op: {e}"}))
                print(f"[GA] Error aplicando {op} id={idsol}: {e}")

    except KeyboardInterrupt:
        print("\n[GA] Saliendo...")
    finally:
        rep.close(0); ctx.term(); con.close()

if __name__ == "__main__":
    main()
