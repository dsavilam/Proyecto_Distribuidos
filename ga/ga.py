import argparse
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Optional

import zmq

from common.config import GA_REP_ADDR, DB_PATH


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path: str) -> sqlite3.Connection:
    con = sqlite3.connect(
        db_path,
        timeout=10,
        isolation_level=None,  # controlaremos las transacciones a mano con BEGIN/COMMIT/ROLLBACK
        check_same_thread=False,
    )
    con.execute("PRAGMA foreign_keys = ON")
    return con


def apply_idempotency(
    con: sqlite3.Connection,
    key: str,
    op: str,
    idSolicitud: str,
    ts: str,
) -> bool:
    """
    Registra la operación en applied_ops si no existe.
    Devuelve True si YA estaba aplicada (idempotente).
    """
    cur = con.execute(
        "SELECT 1 FROM applied_ops WHERE idempotencyKey = ?",
        (key,),
    )
    if cur.fetchone():
        return True

    con.execute(
        """
        INSERT INTO applied_ops(idempotencyKey, op, idSolicitud, timestamp)
        VALUES (?,?,?,?)
        """,
        (key, op, idSolicitud, ts),
    )
    return False


def op_devolucion(con: sqlite3.Connection, data: dict) -> dict:
    idLibro = data["idLibro"]
    idUsuario = data["idUsuario"]
    sede = data["sede"]
    ahora = data.get("timestamp") or iso_now()

    # Buscar préstamo ACTIVO
    cur = con.execute(
        """
        SELECT idPrestamo
        FROM prestamos
        WHERE idLibro=? AND idUsuario=? AND sede=? AND estado='ACTIVO'
        ORDER BY idPrestamo DESC
        LIMIT 1
        """,
        (idLibro, idUsuario, sede),
    )
    row = cur.fetchone()
    if not row:
        return {"ok": True, "msg": "No había préstamo activo (idempotente)."}

    idp = row[0]

    # Marcar DEVUELTO y liberar ejemplar
    con.execute(
        "UPDATE prestamos SET estado='DEVUELTO', fecha_entrega=? WHERE idPrestamo=?",
        (ahora, idp),
    )
    con.execute(
        """
        UPDATE libros
        SET ejemplares_disponibles = MIN(ejemplares_totales, ejemplares_disponibles + 1)
        WHERE idLibro=?
        """,
        (idLibro,),
    )

    return {"ok": True, "msg": f"Devolución aplicada sobre préstamo {idp}"}


def op_renovacion(con: sqlite3.Connection, data: dict) -> dict:
    idLibro = data["idLibro"]
    idUsuario = data["idUsuario"]
    sede = data["sede"]
    nueva = data.get("nuevaFechaEntrega")

    if not nueva:
        # Si el actor no calculó nueva fecha, sumamos 7 días
        base_ts = data.get("timestamp")
        try:
            if base_ts:
                dt = datetime.strptime(base_ts, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            else:
                dt = datetime.now(timezone.utc)
        except Exception:
            dt = datetime.now(timezone.utc)
        nueva = (dt + timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%SZ")

    cur = con.execute(
        """
        SELECT idPrestamo
        FROM prestamos
        WHERE idLibro=? AND idUsuario=? AND sede=? AND estado='ACTIVO'
        ORDER BY idPrestamo DESC
        LIMIT 1
        """,
        (idLibro, idUsuario, sede),
    )
    row = cur.fetchone()
    if not row:
        return {"ok": False, "msg": "No hay préstamo activo para renovar."}

    idp = row[0]
    con.execute(
        "UPDATE prestamos SET fecha_entrega=? WHERE idPrestamo=?",
        (nueva, idp),
    )
    return {"ok": True, "msg": f"Renovación aplicada sobre préstamo {idp} nueva_entrega={nueva}"}


def op_prestamo(con: sqlite3.Connection, data: dict) -> dict:
    """
    PRESTAMO: si hay ejemplar disponible, crea préstamo ACTIVO y decrementa disponibles.
    Si no hay ejemplares, responde ok=False.
    """
    idLibro = data["idLibro"]
    idUsuario = data["idUsuario"]
    sede = data["sede"]
    ahora = data.get("timestamp") or iso_now()
    dias = int(data.get("dias", 14))

    # Comprobar disponibilidad del libro
    cur = con.execute(
        """
        SELECT ejemplares_totales, ejemplares_disponibles
        FROM libros
        WHERE idLibro=? AND sede=?
        """,
        (idLibro, sede),
    )
    row = cur.fetchone()
    if not row:
        return {"ok": False, "msg": f"Libro {idLibro} no existe en sede {sede}."}

    tot, disp = row
    if disp <= 0:
        return {"ok": False, "msg": f"Sin ejemplares disponibles para {idLibro} en {sede}."}

    fecha_entrega = (
        datetime.strptime(ahora, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
        + timedelta(days=dias)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    con.execute(
        """
        INSERT INTO prestamos(idSolicitud, idUsuario, idLibro, sede, fecha_prestamo, fecha_entrega, estado)
        VALUES (?,?,?,?,?,?,?)
        """,
        (
            data.get("idSolicitud") or f"S-PREST-{idLibro}-{idUsuario}",
            idUsuario,
            idLibro,
            sede,
            ahora,
            fecha_entrega,
            "ACTIVO",
        ),
    )
    con.execute(
        """
        UPDATE libros
        SET ejemplares_disponibles = ejemplares_disponibles - 1
        WHERE idLibro=? AND sede=?
        """,
        (idLibro, sede),
    )

    return {
        "ok": True,
        "msg": f"Préstamo creado para {idLibro} en {sede}",
        "fecha_entrega": fecha_entrega,
    }


def process_operation(con: sqlite3.Connection, data: dict) -> dict:
    """
    Aplica la operación en UNA base de datos (primaria o réplica) respetando idempotencia.
    """
    op = (data.get("op") or "").upper()
    idem = data.get("idempotencyKey")
    idsol = data.get("idSolicitud") or "?"
    ts = data.get("timestamp") or iso_now()

    con.execute("BEGIN")

    if not idem:
        idem = f"NOIDEMP-{op}-{idsol}"

    ya = apply_idempotency(con, idem, op, idsol, ts)
    if ya:
        con.execute("COMMIT")
        return {"ok": True, "msg": "Ya aplicado (idempotente)."}

    if op == "DEVOLUCION":
        res = op_devolucion(con, data)
    elif op == "RENOVACION":
        res = op_renovacion(con, data)
    elif op == "PRESTAMO":
        res = op_prestamo(con, data)
    else:
        con.execute("ROLLBACK")
        return {"ok": False, "msg": "op no soportada (Ent2)"}

    con.execute("COMMIT")
    return res


def main():
    ap = argparse.ArgumentParser(description="Gestor de Almacenamiento (GA) con réplica y PRESTAMO")
    ap.add_argument(
        "--rep",
        default=GA_REP_ADDR,
        help="Endpoint REP bind (p.e., tcp://*:5570)",
    )
    ap.add_argument(
        "--db",
        default=DB_PATH,
        help="Ruta a la BD SQLite primaria (biblioteca.db)",
    )
    ap.add_argument(
        "--db-replica",
        dest="db_replica",
        default=None,
        help="Ruta a la BD SQLite réplica (se actualiza desde el GA primario).",
    )
    ap.add_argument(
        "--role",
        choices=["primary", "backup"],
        default="primary",
        help="Rol de este GA: primary (aplica ops y replica) o backup (sólo aplica en su propia BD).",
    )
    args = ap.parse_args()

    # Asegurar carpetas
    os.makedirs(os.path.dirname(args.db), exist_ok=True)
    if args.db_replica:
        os.makedirs(os.path.dirname(args.db_replica), exist_ok=True)

    # DB que maneja este proceso
    if args.role == "backup" and args.db_replica:
        db_path = args.db_replica
    else:
        db_path = args.db

    con = connect(db_path)

    replica_con: Optional[sqlite3.Connection] = None
    if args.role == "primary" and args.db_replica:
        replica_con = connect(args.db_replica)
        print(f"[GA] Modo PRIMARY con réplica en {args.db_replica}")
    elif args.role == "backup":
        print(f"[GA] Modo BACKUP usando BD {db_path}")
    else:
        print("[GA] Modo PRIMARY sin réplica (solo BD principal).")

    ctx = zmq.Context.instance()
    rep = ctx.socket(zmq.REP)
    rep.bind(args.rep)

    print(f"[GA] REP en {args.rep}")
    print(f"[GA] Usando BD principal: {db_path}")
    if replica_con:
        print(f"[GA] Réplica activada en: {args.db_replica}")
    print("[GA] Esperando operaciones...")

    try:
        while True:
            raw = rep.recv()
            try:
                data = json.loads(raw.decode("utf-8"))
            except Exception as e:
                rep.send_string(json.dumps({"ok": False, "msg": f"JSON inválido: {e}"}))
                continue

            op = (data.get("op") or "").upper()
            idsol = data.get("idSolicitud") or "?"
            print(f"[GA][{args.role}] op={op} id={idsol}")

            try:
                # Aplica en la BD de este GA
                res = process_operation(con, data)

                # Si soy primario y tengo réplica, replico la misma operación
                if args.role == "primary" and replica_con is not None:
                    try:
                        _ = process_operation(replica_con, data)
                        print(f"[GA] Réplica OK para id={idsol}")
                    except Exception as e_rep:
                        print(f"[GA][WARN] Fallo replicando en BD réplica: {e_rep}")

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
        rep.close(0)
        ctx.term()
        con.close()
        if replica_con:
            replica_con.close()


if __name__ == "__main__":
    main()
