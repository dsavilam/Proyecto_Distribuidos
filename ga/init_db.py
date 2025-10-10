import sqlite3, os, random
from datetime import datetime, timedelta, timezone

DB_NAME = os.path.join(os.path.dirname(__file__), "biblioteca.db")
SCHEMA = os.path.join(os.path.dirname(__file__), "schema.sql")

def iso_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def iso_days_from_now(days: int):
    return (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")

def main():
    if os.path.exists(DB_NAME):
        os.remove(DB_NAME)

    with sqlite3.connect(DB_NAME) as con, open(SCHEMA, "r", encoding="utf-8") as f:
        con.executescript(f.read())

        # 1000 libros, mitad en SEDE1 y mitad en SEDE2 (simple)
        libros = []
        for i in range(1, 1001):
            idLibro = f"L{i:04d}"
            sede = "SEDE1" if i <= 500 else "SEDE2"
            tot = 1  # simple para Entrega 1
            disp = 1
            titulo = f"Libro {i:04d}"
            libros.append((idLibro, titulo, sede, tot, disp))

        con.executemany(
            "INSERT INTO libros(idLibro, titulo, sede, ejemplares_totales, ejemplares_disponibles) VALUES (?,?,?,?,?)",
            libros
        )

        # 200 prestados: 50 en SEDE1 (en rango 1..500), 150 en SEDE2 (501..1000)
        prestados_s1 = list(range(1, 501))
        prestados_s2 = list(range(501, 1001))
        random.seed(42)
        sample_s1 = random.sample(prestados_s1, 50)
        sample_s2 = random.sample(prestados_s2, 150)
        activos = []
        now = iso_now()
        plus14 = iso_days_from_now(14)

        # baja disponibles a 0 por cada prestamo (tot=1)
        for i in sample_s1:
            idLibro = f"L{i:04d}"
            con.execute("UPDATE libros SET ejemplares_disponibles = 0 WHERE idLibro = ?", (idLibro,))
            activos.append((
                f"S-INIT-S1-{i:04d}", f"U{i:04d}", idLibro, "SEDE1", now, plus14, "ACTIVO"
            ))
        for i in sample_s2:
            idLibro = f"L{i:04d}"
            con.execute("UPDATE libros SET ejemplares_disponibles = 0 WHERE idLibro = ?", (idLibro,))
            activos.append((
                f"S-INIT-S2-{i:04d}", f"U{i:04d}", idLibro, "SEDE2", now, plus14, "ACTIVO"
            ))

        con.executemany("""
            INSERT INTO prestamos(idSolicitud, idUsuario, idLibro, sede, fecha_prestamo, fecha_entrega, estado)
            VALUES (?,?,?,?,?,?,?)
        """, activos)

        con.commit()
        print(f"[INIT-DB] BD creada en {DB_NAME} con 1000 libros y 200 préstamos ACTIVO.")

if __name__ == "__main__":
    main()
