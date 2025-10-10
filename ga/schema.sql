PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS libros (
  idLibro TEXT PRIMARY KEY,
  titulo  TEXT NOT NULL,
  sede    TEXT NOT NULL,        -- SEDE1 / SEDE2
  ejemplares_totales INTEGER NOT NULL,
  ejemplares_disponibles INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS prestamos (
  idPrestamo INTEGER PRIMARY KEY AUTOINCREMENT,
  idSolicitud TEXT NOT NULL,
  idUsuario   TEXT NOT NULL,
  idLibro     TEXT NOT NULL,
  sede        TEXT NOT NULL,
  fecha_prestamo  TEXT NOT NULL,  -- ISO8601 Z
  fecha_entrega   TEXT NOT NULL,  -- ISO8601 Z (límite / o fecha devolución)
  estado      TEXT NOT NULL CHECK(estado IN ('ACTIVO','DEVUELTO')),
  FOREIGN KEY (idLibro) REFERENCES libros(idLibro)
);

CREATE INDEX IF NOT EXISTS idx_prestamos_activos ON prestamos(idLibro, idUsuario, sede, estado);

CREATE TABLE IF NOT EXISTS applied_ops (
  idempotencyKey TEXT PRIMARY KEY,
  op     TEXT NOT NULL,
  idSolicitud TEXT NOT NULL,
  timestamp   TEXT NOT NULL
);
