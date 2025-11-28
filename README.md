# Proyecto Biblioteca Distribuida – Entrega 2

Sistema distribuido de biblioteca con:

- **PS** (Procesos Solicitantes) por sede  
- **GC** (Gestor de Carga)  
- **Actores**: PRÉSTAMO, DEVOLUCIÓN, RENOVACIÓN  
- **GA primario + GA backup** con **réplica de BD** e **idempotencia**  
- Comunicación con **ZeroMQ** (REQ/REP, PUB/SUB)

Este README explica cómo **levantar el sistema**, **ejecutar las pruebas funcionales** y **correr los experimentos de rendimiento** (4, 6 y 10 PS por sede), tanto en **localhost** como en las **4 VMs**.

---

## 1. Requisitos

- Python 3.x instalado  
- Librerías:
  - `pyzmq`
- SQLite (incluido en Python)
- Repositorio con la siguiente estructura (simplificada):

```text
PROYECTO_DISTRIBUIDOS/
  ga/
    ga.py
    init_db.py
    schema.sql
    biblioteca.db (se genera)
    biblioteca_replica.db (se genera)
  actores/
    actor_devol.py
    actor_renov.py
    actor_prestamo.py
  gestor_carga/
    gc.py
  ps/
    ps.py
    data/
      sol_sede1.txt
      sol_sede2.txt
      sol_prest_sede1.txt
      sol_prest_sede2.txt
  common/
    config.py
```

### 1.1. (Opcional) Virtualenv en Windows

```powershell
Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned -Force
.\.venv\Scripts\Activate.ps1
```

---

## 2. Inicializar la base de datos (una vez)

### 2.1. En localhost

```bash
cd ga
python init_db.py --db biblioteca.db
python init_db.py --db biblioteca_replica.db
cd ..
```

### 2.2. En VM1 (Daniel) desde la raíz del proyecto

```bash
cd PROYECTO_DISTRIBUIDOS
python3 ga/init_db.py --db ga/biblioteca.db
python3 ga/init_db.py --db ga/biblioteca_replica.db
```

Esto crea:

- **BD principal:** `ga/biblioteca.db`  
- **BD réplica:** `ga/biblioteca_replica.db`  

Con **1000 libros** y ~**200 préstamos activos** repartidos entre **SEDE1** y **SEDE2**.

---

## 3. Ejecución en LOCALHOST (Entrega 2 completa)

Se usan **8 terminales**.  
Orden recomendado: **GA primario → GA backup → Actores → GC → PS**.

### 3.1. GA primario y backup

**Terminal 1 – GA PRIMARIO**

```bash
python -m ga.ga --role primary --rep tcp://*:5570 --db ga/biblioteca.db --db-replica ga/biblioteca_replica.db
```

**Terminal 2 – GA BACKUP**

```bash
python -m ga.ga --role backup --rep tcp://*:5571 --db ga/biblioteca_replica.db
```

---

### 3.2. Actores (con failover a GA primario/backup)

**Terminal 3 – Actor DEVOL**

```bash
python -m actores.actor_devol \
  --sub tcp://127.0.0.1:5560 \
  --ga-primary tcp://127.0.0.1:5570 \
  --ga-backup tcp://127.0.0.1:5571 \
  --hc tcp://*:5601
```

**Terminal 4 – Actor RENOV**

```bash
python -m actores.actor_renov \
  --sub tcp://127.0.0.1:5560 \
  --ga-primary tcp://127.0.0.1:5570 \
  --ga-backup tcp://127.0.0.1:5571 \
  --hc tcp://*:5602
```

**Terminal 5 – Actor PRESTAMO (síncrono, con failover GA)**

```bash
python -m actores.actor_prestamo \
  --bind tcp://*:5585 \
  --ga-primary tcp://127.0.0.1:5570 \
  --ga-backup tcp://127.0.0.1:5571 \
  --hc tcp://*:5603
```

---

### 3.3. Gestor de Carga (GC)

**Terminal 6 – GC**

```bash
python -m gestor_carga.gc \
  --rep tcp://*:5555 \
  --pub tcp://*:5560 \
  --hc-dev tcp://127.0.0.1:5601 \
  --hc-ren tcp://127.0.0.1:5602 \
  --prestamo-addr tcp://127.0.0.1:5585
```

- Escucha PS en `5555` (REP).  
- Publica a Actores en `5560` (PUB).  
- Hace **health-check** a actores DEV/REN.  
- Habla con Actor PRESTAMO en `5585`.

---

### 3.4. PS para DEVOLUCIÓN/RENOVACIÓN (localhost)

**Terminal 7 – PS Sede 1**

```bash
python -m ps.ps \
  --file ps/data/sol_sede1.txt \
  --endpoint tcp://127.0.0.1:5555 \
  --label SEDE1-PS1
```

**Terminal 8 – PS Sede 2**

```bash
python -m ps.ps \
  --file ps/data/sol_sede2.txt \
  --endpoint tcp://127.0.0.1:5555 \
  --label SEDE2-PS1
```

Con esto puedes comprobar el flujo completo de **DEVOLUCIÓN** y **RENOVACIÓN**.

---

## 4. Experimentos de PRÉSTAMO en LOCALHOST

Antes de correr estos experimentos, asegúrate de tener levantados los **Terminales 1–6** (GA primario, GA backup, actores y GC).

Archivos de entrada:

- `ps/data/sol_prest_sede1.txt` (≈30 PRÉSTAMO SEDE1)  
- `ps/data/sol_prest_sede2.txt` (≈30 PRÉSTAMO SEDE2)  

Cada PS imprime al final:

```text
[METRICAS] Duración total ...
[METRICAS] Throughput ...
[METRICAS] Latencia media ...
```

---

### 4.1. Escenario A – 4 PS por sede

**SEDE1**

```bash
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS1
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS2
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS3
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS4
```

**SEDE2**

```bash
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS1
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS2
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS3
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS4
```

Ejecutar todos estos procesos en paralelo (consolas separadas) para recopilar métricas.

---

### 4.2. Escenario B – 6 PS por sede

**SEDE1**

```bash
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS1
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS2
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS3
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS4
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS5
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS6
```

**SEDE2**

```bash
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS1
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS2
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS3
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS4
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS5
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS6
```

---

### 4.3. Escenario C – 10 PS por sede

**SEDE1**

```bash
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS1
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS2
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS3
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS4
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS5
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS6
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS7
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS8
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS9
python -m ps.ps --file ps/data/sol_prest_sede1.txt --endpoint tcp://127.0.0.1:5555 --label SEDE1-PS10
```

**SEDE2**

```bash
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS1
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS2
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS3
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS4
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS5
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS6
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS7
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS8
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS9
python -m ps.ps --file ps/data/sol_prest_sede2.txt --endpoint tcp://127.0.0.1:5555 --label SEDE2-PS10
```
