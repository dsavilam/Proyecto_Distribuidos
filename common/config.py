import os

# Binds (para quien escucha) 
GC_REP_ADDR = os.getenv("GC_REP_ADDR", "tcp://*:5555")   # Gestor escucha a PS
GC_PUB_ADDR = os.getenv("GC_PUB_ADDR", "tcp://*:5560")   # Gestor publica a Actores
GA_REP_ADDR = os.getenv("GA_REP_ADDR", "tcp://*:5570")   # GA escucha a Actores

# Connects (para quien se conecta) 
# Localhost por defecto; en VMs cambiamos host por la IP del proceso remoto:
GC_REP_CONNECT = os.getenv("GC_REP_CONNECT", "tcp://127.0.0.1:5555")
GC_PUB_CONNECT = os.getenv("GC_PUB_CONNECT", "tcp://127.0.0.1:5560")
GA_REP_CONNECT = os.getenv("GA_REP_CONNECT", "tcp://127.0.0.1:5570")

#Topics
TOPIC_DEVOL = "DEVOLUCION"
TOPIC_RENOV = "RENOVACION"

# bd
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "ga", "biblioteca.db"))
