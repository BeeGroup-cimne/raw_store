"""
Test de connexió Kafka (×2) + Cassandra.
Omple les variables de la secció CONFIG i executa:
    python3 test_connection.py
"""
import sys
import logging
import os
import beelib
import beelib.beekafka
import beelib.beecassandra
import beelib.beeconfig
import load_dotenv

load_dotenv.load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ─── CONFIG ──────────────────────────────────────────────────────────────────

# Fitxer de config a testejar
CONFIG_FILE = os.environ.get("CONF_FILE", "config_aws.json")

# True → testeja el Kafka principal (clau "kafka" al config)
TEST_KAFKA = True
# True → testeja el Kafka OVH (clau "kafka_ovh" al config); s'omet si no existeix la clau
TEST_KAFKA_OVH = True
# True → testeja Cassandra (clau "cassandra" al config)
TEST_CASSANDRA = True

# ─────────────────────────────────────────────────────────────────────────────


def test_kafka(config, key="kafka"):
    label = f"Kafka[{key}]"
    logger.info(f"[{label}] Connectant a {config[key]['connection']['bootstrap.servers']}...")
    try:
        producer = beelib.beekafka.create_kafka_producer(config[key]["connection"], "JSON")
        producer.producer.flush(timeout=5)
        producer.producer.close()
        logger.info(f"[{label}] OK")
        return True
    except Exception as e:
        logger.error(f"[{label}] Error: {e}")
        return False


def test_cassandra(config):
    contact = config["cassandra"]["connection"].get("contact_points", ["?"])[0]
    logger.info(f"[Cassandra] Connectant a {contact}...")
    session = None
    cluster = None
    try:
        session, cluster = beelib.beecassandra.get_session(config["cassandra"])
        row = session.execute("SELECT release_version FROM system.local").one()
        logger.info(f"[Cassandra] OK — versió {row.release_version if row else 'desconeguda'}")
        return True
    except Exception as e:
        logger.error(f"[Cassandra] Error: {e}")
        return False
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()


def main():
    logger.info(f"Llegint config: {CONFIG_FILE}")
    config = beelib.beeconfig.read_config(CONFIG_FILE)

    results = {}

    if TEST_KAFKA:
        if "kafka" in config:
            results["kafka"] = test_kafka(config, "kafka")
        else:
            logger.warning("[Kafka] Clau 'kafka' no trobada al config — saltant")

    if TEST_KAFKA_OVH:
        if "kafka_ovh" in config:
            results["kafka_ovh"] = test_kafka(config, "kafka_ovh")
        else:
            logger.warning("[Kafka OVH] Clau 'kafka_ovh' no trobada al config — saltant")

    if TEST_CASSANDRA:
        if "cassandra" in config:
            results["cassandra"] = test_cassandra(config)
        else:
            logger.warning("[Cassandra] Clau 'cassandra' no trobada al config — saltant")

    logger.info("─── RESUM ───────────────────────────────────")
    all_ok = True
    for name, ok in results.items():
        status = "OK" if ok else "KO"
        logger.info(f"  {name:<15} {status}")
        if not ok:
            all_ok = False
    logger.info("─────────────────────────────────────────────")

    if not all_ok:
        logger.error("[RESULTAT] Alguna connexió ha fallat")
        sys.exit(1)
    logger.info("[RESULTAT] Totes les connexions OK")


if __name__ == "__main__":
    main()