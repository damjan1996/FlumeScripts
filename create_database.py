import pyodbc
import logging
import sys
import time
from pathlib import Path

# Verzeichnispfade
LOG_DIR = Path("./logs")
LOG_DIR.mkdir(exist_ok=True)

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "create_ritterplenty_database.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Datenbank-Konfiguration
SERVER = "116.202.224.248\\SQLEXPRESS"  # Korrekte Serveradresse ohne "\Databases\"
USERNAME = "sa"
PASSWORD = "YJ5C19QZ7ZUW!"
DB_NAME = "RITTERPlenty"


def create_database():
    """Erstellt die Datenbank RITTERPlenty auf dem SQL Server"""
    logger.info(f"Starte Erstellung der Datenbank {DB_NAME} auf Server {SERVER}")

    # Verschiedene Verbindungsstring-Varianten zum Testen
    connection_strings = [
        # Variante 1: Standard mit TrustServerCertificate
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",

        # Variante 2: Mit TCP/IP und Port
        f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{SERVER},1433;UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",

        # Variante 3: Ohne doppelte Backslashes im Servernamen
        f"Driver={{ODBC Driver 17 for SQL Server}};Server=116.202.224.248\\SQLEXPRESS;UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",

        # Variante 4: Mit nur dem Hostnamen und Instanznamen
        f"Driver={{ODBC Driver 17 for SQL Server}};Server=116.202.224.248\\SQLEXPRESS;UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",
    ]

    # Versuche jede Verbindungsstring-Variante
    for i, conn_string in enumerate(connection_strings, 1):
        logger.info(f"Versuche Verbindungsstring Variante {i}: {conn_string}")

        try:
            # Verbindung zum Server herstellen
            cnxn = pyodbc.connect(conn_string, autocommit=True)
            cursor = cnxn.cursor()

            logger.info("Verbindung zum Server erfolgreich hergestellt!")

            # SQL Server Version ausgeben
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            logger.info(f"SQL Server Version: {version}")

            # Prüfen, ob die Datenbank bereits existiert
            cursor.execute(f"SELECT database_id FROM sys.databases WHERE Name = '{DB_NAME}'")
            db_exists = cursor.fetchone() is not None

            if db_exists:
                logger.info(f"Datenbank {DB_NAME} existiert bereits")
            else:
                # Datenbank erstellen
                logger.info(f"Erstelle Datenbank {DB_NAME}")
                cursor.execute(f"CREATE DATABASE [{DB_NAME}]")
                logger.info(f"Datenbank {DB_NAME} erfolgreich erstellt")

            # Verbindung schließen
            cursor.close()
            cnxn.close()

            # Generiere den kompletten Verbindungsstring für später
            conn_string_with_db = conn_string.replace(f"Server={SERVER}", f"Server={SERVER};Database={DB_NAME}")

            # Erfolgsmeldung mit Verbindungsstring
            logger.info("Datenbankerstellung erfolgreich abgeschlossen!")
            logger.info("Für die config.ini verwenden Sie bitte folgenden Verbindungsstring:")
            logger.info(conn_string_with_db)

            return True

        except Exception as e:
            logger.warning(f"Verbindungsversuch {i} fehlgeschlagen: {str(e)}")
            # Kurze Pause vor dem nächsten Versuch
            time.sleep(2)

    # Wenn alle Verbindungsversuche fehlgeschlagen sind
    logger.error("Alle Verbindungsversuche fehlgeschlagen.")
    logger.error("Mögliche Probleme:")
    logger.error("1. Der SQL Server ist nicht erreichbar (Firewall-Einstellungen, Server offline)")
    logger.error("2. Der Instanzname ist falsch")
    logger.error("3. Der Benutzer hat keine Berechtigung zur Verbindung")
    logger.error("4. SQL Server Browser-Dienst ist nicht aktiviert")
    logger.error("\nAlternative Lösungen:")
    logger.error("1. Überprüfen Sie die Server-Einstellungen und Netzwerkkonfiguration")
    logger.error("2. Verwenden Sie SQL Server Management Studio, um die Verbindung zu testen")
    logger.error("3. Verwenden Sie SQLite als Alternative für Entwicklungs-/Testzwecke")
    return False


if __name__ == "__main__":
    create_database()