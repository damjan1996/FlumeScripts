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
        logging.FileHandler(LOG_DIR / "create_tables_ritterplenty.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Datenbankverbindungsinformationen
SERVER = "116.202.224.248\\SQLEXPRESS"
DATABASE = "RITTERPlenty"
USERNAME = "sa"
PASSWORD = "YJ5C19QZ7ZUW!"

# SQL-Skripte für die Tabellenerstellung
TABLE_CREATION_SCRIPTS = [
    # Barcodes Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[barcodes]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[barcodes] (
            [variation_id] NVARCHAR(255),
            [wg1] NVARCHAR(255),
            [wg2] NVARCHAR(255)
        );
        PRINT 'Tabelle barcodes erstellt';
    END
    """,

    # Matchcodes Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[matchcodes]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[matchcodes] (
            [variation_id] NVARCHAR(255),
            [variation_number] NVARCHAR(255),
            [variation_name] NVARCHAR(MAX),
            [item_description_name] NVARCHAR(MAX),
            [variation_default_category_branch_name] NVARCHAR(MAX),
            [variation_default_category_manually] INT,
            [variation_bundle_components] NVARCHAR(MAX),
            [item_manufacturer_name] NVARCHAR(255)
        );
        PRINT 'Tabelle matchcodes erstellt';
    END
    """,

    # Orders Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[orders]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[orders] (
            [plenty_id] NVARCHAR(255),
            [o_id] NVARCHAR(255),
            [o_plenty_id] NVARCHAR(255),
            [o_origin_order_id] NVARCHAR(255),
            [os_id] NVARCHAR(255),
            [o_referrer] DECIMAL(18, 6),
            [o_global_referrer] DECIMAL(18, 6),
            [o_type] NVARCHAR(255),
            [o_is_main_order] BIT,
            [o_is_net] BIT,
            [o_is_b2b] BIT,
            [ac_id] NVARCHAR(255),
            [o_payment_status] INT,
            [o_invoice_postal_code] NVARCHAR(255),
            [o_invoice_town] NVARCHAR(255),
            [o_invoice_country] NVARCHAR(255),
            [o_delivery_postal_code] NVARCHAR(255),
            [o_delivery_town] NVARCHAR(255),
            [o_delivery_country] NVARCHAR(255),
            [o_shipping_profile] INT,
            [o_parcel_service] INT,
            [smw_id] NVARCHAR(255),
            [smw_country] NVARCHAR(255),
            [smw_postal_code] NVARCHAR(255),
            [o_shipping_provider] NVARCHAR(255),
            [o_entry_at] DATETIME,
            [o_created_at] DATETIME,
            [o_goods_issue_at] DATETIME,
            [o_paid_at] DATETIME,
            [o_updated_at] DATETIME
        );
        PRINT 'Tabelle orders erstellt';
    END
    """,

    # OrderItems Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[orderItems]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[orderItems] (
            [plenty_id] NVARCHAR(255),
            [oi_id] NVARCHAR(255),
            [o_id] NVARCHAR(255),
            [iv_id] NVARCHAR(255),
            [i_id] NVARCHAR(255),
            [oi_quantity] DECIMAL(18, 6),
            [oi_type_id] INT,
            [smw_id] NVARCHAR(255),
            [oi_updated_at] DATETIME,
            [o_created_at] DATETIME,
            [o_updated_at] DATETIME
        );
        PRINT 'Tabelle orderItems erstellt';
    END
    """,

    # OrderItemAmounts Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[orderItemAmounts]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[orderItemAmounts] (
            [plenty_id] NVARCHAR(255),
            [oia_id] NVARCHAR(255),
            [oi_id] NVARCHAR(255),
            [oia_is_system_currency] BIT,
            [oi_price_gross] DECIMAL(18, 6),
            [oi_price_net] DECIMAL(18, 6),
            [oi_price_currency] NVARCHAR(255),
            [oi_exchange_rate] DECIMAL(18, 6),
            [oi_purchase_price] DECIMAL(18, 6),
            [oi_updated_at] DATETIME,
            [o_created_at] DATETIME
        );
        PRINT 'Tabelle orderItemAmounts erstellt';
    END
    """,

    # WG1 Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[wg1]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[wg1] (
            [code] NVARCHAR(255),
            [beschreibung] NVARCHAR(MAX),
            [beschreibung_export] NVARCHAR(MAX)
        );
        PRINT 'Tabelle wg1 erstellt';
    END
    """,

    # WG2 Tabelle
    """
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[wg2]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [dbo].[wg2] (
            [code] NVARCHAR(255),
            [beschreibung] NVARCHAR(MAX),
            [beschreibung_export] NVARCHAR(MAX),
            [artikelrabattgruppe] NVARCHAR(255)
        );
        PRINT 'Tabelle wg2 erstellt';
    END
    """
]


def create_tables():
    """Erstellt alle erforderlichen Tabellen in der RITTERPlenty-Datenbank"""
    logger.info(f"Starte Erstellung der Tabellen in Datenbank {DATABASE}")

    # Verschiedene Verbindungsstring-Varianten zum Testen
    connection_strings = [
        # Variante 1: Standard mit TrustServerCertificate
        f"Driver={{ODBC Driver 17 for SQL Server}};Server={SERVER};Database={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",

        # Variante 2: Mit TCP/IP und Port
        f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{SERVER},1433;Database={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",

        # Variante 3: Ohne doppelte Backslashes im Servernamen
        f"Driver={{ODBC Driver 17 for SQL Server}};Server=116.202.224.248\\SQLEXPRESS;Database={DATABASE};UID={USERNAME};PWD={PASSWORD};TrustServerCertificate=yes;Connection Timeout=60",
    ]

    # Versuche jede Verbindungsstring-Variante
    for i, conn_string in enumerate(connection_strings, 1):
        logger.info(f"Versuche Verbindungsstring Variante {i}: {conn_string}")

        try:
            # Verbindung zur Datenbank herstellen
            cnxn = pyodbc.connect(conn_string)
            cursor = cnxn.cursor()

            logger.info("Verbindung zur Datenbank erfolgreich hergestellt!")

            # Tabellen erstellen
            success_count = 0
            for j, script in enumerate(TABLE_CREATION_SCRIPTS, 1):
                logger.info(f"Führe Tabellenerstellungs-Script {j} von {len(TABLE_CREATION_SCRIPTS)} aus")
                try:
                    cursor.execute(script)
                    cnxn.commit()
                    logger.info(f"Script {j} erfolgreich ausgeführt")
                    success_count += 1
                except Exception as e:
                    logger.error(f"Fehler bei Script {j}: {str(e)}")

            # Erfolgsmeldung
            if success_count == len(TABLE_CREATION_SCRIPTS):
                logger.info("Alle Tabellen erfolgreich erstellt!")
            else:
                logger.warning(f"{success_count} von {len(TABLE_CREATION_SCRIPTS)} Tabellen erstellt.")

            # Vorhandene Tabellen auflisten
            logger.info("Vorhandene Tabellen in der Datenbank:")
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)

            tables = cursor.fetchall()
            for table in tables:
                logger.info(f"  - {table[0]}")

            # Verbindung schließen
            cursor.close()
            cnxn.close()

            # Erfolgsmeldung mit Verbindungsstring
            logger.info("Tabellenerstellung abgeschlossen!")
            logger.info("Für die config.ini verwenden Sie bitte folgenden Verbindungsstring:")
            logger.info(conn_string)

            return True

        except Exception as e:
            logger.warning(f"Verbindungsversuch {i} fehlgeschlagen: {str(e)}")
            # Kurze Pause vor dem nächsten Versuch
            time.sleep(2)

    # Wenn alle Verbindungsversuche fehlgeschlagen sind
    logger.error("Alle Verbindungsversuche fehlgeschlagen.")
    return False


if __name__ == "__main__":
    create_tables()