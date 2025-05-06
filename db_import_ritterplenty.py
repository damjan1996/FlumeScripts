import os
import sys
import logging
import pyodbc
import configparser
import argparse
import json
import csv
import datetime
import traceback
import pandas as pd
import time
from pathlib import Path

# Verzeichnispfade
PROCESSED_DIR = Path("./processed")
LOG_DIR = Path("./logs")
CONFIG_DIR = Path("./config")

# Erstelle Verzeichnisse, falls sie nicht existieren
PROCESSED_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)
CONFIG_DIR.mkdir(exist_ok=True)

# Logging konfigurieren
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "db_import_ritterplenty.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Konstanten
CHUNK_SIZE = 1000  # Größe der Chunks für Bulk-Insert
EXCEL_WG1_TABLE = "wg1"
EXCEL_WG2_TABLE = "wg2"
WG1_KEYS = ["code", "beschreibung", "beschreibung_export"]
WG2_KEYS = [
    "code",
    "beschreibung",
    "beschreibung_export",
    "artikelrabattgruppe",
]
MATCHCODES_TABLE = "matchcodes"
BARCODES_TABLE = "barcodes"


def connect_to_database(connstring):
    """Stellt eine Verbindung zur Datenbank her"""
    logger.info("Stelle Verbindung zur Datenbank her")
    logger.debug(f"Verbindungsstring: {connstring}")

    # Verschiedene Verbindungsversuche unternehmen
    for attempt in range(1, 4):
        try:
            logger.info(f"Verbindungsversuch {attempt}/3")
            cnxn = pyodbc.connect(connstring, timeout=60)
            cursor = cnxn.cursor()
            cursor.fast_executemany = True  # Für schnellere Bulk-Inserts

            # Teste Verbindung
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            logger.info(f"Verbindung erfolgreich hergestellt. SQL Server Version: {version}")

            # Überprüfe, ob die Datenbank korrekt ist
            cursor.execute("SELECT DB_NAME()")
            db_name = cursor.fetchone()[0]
            logger.info(f"Verbunden mit Datenbank: {db_name}")

            return cnxn, cursor
        except Exception as e:
            logger.warning(f"Verbindungsversuch {attempt} fehlgeschlagen: {str(e)}")
            if attempt < 3:
                logger.info(f"Warte 5 Sekunden vor dem nächsten Versuch...")
                time.sleep(5)
            else:
                logger.critical(f"Alle Verbindungsversuche fehlgeschlagen. Letzter Fehler: {str(e)}")
                logger.error(traceback.format_exc())
                raise


def ensure_table_exists(cursor, table_name):
    """Stellt sicher, dass die Tabelle existiert, erstellt sie sonst"""
    logger.info(f"Stelle sicher, dass Tabelle {table_name} existiert")

    try:
        # Prüfe, ob die Tabelle existiert
        cursor.execute(f"""
            IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{table_name}') 
            SELECT 1 ELSE SELECT 0
        """)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            logger.info(f"Tabelle {table_name} existiert bereits")
            return True
        else:
            logger.warning(f"Tabelle {table_name} existiert nicht, erstelle sie")

            # Tabellendefinitionen
            table_definitions = {
                BARCODES_TABLE: """
                    CREATE TABLE [dbo].[barcodes] (
                        [variation_id] NVARCHAR(255),
                        [wg1] NVARCHAR(255),
                        [wg2] NVARCHAR(255)
                    )
                """,
                MATCHCODES_TABLE: """
                    CREATE TABLE [dbo].[matchcodes] (
                        [variation_id] NVARCHAR(255),
                        [variation_number] NVARCHAR(255),
                        [variation_name] NVARCHAR(MAX),
                        [item_description_name] NVARCHAR(MAX),
                        [variation_default_category_branch_name] NVARCHAR(MAX),
                        [variation_default_category_manually] INT,
                        [variation_bundle_components] NVARCHAR(MAX),
                        [item_manufacturer_name] NVARCHAR(255)
                    )
                """,
                "orders": """
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
                    )
                """,
                "orderItems": """
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
                    )
                """,
                "orderItemAmounts": """
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
                    )
                """,
                EXCEL_WG1_TABLE: """
                    CREATE TABLE [dbo].[wg1] (
                        [code] NVARCHAR(255),
                        [beschreibung] NVARCHAR(MAX),
                        [beschreibung_export] NVARCHAR(MAX)
                    )
                """,
                EXCEL_WG2_TABLE: """
                    CREATE TABLE [dbo].[wg2] (
                        [code] NVARCHAR(255),
                        [beschreibung] NVARCHAR(MAX),
                        [beschreibung_export] NVARCHAR(MAX),
                        [artikelrabattgruppe] NVARCHAR(255)
                    )
                """
            }

            if table_name in table_definitions:
                try:
                    cursor.execute(table_definitions[table_name])
                    cursor.commit()
                    logger.info(f"Tabelle {table_name} erfolgreich erstellt")
                    return True
                except Exception as e:
                    logger.error(f"Fehler beim Erstellen der Tabelle {table_name}: {str(e)}")
                    return False
            else:
                logger.error(f"Keine Tabellendefinition für {table_name} vorhanden")
                return False
    except Exception as e:
        logger.error(f"Fehler bei der Überprüfung der Tabelle {table_name}: {str(e)}")
        return False


def truncate_table(cursor, table_name):
    """Leert eine Tabelle (TRUNCATE)"""
    logger.info(f"Leere Tabelle {table_name}")

    # Stelle sicher, dass die Tabelle existiert
    if not ensure_table_exists(cursor, table_name):
        logger.warning(f"Tabelle {table_name} existiert nicht und konnte nicht erstellt werden")
        return False

    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        cursor.commit()
        logger.info(f"Tabelle {table_name} erfolgreich geleert")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Leeren der Tabelle {table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def write_to_db(cursor, table, columns, data_rows):
    """Schreibt Daten in die Datenbank"""
    if not data_rows or len(data_rows) == 0:
        logger.warning(f"Keine Daten zum Schreiben in Tabelle {table}")
        return 0

    logger.debug(f"Schreibe {len(data_rows)} Zeilen in Tabelle {table}")

    # SQL-Query für INSERT
    query = f'INSERT INTO {table} ({",".join(columns)}) VALUES ({",".join(["?"] * len(columns))})'

    try:
        # Für effizientes Bulk-Insert
        cursor.executemany(query, data_rows)
        return len(data_rows)
    except Exception as e:
        logger.error(f"Fehler beim Schreiben in Tabelle {table}: {str(e)}")
        logger.error(traceback.format_exc())
        # Bei einer problematischen Zeile diese ausgeben
        if data_rows and len(data_rows) > 0:
            logger.debug(f"Erste problematische Zeile: {data_rows[0]}")

        # Versuche es mit einzelnen Inserts, um das Problem zu identifizieren
        logger.info("Versuche Einzelzeilen-Import...")
        success_count = 0
        for i, row in enumerate(data_rows):
            try:
                cursor.execute(query, row)
                cursor.commit()
                success_count += 1
                if success_count % 100 == 0:
                    logger.info(f"Fortschritt: {success_count}/{len(data_rows)} Zeilen importiert")
            except Exception as row_err:
                logger.error(f"Fehler bei Zeile {i}: {str(row_err)}")

        return success_count


def import_data_file(cursor, file_path, table_name, skip_truncate=False):
    """Importiert Daten aus einer CSV-Datei in eine Datenbank-Tabelle"""
    logger.info(f"Importiere Daten aus {file_path} in Tabelle {table_name}")

    try:
        # Überprüfe, ob die Datei existiert
        if not file_path.exists():
            logger.error(f"Datei {file_path} existiert nicht")
            return 0

        # Leere Tabelle, falls gewünscht
        if not skip_truncate:
            if not truncate_table(cursor, table_name):
                logger.warning(f"Konnte Tabelle {table_name} nicht leeren, fahre trotzdem fort")

        # Lade CSV-Datei
        df = pd.read_csv(file_path, dtype=str)  # Lade alle Spalten als Strings

        logger.info(f"CSV-Datei geladen: {len(df)} Zeilen, {len(df.columns)} Spalten")

        # Spalten aus der CSV-Datei
        columns = df.columns.tolist()

        # Daten bereinigen - NaN-Werte durch None ersetzen
        df = df.where(pd.notnull(df), None)

        # Verarbeite die Daten in Chunks
        total_rows = 0
        for i in range(0, len(df), CHUNK_SIZE):
            chunk = df.iloc[i:i + CHUNK_SIZE]
            data_rows = chunk.values.tolist()

            rows_written = write_to_db(cursor, table_name, columns, data_rows)
            total_rows += rows_written

            if i % (CHUNK_SIZE * 10) == 0 and i > 0:
                logger.debug(f"Fortschritt: {i}/{len(df)} Zeilen ({i / len(df) * 100:.1f}%) verarbeitet")
                cursor.commit()  # Zwischenspeichern nach jedem 10. Chunk

        # Commit am Ende
        cursor.commit()

        logger.info(f"Insgesamt {total_rows} Zeilen in Tabelle {table_name} importiert")
        return total_rows

    except Exception as e:
        logger.error(f"Fehler beim Importieren der Datei {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def import_barcodes(cursor):
    """Importiert die Barcode-Daten"""
    logger.info("Importiere Barcode-Daten")

    try:
        # Suche nach der verarbeiteten Barcode-Datei
        barcode_dir = PROCESSED_DIR / "barcodes"

        if not barcode_dir.exists():
            logger.error(f"Barcode-Verzeichnis {barcode_dir} nicht gefunden")
            return 0

        barcode_file = barcode_dir / "barcodes.csv"

        if not barcode_file.exists():
            logger.error(f"Barcode-Datei {barcode_file} nicht gefunden")
            return 0

        # Importiere die Daten
        return import_data_file(cursor, barcode_file, BARCODES_TABLE)

    except Exception as e:
        logger.error(f"Fehler beim Importieren der Barcode-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def import_matchcodes(cursor):
    """Importiert die Matchcode-Daten"""
    logger.info("Importiere Matchcode-Daten")

    try:
        # Suche nach der verarbeiteten Matchcode-Datei
        matchcode_dir = PROCESSED_DIR / "matchcodes"

        if not matchcode_dir.exists():
            logger.error(f"Matchcode-Verzeichnis {matchcode_dir} nicht gefunden")
            return 0

        matchcode_file = matchcode_dir / "matchcodes.csv"

        if not matchcode_file.exists():
            logger.error(f"Matchcode-Datei {matchcode_file} nicht gefunden")
            return 0

        # Importiere die Daten
        return import_data_file(cursor, matchcode_file, MATCHCODES_TABLE)

    except Exception as e:
        logger.error(f"Fehler beim Importieren der Matchcode-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def import_order_data(cursor, type_name, specific_file=None, skip_truncate=False):
    """Importiert die Bestelldaten eines bestimmten Typs"""
    logger.info(f"Importiere Bestelldaten vom Typ {type_name}")

    try:
        # Suche nach der verarbeiteten Datei
        order_dir = PROCESSED_DIR / type_name

        if not order_dir.exists():
            logger.error(f"Verzeichnis {order_dir} nicht gefunden")
            return 0

        # Verwende entweder die angegebene Datei oder die "all" Datei
        if specific_file:
            order_file = Path(specific_file)
            if not order_file.exists():
                logger.error(f"Angegebene Datei {order_file} nicht gefunden")
                return 0
        else:
            order_file = order_dir / f"{type_name}_all.csv"
            if not order_file.exists():
                logger.warning(f"Zusammengefasste Datei {order_file} nicht gefunden, suche nach neuester Datei")

                # Suche nach der neuesten Datei
                csv_files = list(order_dir.glob(f"{type_name}_*.csv"))
                if not csv_files:
                    logger.error(f"Keine CSV-Dateien für {type_name} gefunden")
                    return 0

                # Sortiere nach Änderungsdatum (neueste zuerst)
                csv_files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                order_file = csv_files[0]

        logger.info(f"Verwende Datei {order_file} für Import von {type_name}")

        # Importiere die Daten
        return import_data_file(cursor, order_file, type_name, skip_truncate)

    except Exception as e:
        logger.error(f"Fehler beim Importieren der {type_name}-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def load_excel_file(excel_path):
    """Lädt eine Excel-Datei und gibt sie zurück"""
    logger.info(f"Lade Excel-Datei {excel_path}")

    try:
        # Überprüfe, ob die Datei existiert
        if not os.path.exists(excel_path):
            logger.error(f"Excel-Datei {excel_path} nicht gefunden")

            # Versuche, die CSV-Dateien als Fallback zu verwenden
            wg1_csv = Path("./data") / "wg1.csv"
            wg2_csv = Path("./data") / "wg2.csv"

            if wg1_csv.exists() and wg2_csv.exists():
                logger.info(f"Excel-Datei nicht gefunden, verwende CSV-Dateien stattdessen")

                # Erstelle eine "falsche" Excel-Datei-Struktur, die mit dem Rest des Codes funktioniert
                class MockExcelFile:
                    def __init__(self):
                        self.sheet_names = ["WG1", "WG2"]
                        self._wg1_data = pd.read_csv(wg1_csv)
                        self._wg2_data = pd.read_csv(wg2_csv)

                    def parse(self, sheet_name):
                        if sheet_name == "WG1":
                            return self._wg1_data
                        elif sheet_name == "WG2":
                            return self._wg2_data
                        return None

                return MockExcelFile()

            return None

        # Versuche, die Datei zu öffnen
        try:
            excel_data = pd.ExcelFile(excel_path)
            logger.info(f"Excel-Datei {excel_path} erfolgreich geladen")
            return excel_data
        except Exception as e:
            logger.error(f"Fehler beim Öffnen der Excel-Datei {excel_path}: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    except Exception as e:
        logger.error(f"Unerwarteter Fehler beim Laden der Excel-Datei: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def import_wg1(cursor, excel_data):
    """Importiert die WG1-Daten aus der Excel-Datei"""
    logger.info("Importiere WG1-Daten")

    try:
        # Überprüfe, ob das Sheet existiert
        if "WG1" not in excel_data.sheet_names:
            logger.error("Sheet 'WG1' nicht in der Excel-Datei gefunden")
            return 0

        # Lade das Sheet
        df = excel_data.parse("WG1")

        logger.info(f"WG1-Sheet geladen: {len(df)} Zeilen")

        # Überprüfe erforderliche Spalten
        required_columns = ["Code", "Beschreibung", "Beschreibung Export"]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"Fehlende Spalten im WG1-Sheet: {missing}")
            return 0

        # Leere die Tabelle
        if not truncate_table(cursor, EXCEL_WG1_TABLE):
            logger.warning(f"Konnte Tabelle {EXCEL_WG1_TABLE} nicht leeren, fahre trotzdem fort")

        # Bereite die Daten vor
        data_rows = []
        for _, row in df.iterrows():
            try:
                # Konvertiere NaN zu None
                code = None if pd.isna(row["Code"]) else str(row["Code"])
                beschreibung = None if pd.isna(row["Beschreibung"]) else str(row["Beschreibung"])
                beschreibung_export = None if pd.isna(row["Beschreibung Export"]) else str(row["Beschreibung Export"])

                data_rows.append((code, beschreibung, beschreibung_export))
            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung einer WG1-Zeile: {str(e)}")
                continue

        # Schreibe die Daten
        if data_rows:
            rows_written = write_to_db(cursor, EXCEL_WG1_TABLE, WG1_KEYS, data_rows)
            cursor.commit()
            logger.info(f"{rows_written} WG1-Zeilen in die Datenbank geschrieben")
            return rows_written
        else:
            logger.warning("Keine WG1-Daten zum Importieren")
            return 0

    except Exception as e:
        logger.error(f"Fehler beim Importieren der WG1-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def import_wg2(cursor, excel_data):
    """Importiert die WG2-Daten aus der Excel-Datei"""
    logger.info("Importiere WG2-Daten")

    try:
        # Überprüfe, ob das Sheet existiert
        if "WG2" not in excel_data.sheet_names:
            logger.error("Sheet 'WG2' nicht in der Excel-Datei gefunden")
            return 0

        # Lade das Sheet
        df = excel_data.parse("WG2")

        logger.info(f"WG2-Sheet geladen: {len(df)} Zeilen")

        # Überprüfe erforderliche Spalten
        required_columns = ["Code", "Beschreibung", "Beschreibung Export", "Artikelrabattgruppe"]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.error(f"Fehlende Spalten im WG2-Sheet: {missing}")
            return 0

        # Leere die Tabelle
        if not truncate_table(cursor, EXCEL_WG2_TABLE):
            logger.warning(f"Konnte Tabelle {EXCEL_WG2_TABLE} nicht leeren, fahre trotzdem fort")

        # Bereite die Daten vor
        data_rows = []
        for _, row in df.iterrows():
            try:
                # Konvertiere NaN zu None
                code = None if pd.isna(row["Code"]) else str(row["Code"])
                beschreibung = None if pd.isna(row["Beschreibung"]) else str(row["Beschreibung"])
                beschreibung_export = None if pd.isna(row["Beschreibung Export"]) else str(row["Beschreibung Export"])
                artikelrabattgruppe = None if pd.isna(row["Artikelrabattgruppe"]) else str(row["Artikelrabattgruppe"])

                data_rows.append((code, beschreibung, beschreibung_export, artikelrabattgruppe))
            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung einer WG2-Zeile: {str(e)}")
                continue

        # Schreibe die Daten
        if data_rows:
            rows_written = write_to_db(cursor, EXCEL_WG2_TABLE, WG2_KEYS, data_rows)
            cursor.commit()
            logger.info(f"{rows_written} WG2-Zeilen in die Datenbank geschrieben")
            return rows_written
        else:
            logger.warning("Keine WG2-Daten zum Importieren")
            return 0

    except Exception as e:
        logger.error(f"Fehler beim Importieren der WG2-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return 0


def optimize_table(cursor, table_name):
    """Optimiert eine Tabelle (Rebuild Index, Update Statistics)"""
    logger.info(f"Optimiere Tabelle {table_name}")

    try:
        # In SQL Server: Rebuild aller Indizes und Update Statistics
        cursor.execute(
            f"IF EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('{table_name}') AND type_desc = 'CLUSTERED') ALTER INDEX ALL ON {table_name} REBUILD")
        cursor.execute(f"UPDATE STATISTICS {table_name}")
        cursor.commit()
        logger.info(f"Tabelle {table_name} erfolgreich optimiert")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Optimieren der Tabelle {table_name}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def main():
    """Hauptfunktion für den Datenbankimport"""
    # Argument-Parser
    parser = argparse.ArgumentParser(description="Importiert verarbeitete Daten in die Datenbank")
    parser.add_argument("config_file", help="Pfad zur Konfigurationsdatei")
    parser.add_argument("--skip-barcodes", action="store_true", help="Überspringe den Import der Barcode-Daten")
    parser.add_argument("--skip-orders", action="store_true", help="Überspringe den Import der Bestelldaten")
    parser.add_argument("--skip-external", action="store_true", help="Überspringe den Import der externen CSV")
    parser.add_argument("--skip-excel", action="store_true", help="Überspringe den Import der Excel-Daten")
    parser.add_argument("--skip-truncate", action="store_true",
                        help="Überspringe das Leeren der Tabellen (füge Daten hinzu)")
    parser.add_argument("--skip-optimize", action="store_true", help="Überspringe die Tabellenoptimierung")
    parser.add_argument("--orders-file", help="Spezifische Datei für Orders-Import")
    parser.add_argument("--order-items-file", help="Spezifische Datei für OrderItems-Import")
    parser.add_argument("--order-item-amounts-file", help="Spezifische Datei für OrderItemAmounts-Import")

    args = parser.parse_args()
    logger.info(f"Programm gestartet mit: {vars(args)}")

    # Startzeit für Performance-Messung
    start_time = time.time()

    try:
        # Konfigurationsdatei laden
        config = configparser.ConfigParser()
        config.read(args.config_file)
        logger.info(f"Konfigurationsdatei {args.config_file} geladen")

        # Datenbankverbindungsstring
        try:
            connstring = config.get("database", "connstring")
            logger.debug("Datenbankverbindungsstring aus Konfiguration geladen")
        except (configparser.NoSectionError, configparser.NoOptionError) as e:
            logger.critical(f"Datenbankverbindungsstring nicht gefunden in Konfiguration: {str(e)}")
            sys.exit(1)

        # Verbindung zur Datenbank herstellen
        cnxn, cursor = connect_to_database(connstring)

        # Importstatistik initialisieren
        import_stats = {
            "barcodes": 0,
            "matchcodes": 0,
            "orders": 0,
            "orderItems": 0,
            "orderItemAmounts": 0,
            "wg1": 0,
            "wg2": 0
        }

        # 1. Barcode-Daten importieren
        if not args.skip_barcodes:
            logger.info("Starte Import der Barcode-Daten")
            import_stats["barcodes"] = import_barcodes(cursor)
        else:
            logger.info("Überspringe Import der Barcode-Daten")

        # 2. Matchcode-Daten (externe CSV) importieren
        if not args.skip_external:
            logger.info("Starte Import der Matchcode-Daten")
            import_stats["matchcodes"] = import_matchcodes(cursor)
        else:
            logger.info("Überspringe Import der Matchcode-Daten")

        # 3. Bestelldaten importieren
        if not args.skip_orders:
            logger.info("Starte Import der Bestelldaten")

            # Orders
            import_stats["orders"] = import_order_data(
                cursor, "orders", args.orders_file, args.skip_truncate
            )

            # OrderItems
            import_stats["orderItems"] = import_order_data(
                cursor, "orderItems", args.order_items_file, args.skip_truncate
            )

            # OrderItemAmounts
            import_stats["orderItemAmounts"] = import_order_data(
                cursor, "orderItemAmounts", args.order_item_amounts_file, args.skip_truncate
            )
        else:
            logger.info("Überspringe Import der Bestelldaten")

        # 4. Excel-Daten importieren
        if not args.skip_excel:
            logger.info("Starte Import der Excel-Daten")

            try:
                excel_file = config.get("misc", "excelfile")
                logger.info(f"Excel-Datei konfiguriert: {excel_file}")

                excel_data = load_excel_file(excel_file)

                if excel_data:
                    # WG1 importieren
                    import_stats["wg1"] = import_wg1(cursor, excel_data)

                    # WG2 importieren
                    import_stats["wg2"] = import_wg2(cursor, excel_data)
                else:
                    logger.error("Excel-Datei konnte nicht geladen werden")
            except (configparser.NoSectionError, configparser.NoOptionError) as e:
                logger.warning(f"Excel-Datei nicht in Konfiguration gefunden: {str(e)}")
        else:
            logger.info("Überspringe Import der Excel-Daten")

        # 5. Tabellen optimieren
        if not args.skip_optimize:
            logger.info("Optimiere Datenbanktabellen")

            tables_to_optimize = [
                BARCODES_TABLE, MATCHCODES_TABLE,
                "orders", "orderItems", "orderItemAmounts",
                EXCEL_WG1_TABLE, EXCEL_WG2_TABLE
            ]

            for table in tables_to_optimize:
                optimize_table(cursor, table)
        else:
            logger.info("Überspringe Tabellenoptimierung")

        # Verbindung schließen
        cursor.close()
        cnxn.close()

        # Performance-Messung und Zusammenfassung
        elapsed_time = time.time() - start_time
        logger.info(f"Programm erfolgreich beendet in {elapsed_time:.2f} Sekunden")

        # Abschluss-Statusdatei
        status = {
            "status": "success",
            "timestamp": datetime.datetime.now().isoformat(),
            "runtime_seconds": elapsed_time,
            "import_statistics": import_stats,
            "config_file": args.config_file,
            "skipped": {
                "barcodes": args.skip_barcodes,
                "orders": args.skip_orders,
                "external": args.skip_external,
                "excel": args.skip_excel,
                "truncate": args.skip_truncate,
                "optimize": args.skip_optimize
            }
        }

        # Speichere Status in Config-Verzeichnis
        with open(CONFIG_DIR / "db_import_status.json", 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2)

        logger.info("Import-Status gespeichert")

    except Exception as e:
        logger.critical(f"Unbehandelte Ausnahme: {str(e)}")
        logger.critical(traceback.format_exc())

        # Fehler-Statusdatei
        elapsed_time = time.time() - start_time
        status = {
            "status": "error",
            "timestamp": datetime.datetime.now().isoformat(),
            "runtime_seconds": elapsed_time,
            "error": str(e),
            "config_file": args.config_file
        }

        try:
            # Speichere Status in Config-Verzeichnis
            with open(CONFIG_DIR / "db_import_error.json", 'w', encoding='utf-8') as f:
                json.dump(status, f, indent=2)
        except:
            pass

        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("Programm durch Benutzer unterbrochen")
        sys.exit(2)
    except Exception as e:
        logger.critical(f"Kritischer Fehler im Hauptprogramm: {str(e)}")
        logger.critical(traceback.format_exc())
        sys.exit(1)