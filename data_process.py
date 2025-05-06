import json
import csv
import os
import sys
import logging
import datetime
import argparse
import traceback
import multiprocessing as mp
import pandas as pd
from pathlib import Path
from decimal import Decimal
import pytz
import time
from enum import Enum

# Verzeichnispfade
DATA_DIR = Path("./data")
PROCESSED_DIR = Path("./processed")
LOG_DIR = Path("./logs")

# Erstelle Verzeichnisse, falls sie nicht existieren
DATA_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Logging konfigurieren
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "data_process.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Konstanten
WORKERS = max(1, mp.cpu_count() - 1)  # Nutze alle verfügbaren Kerne außer einem
CHUNK_SIZE = 10000  # Größe der Chunks für Datenverarbeitung
DATETIME_FORMAT = "%m/%d/%Y %H:%M:%S"  # Format für Datums-Parsing


class SQLType(Enum):
    STRING = 1
    BOOL = 2
    NUMBER = 3
    DECIMAL = 4
    DATETIME = 5


# Spaltendefinitionen für die verschiedenen Tabellen
COLUMNS = {
    "orders": {
        "plenty_id": SQLType.STRING,
        "o_id": SQLType.STRING,
        "o_plenty_id": SQLType.STRING,
        "o_origin_order_id": SQLType.STRING,
        "os_id": SQLType.STRING,
        "o_referrer": SQLType.DECIMAL,
        "o_global_referrer": SQLType.DECIMAL,
        "o_type": SQLType.STRING,
        "o_is_main_order": SQLType.BOOL,
        "o_is_net": SQLType.BOOL,
        "o_is_b2b": SQLType.BOOL,
        "ac_id": SQLType.STRING,
        "o_payment_status": SQLType.NUMBER,
        "o_invoice_postal_code": SQLType.STRING,
        "o_invoice_town": SQLType.STRING,
        "o_invoice_country": SQLType.STRING,
        "o_delivery_postal_code": SQLType.STRING,
        "o_delivery_town": SQLType.STRING,
        "o_delivery_country": SQLType.STRING,
        "o_shipping_profile": SQLType.NUMBER,
        "o_parcel_service": SQLType.NUMBER,
        "smw_id": SQLType.STRING,
        "smw_country": SQLType.STRING,
        "smw_postal_code": SQLType.STRING,
        "o_shipping_provider": SQLType.STRING,
        "o_entry_at": SQLType.DATETIME,
        "o_created_at": SQLType.DATETIME,
        "o_goods_issue_at": SQLType.DATETIME,
        "o_paid_at": SQLType.DATETIME,
        "o_updated_at": SQLType.DATETIME,
    },
    "orderItems": {
        "plenty_id": SQLType.STRING,
        "oi_id": SQLType.STRING,
        "o_id": SQLType.STRING,
        "iv_id": SQLType.STRING,
        "i_id": SQLType.STRING,
        "oi_quantity": SQLType.DECIMAL,
        "oi_type_id": SQLType.NUMBER,
        "smw_id": SQLType.STRING,
        "oi_updated_at": SQLType.DATETIME,
        "o_created_at": SQLType.DATETIME,
        "o_updated_at": SQLType.DATETIME,
    },
    "orderItemAmounts": {
        "plenty_id": SQLType.STRING,
        "oia_id": SQLType.STRING,
        "oi_id": SQLType.STRING,
        "oia_is_system_currency": SQLType.BOOL,
        "oi_price_gross": SQLType.DECIMAL,
        "oi_price_net": SQLType.DECIMAL,
        "oi_price_currency": SQLType.STRING,
        "oi_exchange_rate": SQLType.DECIMAL,
        "oi_purchase_price": SQLType.DECIMAL,
        "oi_updated_at": SQLType.DATETIME,
        "o_created_at": SQLType.DATETIME,
    },
}


def parse_date(value):
    """Parst ein Datum im angegebenen Format mit Zeitzoneninformation"""
    try:
        if not value or len(str(value).strip()) == 0:
            return None

        parts = value.split(" ")
        tz = parts[-1]  # extract timezone name
        value = " ".join(parts[:-1])  # recombine without timezone

        # parse without timezone
        dt = datetime.datetime.strptime(value, DATETIME_FORMAT)

        # update timezone info and return
        return dt.replace(tzinfo=pytz.timezone(tz))
    except Exception as e:
        logger.error(f"Fehler beim Parsen des Datums '{value}': {str(e)}")
        return None


def clean_value(type_name, key, value):
    """Konvertiert einen Wert basierend auf seinem definierten Typen"""
    try:
        # catch empty parameters
        if value is None or len(str(value).strip()) == 0:
            return None

        # convert non-empty ones
        column_type = COLUMNS[type_name][key]

        if column_type == SQLType.STRING:
            return str(value)
        elif column_type == SQLType.BOOL:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ('true', 't', 'yes', 'y', '1')
            return bool(value)
        elif column_type == SQLType.NUMBER:
            return int(value)
        elif column_type == SQLType.DECIMAL:
            return float(Decimal(value))
        elif column_type == SQLType.DATETIME:
            if isinstance(value, datetime.datetime):
                return value
            return parse_date(value)
        else:
            logger.warning(f"Unbekannter Spaltentyp für {type_name}.{key}: {column_type}")
            return str(value)

    except Exception as e:
        logger.error(f"Fehler beim Bereinigen des Werts für {type_name}.{key} = '{value}': {str(e)}")
        logger.error(traceback.format_exc())
        # Rückgabe eines sicheren Default-Werts basierend auf dem Spaltentyp
        try:
            column_type = COLUMNS[type_name][key]
            if column_type == SQLType.STRING:
                return ""
            elif column_type == SQLType.BOOL:
                return False
            elif column_type == SQLType.NUMBER:
                return 0
            elif column_type == SQLType.DECIMAL:
                return 0.0
            elif column_type == SQLType.DATETIME:
                return None
            else:
                return None
        except:
            return None


def process_csv_file(file_path, type_name):
    """Verarbeitet eine CSV-Datei und konvertiert die Daten in das richtige Format"""
    logger.debug(f"Verarbeite CSV-Datei: {file_path} für Typ {type_name}")

    try:
        # Encodings ausprobieren
        encodings = ['utf-8', 'latin-1', 'cp1252']
        data = None

        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    reader = csv.DictReader(f)
                    data = list(reader)
                logger.debug(f"CSV erfolgreich mit Encoding {encoding} gelesen")
                break
            except UnicodeDecodeError:
                logger.debug(f"Encoding {encoding} fehlgeschlagen, versuche nächstes")
            except Exception as e:
                logger.error(f"Fehler beim Lesen der Datei mit Encoding {encoding}: {str(e)}")
                raise

        if data is None:
            logger.error(f"Konnte die Datei {file_path} mit keinem der versuchten Encodings lesen")
            return []

        if not data:
            logger.warning(f"Datei {file_path} enthält keine Daten")
            return []

        # Überprüfe und bereinige die Daten
        processed_data = []
        for row in data:
            try:
                processed_row = {}
                for key in COLUMNS[type_name].keys():
                    if key in row:
                        processed_row[key] = clean_value(type_name, key, row[key])
                    else:
                        processed_row[key] = None

                processed_data.append(processed_row)
            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung einer Zeile: {str(e)}")
                logger.debug(f"Problematische Zeile: {row}")
                continue

        logger.debug(f"CSV-Datei {file_path} erfolgreich verarbeitet: {len(processed_data)} Zeilen")
        return processed_data

    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung der CSV-Datei {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def process_date_directory(date_dir, type_name):
    """Verarbeitet alle CSV-Dateien eines bestimmten Typs in einem Datumsverzeichnis"""
    logger.info(f"Verarbeite Verzeichnis {date_dir} für Typ {type_name}")

    # Finde alle relevanten CSV-Dateien
    csv_files = list(date_dir.glob(f"{type_name}_page_*.csv"))

    if not csv_files:
        logger.warning(f"Keine CSV-Dateien für Typ {type_name} in {date_dir} gefunden")
        return []

    logger.info(f"{len(csv_files)} CSV-Dateien für Typ {type_name} in {date_dir} gefunden")

    # Verarbeite jede Datei
    all_data = []
    for csv_file in csv_files:
        data = process_csv_file(csv_file, type_name)
        all_data.extend(data)

    logger.info(f"Insgesamt {len(all_data)} Datensätze für Typ {type_name} aus {date_dir} verarbeitet")
    return all_data


def save_processed_data(data, type_name, date_str=None):
    """Speichert verarbeitete Daten im CSV-Format für den DB-Import"""
    if not data:
        logger.warning(f"Keine Daten zum Speichern für Typ {type_name}")
        return

    # Erzeuge Ausgabeverzeichnis, falls notwendig
    output_dir = PROCESSED_DIR / type_name
    output_dir.mkdir(exist_ok=True)

    # Bestimme Dateinamen
    if date_str:
        output_file = output_dir / f"{type_name}_{date_str}.csv"
    else:
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"{type_name}_{timestamp}.csv"

    logger.info(f"Speichere {len(data)} Datensätze in {output_file}")

    try:
        # Konvertiere zu DataFrame für einfachere CSV-Erstellung
        df = pd.DataFrame(data)

        # Stelle sicher, dass alle erforderlichen Spalten vorhanden sind
        for column in COLUMNS[type_name].keys():
            if column not in df.columns:
                df[column] = None

        # Sortiere Spalten gemäß der definierten Reihenfolge
        df = df[list(COLUMNS[type_name].keys())]

        # Speichere als CSV
        df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)
        logger.debug(f"Daten erfolgreich in {output_file} gespeichert")

        # Speichere Metadaten
        metadata = {
            "type": type_name,
            "row_count": len(data),
            "columns": list(COLUMNS[type_name].keys()),
            "file_path": str(output_file),
            "created_at": datetime.datetime.now().isoformat()
        }

        metadata_file = output_dir / f"{output_file.stem}_metadata.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)

        return str(output_file)

    except Exception as e:
        logger.error(f"Fehler beim Speichern der verarbeiteten Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def process_order_type(type_name, date_dirs):
    """Verarbeitet einen bestimmten Bestelldatentyp für mehrere Datumspfade"""
    logger.info(f"Starte Verarbeitung für Typ {type_name} über {len(date_dirs)} Tage")

    all_data = []

    for date_dir in date_dirs:
        try:
            # Extrahiere Datumsstempel aus dem Verzeichnisnamen
            date_str = date_dir.name

            logger.info(f"Verarbeite Daten für {type_name} am {date_str}")

            # Verarbeite alle CSVs in diesem Datumsverzeichnis
            data = process_date_directory(date_dir, type_name)

            if data:
                # Speichere die Daten für diesen Tag separat
                output_file = save_processed_data(data, type_name, date_str)
                logger.info(f"Daten für {type_name} am {date_str} gespeichert in {output_file}")

                # Füge die Daten zur Gesamtmenge hinzu
                all_data.extend(data)
            else:
                logger.warning(f"Keine Daten für {type_name} am {date_str} gefunden")

        except Exception as e:
            logger.error(f"Fehler bei der Verarbeitung von {type_name} für {date_dir}: {str(e)}")
            logger.error(traceback.format_exc())

    # Speichere die Gesamtdaten
    if all_data:
        output_file = save_processed_data(all_data, type_name, "all")
        logger.info(f"Alle Daten für {type_name} ({len(all_data)} Einträge) gespeichert in {output_file}")
    else:
        logger.warning(f"Keine Daten für {type_name} gefunden")

    return all_data


def process_barcodes():
    """Verarbeitet die Barcode-Daten"""
    logger.info("Verarbeite Barcode-Daten")

    try:
        # Lade die Barcode-Daten
        barcode_file = DATA_DIR / "all_variation_barcodes.json"

        if not barcode_file.exists():
            logger.error(f"Barcode-Datei {barcode_file} nicht gefunden")
            return []

        with open(barcode_file, 'r', encoding='utf-8') as f:
            barcode_data = json.load(f)

        entries = barcode_data.get("entries", [])

        if not entries:
            logger.warning("Keine Barcode-Einträge gefunden")
            return []

        logger.info(f"{len(entries)} Barcode-Einträge geladen")

        # Filtere und transformiere die Daten
        filtered_entries = []
        for entry in entries:
            # Nur Einträge mit WG1 oder WG2 berücksichtigen
            if entry.get("wg1") is not None or entry.get("wg2") is not None:
                filtered_entries.append({
                    "variation_id": entry.get("variation_id"),
                    "wg1": entry.get("wg1"),
                    "wg2": entry.get("wg2")
                })

        logger.info(f"{len(filtered_entries)} relevante Barcode-Einträge gefiltert")

        # Speichere die verarbeiteten Daten
        if filtered_entries:
            output_dir = PROCESSED_DIR / "barcodes"
            output_dir.mkdir(exist_ok=True)

            output_file = output_dir / "barcodes.csv"

            df = pd.DataFrame(filtered_entries)
            df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

            logger.info(f"Barcode-Daten gespeichert in {output_file}")

            # Speichere Metadaten
            metadata = {
                "type": "barcodes",
                "row_count": len(filtered_entries),
                "columns": ["variation_id", "wg1", "wg2"],
                "file_path": str(output_file),
                "created_at": datetime.datetime.now().isoformat()
            }

            metadata_file = output_dir / "barcodes_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)

            return filtered_entries
        else:
            logger.warning("Keine relevanten Barcode-Einträge zum Speichern")
            return []

    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung der Barcode-Daten: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def process_external_csv():
    """Verarbeitet die externe CSV-Datei"""
    logger.info("Verarbeite externe CSV-Daten")

    try:
        # Überprüfe, ob die externe CSV existiert
        external_csv = DATA_DIR / "external_data.csv"

        if not external_csv.exists():
            logger.warning(f"Externe CSV-Datei {external_csv} nicht gefunden")
            return []

        # Suche nach Metadaten
        metadata_file = DATA_DIR / "external_data_metadata.json"
        encoding = 'utf-8'

        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                    encoding = metadata.get("encoding", 'utf-8')
            except Exception as e:
                logger.warning(f"Fehler beim Lesen der Metadaten, verwende Standard-Encoding: {str(e)}")

        # Lade die externe CSV
        logger.info(f"Lade externe CSV mit Encoding {encoding}")

        try:
            df = pd.read_csv(external_csv, sep=';', encoding=encoding)
        except UnicodeDecodeError:
            logger.warning(f"Encoding {encoding} fehlgeschlagen, versuche alternative Encodings")
            for alt_encoding in ['latin-1', 'cp1252', 'utf-8-sig']:
                try:
                    df = pd.read_csv(external_csv, sep=';', encoding=alt_encoding)
                    logger.info(f"CSV erfolgreich mit Encoding {alt_encoding} gelesen")
                    encoding = alt_encoding
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.error(f"Fehler beim Lesen mit Encoding {alt_encoding}: {str(e)}")
                    raise
            else:
                logger.error("Konnte die externe CSV mit keinem der versuchten Encodings lesen")
                return []

        logger.info(f"Externe CSV geladen: {len(df)} Zeilen, {len(df.columns)} Spalten")

        # Überprüfe erforderliche Spalten
        required_columns = [
            "Variation.id", "Variation.number", "Variation.name",
            "ItemDescription.name", "VariationDefaultCategory.branchName"
        ]

        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            logger.warning(f"Fehlende Spalten in externer CSV: {missing}")

        # Bereinige und transformiere die Daten
        processed_data = []

        for _, row in df.iterrows():
            try:
                entry = {
                    "variation_id": row.get("Variation.id"),
                    "variation_number": row.get("Variation.number"),
                    "variation_name": row.get("Variation.name"),
                    "item_description_name": row.get("ItemDescription.name"),
                    "variation_default_category_branch_name": row.get("VariationDefaultCategory.branchName"),
                    "variation_default_category_manually": None,
                    "variation_bundle_components": None,
                    "item_manufacturer_name": None
                }

                # Zusätzliche optionale Spalten
                if "VariationDefaultCategory.manually" in row:
                    manually_value = row.get("VariationDefaultCategory.manually")
                    if pd.notna(manually_value) and manually_value != "":
                        try:
                            entry["variation_default_category_manually"] = int(manually_value)
                        except (ValueError, TypeError):
                            logger.warning(f"Ungültiger Wert für VariationDefaultCategory.manually: {manually_value}")

                if "VariationBundle.components" in row:
                    entry["variation_bundle_components"] = row.get("VariationBundle.components")

                if "Item.manufacturerName" in row:
                    entry["item_manufacturer_name"] = row.get("Item.manufacturerName")

                processed_data.append(entry)

            except Exception as e:
                logger.error(f"Fehler bei der Verarbeitung einer Zeile der externen CSV: {str(e)}")
                continue

        logger.info(f"{len(processed_data)} Einträge aus externer CSV verarbeitet")

        # Speichere die verarbeiteten Daten
        if processed_data:
            output_dir = PROCESSED_DIR / "matchcodes"
            output_dir.mkdir(exist_ok=True)

            output_file = output_dir / "matchcodes.csv"

            matchcodes_df = pd.DataFrame(processed_data)
            matchcodes_df.to_csv(output_file, index=False, quoting=csv.QUOTE_NONNUMERIC)

            logger.info(f"Matchcodes-Daten gespeichert in {output_file}")

            # Speichere Metadaten
            metadata = {
                "type": "matchcodes",
                "row_count": len(processed_data),
                "columns": list(processed_data[0].keys()) if processed_data else [],
                "file_path": str(output_file),
                "created_at": datetime.datetime.now().isoformat(),
                "source_encoding": encoding
            }

            metadata_file = output_dir / "matchcodes_metadata.json"
            with open(metadata_file, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)

            return processed_data
        else:
            logger.warning("Keine Daten aus externer CSV zum Speichern")
            return []

    except Exception as e:
        logger.error(f"Fehler bei der Verarbeitung der externen CSV: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def process_excel_data():
    """Verarbeitet die Excel-Daten (WG1 und WG2)"""
    # In diesem Fallbeispiel nehmen wir an, dass die Excel-Daten nicht im Rahmen des Datenabrufs heruntergeladen wurden
    # und werden daher in einem separaten Schritt verarbeitet (im db_import.py Script)
    logger.info("Excel-Datenverarbeitung wird im db_import.py Script durchgeführt")
    return True


def main():
    """Hauptfunktion für die Datenverarbeitung"""
    # Argument-Parser
    parser = argparse.ArgumentParser(description="Verarbeitet die abgerufenen PlentyMarkets API-Daten")
    parser.add_argument("--skip-barcodes", action="store_true", help="Überspringe die Verarbeitung der Barcode-Daten")
    parser.add_argument("--skip-orders", action="store_true", help="Überspringe die Verarbeitung der Bestelldaten")
    parser.add_argument("--skip-external", action="store_true", help="Überspringe die Verarbeitung der externen CSV")
    parser.add_argument("--days", type=int, default=None,
                        help="Nur die angegebene Anzahl an Tagen verarbeiten (die neuesten zuerst)")

    args = parser.parse_args()
    logger.info(f"Programm gestartet mit: {vars(args)}")

    # Startzeit für Performance-Messung
    start_time = time.time()

    try:
        # Verarbeitungsstatus initialisieren
        status = {
            "status": "success",
            "timestamp": datetime.datetime.now().isoformat(),
            "processed": {
                "barcodes": 0,
                "orders": 0,
                "orderItems": 0,
                "orderItemAmounts": 0,
                "external": 0
            },
            "errors": []
        }

        # 1. Barcode-Daten verarbeiten
        if not args.skip_barcodes:
            logger.info("Starte Verarbeitung der Barcode-Daten")
            barcodes = process_barcodes()
            status["processed"]["barcodes"] = len(barcodes)
        else:
            logger.info("Überspringe Verarbeitung der Barcode-Daten")

        # 2. Bestelldaten verarbeiten
        if not args.skip_orders:
            logger.info("Starte Verarbeitung der Bestelldaten")

            # Finde alle Datumsverzeichnisse
            date_dirs = [d for d in DATA_DIR.iterdir() if d.is_dir() and d.name.startswith("20")]
            date_dirs.sort(reverse=True)  # Neueste zuerst

            if args.days is not None and args.days > 0:
                date_dirs = date_dirs[:args.days]
                logger.info(f"Beschränke Verarbeitung auf die neuesten {args.days} Tage")

            logger.info(f"{len(date_dirs)} Datumsverzeichnisse gefunden: {[d.name for d in date_dirs]}")

            # Verarbeite jeden Bestelldatentyp
            for type_name in ["orders", "orderItems", "orderItemAmounts"]:
                try:
                    data = process_order_type(type_name, date_dirs)
                    status["processed"][type_name] = len(data)
                except Exception as e:
                    logger.error(f"Fehler bei der Verarbeitung von {type_name}: {str(e)}")
                    logger.error(traceback.format_exc())
                    status["errors"].append(f"Error processing {type_name}: {str(e)}")
        else:
            logger.info("Überspringe Verarbeitung der Bestelldaten")

        # 3. Externe CSV verarbeiten
        if not args.skip_external:
            logger.info("Starte Verarbeitung der externen CSV")
            external_data = process_external_csv()
            status["processed"]["external"] = len(external_data)
        else:
            logger.info("Überspringe Verarbeitung der externen CSV")

        # 4. Excel-Daten verarbeiten (in diesem Beispiel übersprungen, da im db_import.py)
        process_excel_data()

        # Performance-Messung und Zusammenfassung
        elapsed_time = time.time() - start_time
        logger.info(f"Programm erfolgreich beendet in {elapsed_time:.2f} Sekunden")

        # Abschluss-Statusdatei
        status["runtime_seconds"] = elapsed_time
        status["completed_at"] = datetime.datetime.now().isoformat()

        with open(PROCESSED_DIR / "data_process_status.json", 'w', encoding='utf-8') as f:
            json.dump(status, f, indent=2)

        logger.info("Statusdatei gespeichert")

    except Exception as e:
        logger.critical(f"Unbehandelte Ausnahme: {str(e)}")
        logger.critical(traceback.format_exc())

        # Fehler-Statusdatei
        elapsed_time = time.time() - start_time
        status = {
            "status": "error",
            "timestamp": datetime.datetime.now().isoformat(),
            "runtime_seconds": elapsed_time,
            "error": str(e)
        }

        try:
            with open(PROCESSED_DIR / "data_process_error.json", 'w', encoding='utf-8') as f:
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