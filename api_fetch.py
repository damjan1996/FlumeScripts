import requests
from requests.adapters import Retry, HTTPAdapter
import datetime
import configparser
import argparse
import urllib.parse
import json
import os
import logging
import sys
import time
import traceback
import multiprocessing as mp
from pathlib import Path

# Erstelle Verzeichnisse für Daten und Logs
DATA_DIR = Path("./data")
LOG_DIR = Path("./logs")
DATA_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# Logging konfigurieren
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(processName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "api_fetch.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Konstanten - Angepasst für Ratengrenzprobleme
PAGE_SIZE = 100  # max für items -> definiert durch API-Limits
WORKERS = 1  # Reduziert von 3 auf 1, um API-Limits zu vermeiden
REQUEST_TIMEOUT = 60  # Timeout für HTTP-Requests in Sekunden
API_RATE_LIMIT_SLEEP = 1.0  # Erhöht von 0.1 auf 1.0 Sekunden

# HTTP-Session mit Retry-Mechanismus
logger.info("Initialisiere HTTP-Session mit Retry-Mechanismus")
s = requests.Session()
retries = Retry(
    total=10,
    backoff_factor=2.0,  # Erhöht von 0.8 auf 2.0 für längeres exponentielles Backoff
    status_forcelist=[
        429, 500, 502, 503, 504  # Häufige Server-Fehler-Codes
    ],
)
s.mount("https://", HTTPAdapter(max_retries=retries))


def safe_request(method, url, headers, json_data=None, params=None, timeout=REQUEST_TIMEOUT, retry_count=3):
    """Eine sichere Wrapper-Funktion für HTTP-Requests mit detailliertem Fehler-Logging"""
    current_retry = 0
    while current_retry < retry_count:
        try:
            logger.debug(f"HTTP {method} Request an {url}")
            if method.lower() == 'get':
                response = s.get(url, headers=headers, json=json_data, params=params, timeout=timeout)
            elif method.lower() == 'post':
                response = s.post(url, headers=headers, json=json_data, params=params, timeout=timeout)
            else:
                logger.error(f"Unbekannte HTTP-Methode: {method}")
                return None

            logger.debug(f"HTTP {method} Response: Status {response.status_code}")

            # Spezielle Behandlung für 429 (Too Many Requests)
            if response.status_code == 429:
                wait_time = 60  # Standard-Wartezeit bei Ratengrenzen: 60 Sekunden
                # Versuche Retry-After Header zu lesen, falls vorhanden
                if 'Retry-After' in response.headers:
                    try:
                        wait_time = int(response.headers['Retry-After'])
                    except (ValueError, TypeError):
                        pass

                logger.warning(f"Ratengrenze überschritten (429). Warte {wait_time} Sekunden.")
                time.sleep(wait_time)
                current_retry += 1
                continue

            # Kurze Pause, um API-Rate-Limits nicht zu überschreiten
            time.sleep(API_RATE_LIMIT_SLEEP)
            return response
        except requests.exceptions.Timeout:
            current_retry += 1
            wait_time = 5 * current_retry  # Längere Wartezeit bei Timeouts
            logger.warning(
                f"Timeout bei Request an {url}. Warte {wait_time} Sekunden. Retry {current_retry}/{retry_count}")
            time.sleep(wait_time)  # Exponentielles Backoff
        except requests.exceptions.ConnectionError:
            current_retry += 1
            wait_time = 5 * current_retry  # Längere Wartezeit bei Verbindungsfehlern
            logger.warning(
                f"Verbindungsfehler bei Request an {url}. Warte {wait_time} Sekunden. Retry {current_retry}/{retry_count}")
            time.sleep(wait_time)
        except Exception as e:
            logger.error(f"Fehler bei HTTP Request an {url}: {str(e)}")
            logger.error(traceback.format_exc())
            current_retry += 1
            time.sleep(10)  # 10 Sekunden warten bei anderen Fehlern

    logger.error(f"Maximale Anzahl an Retries erreicht für {url}")
    return None


def save_json_data(data, filename):
    """Speichert Daten als JSON in die angegebene Datei"""
    file_path = DATA_DIR / filename
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Daten erfolgreich in {file_path} gespeichert")
        return True
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Daten in {file_path}: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def get_bearer_token(urlmain, username, password):
    """Authentifizierung und Abrufen des Bearer-Tokens"""
    logger.info(f"Bearer Token wird angefordert von {urlmain}")
    url = f"https://{urlmain}/rest/login"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    json_data = {"username": username, "password": password}

    r = safe_request('post', url, headers=headers, json_data=json_data)
    if not r or r.status_code != 200:
        error_msg = f"Fehler beim Anfordern des Bearer Tokens: Status {r.status_code if r else 'None'}"
        logger.error(error_msg)
        raise Exception(error_msg)

    try:
        access = r.json()
        token_type = access["token_type"]
        token_value = access["access_token"]
        logger.info(f"Bearer Token erfolgreich erhalten")
        # Token auch separat speichern für eventuellen späteren Gebrauch
        token_data = {
            "token_type": token_type,
            "token": token_value,
            "full_token": f"{token_type} {token_value}",
            "timestamp": datetime.datetime.now().isoformat()
        }
        save_json_data(token_data, "bearer_token.json")
        return f"{token_type} {token_value}"
    except Exception as e:
        logger.error(f"Fehler beim Parsen des Bearer Tokens: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def get_barcode_ids(urlmain, authorization):
    """Abrufen der Barcode-IDs für WG1 und WG2"""
    logger.info("Hole Barcode IDs für WG1 und WG2")
    url = f"https://{urlmain}/rest/items/barcodes"
    params = {"itemsPerPage": PAGE_SIZE}
    full_url = f"{url}?{urllib.parse.urlencode(params)}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": authorization,
    }

    t = safe_request('get', full_url, headers=headers)

    if not t or t.status_code != 200:
        error_msg = f"Fehler beim Abrufen der Barcode-IDs: Status {t.status_code if t else 'None'}"
        logger.error(error_msg)
        if t:
            logger.debug(f"Response-Body: {t.text[:500]}...")
        raise Exception(error_msg)

    try:
        data = t.json()
        if not data.get("isLastPage", True):
            logger.warning("Zu viele Barcodes für eine Seite, möglicherweise unvollständige Daten")

        # Alle Barcodes für spätere Referenz speichern
        save_json_data(data, "all_barcodes.json")

        wg1 = -1
        wg2 = -1

        barcode_map = {}
        for entry in data.get("entries", []):
            barcode_id = entry.get("id")
            barcode_name = entry.get("name")
            logger.debug(f"Barcode gefunden: ID={barcode_id}, Name={barcode_name}")
            barcode_map[barcode_name] = barcode_id

            if barcode_name == "WG1":
                wg1 = barcode_id
            if barcode_name == "WG2":
                wg2 = barcode_id

        # Barcode-Map speichern für spätere Referenz
        save_json_data(barcode_map, "barcode_map.json")

        if wg1 < 0 or wg2 < 0:
            error_msg = f"Barcode-IDs nicht gefunden: WG1={wg1}, WG2={wg2}"
            logger.error(error_msg)
            raise Exception(error_msg)

        logger.info(f"Barcode-IDs gefunden: WG1={wg1}, WG2={wg2}")
        return (wg1, wg2)
    except Exception as e:
        logger.error(f"Fehler beim Verarbeiten der Barcode-IDs: {str(e)}")
        logger.error(traceback.format_exc())
        raise


def load_variations_barcodes_page(baseurl, headers, page, wg1=11, wg2=12):
    """Lädt eine einzelne Seite der Variations-Barcodes"""
    logger.debug(f"Lade Variations-Barcodes Seite {page}")

    params = {"page": page, "itemsPerPage": PAGE_SIZE, "with": "variationBarcodes"}
    url = f"{baseurl}?{urllib.parse.urlencode(params)}"

    t = safe_request('get', url, headers=headers)

    if not t or t.status_code != 200:
        logger.error(f"Fehler beim Laden der Variations-Barcodes Seite {page}: Status {t.status_code if t else 'None'}")
        if t:
            logger.debug(f"Response-Body: {t.text[:500]}...")
        return {"page": page, "entries": [], "error": True}

    try:
        data = t.json()
        result = {
            "page": page,
            "entries": [],
            "lastPage": data.get("lastPageNumber", 1),
            "total": data.get("totalsCount", 0),
            "error": False
        }

        for entry in data.get("entries", []):
            variation_id = entry.get("id")
            barcodes = entry.get("variationBarcodes", [])

            # Extrahieren aller Barcodes für umfassendere Speicherung
            barcode_dict = {}
            wg1_code = None
            wg2_code = None

            for bc in barcodes:
                barcode_id = bc.get("barcodeId")
                barcode_code = bc.get("code")
                barcode_dict[barcode_id] = barcode_code

                if barcode_id == wg1:
                    wg1_code = barcode_code
                elif barcode_id == wg2:
                    wg2_code = barcode_code

            # Wir speichern alle verfügbaren Daten für spätere Verarbeitung
            result["entries"].append({
                "variation_id": variation_id,
                "wg1": wg1_code,
                "wg2": wg2_code,
                "all_barcodes": barcode_dict
            })

        logger.debug(f"Seite {page}: {len(result['entries'])} Einträge geladen")

        # Einzelseiten in separaten Dateien speichern für Wiederaufnahme bei Fehlern
        if page % 50 == 0 or page == 1:  # Reduzieren des I/O durch Speichern nur jeder 50. Seite
            save_json_data(result, f"variation_barcodes_page_{page}.json")

        return result

    except Exception as e:
        logger.error(f"Fehler beim Parsen der Barcode-Daten für Seite {page}: {str(e)}")
        logger.error(traceback.format_exc())
        return {"page": page, "entries": [], "error": True}


def barcode_worker_simple(url, headers, page_range, wg1, wg2, shared_results, worker_id):
    """Vereinfachte Worker-Funktion für Barcode-Verarbeitung ohne Queue"""
    worker_name = mp.current_process().name
    logger.info(
        f"Worker {worker_name} (ID {worker_id}) gestartet für Seitenbereich {page_range[0]} bis {page_range[-1]}")

    results = []
    for page in page_range:
        try:
            result = load_variations_barcodes_page(url, headers, page, wg1, wg2)
            if not result["error"]:
                results.extend(result["entries"])
            else:
                logger.error(f"Worker {worker_name}: Fehler bei Seite {page}, überspringe")
                # Warte länger bei Fehlern
                time.sleep(10)
        except Exception as e:
            logger.error(f"Worker {worker_name}: Unbehandelte Ausnahme bei Seite {page}: {str(e)}")
            logger.error(traceback.format_exc())
            # Warte länger bei Fehlern
            time.sleep(10)

    logger.info(f"Worker {worker_name} (ID {worker_id}): {len(results)} Einträge aus {len(page_range)} Seiten geladen")
    # Ergebnisse in die gemeinsame Liste einfügen
    shared_results.append(results)


def load_variations_barcodes(urlmain, authorization, wg1Code, wg2Code):
    """Lädt alle Variations-Barcodes mit Multiprocessing"""
    logger.info("Lade Variations-Barcodes")
    url = f"https://{urlmain}/rest/items/variations"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": authorization,
    }

    # Erste Seite laden und Gesamtseitenanzahl ermitteln
    logger.debug("Lade erste Seite der Variations-Barcodes")
    try:
        first_page = load_variations_barcodes_page(url, headers, 1, wg1Code, wg2Code)

        if first_page["error"]:
            logger.error("Fehler beim Laden der ersten Variations-Barcodes-Seite")
            return []

        total_pages = first_page["lastPage"]
        logger.info(f"Insgesamt {total_pages} Seiten mit Variations-Barcodes gefunden")

        # Erste Seite als separaten JSON speichern
        save_json_data(first_page, "variation_barcodes_page_1_full.json")

        # Nur die erste Seite zurückgeben, wenn es nur eine gibt
        if total_pages <= 1:
            logger.info("Nur eine Seite mit Variations-Barcodes vorhanden")
            return first_page["entries"]

        # Sicherheitsmaßnahme gegen zu viele Seiten
        if total_pages > 1000:
            logger.warning(f"Ungewöhnlich viele Seiten ({total_pages}), beschränke auf 1000")
            total_pages = 1000

        # Multiprocessing Setup mit verbesserten Mechanismen
        logger.info(f"Starte Multiprocessing für {total_pages - 1} weitere Seiten mit {WORKERS} Workern")

        # Seiten auf Worker aufteilen
        all_pages = list(range(2, total_pages + 1))  # Seite 1 haben wir bereits
        chunks = [all_pages[i:i + len(all_pages) // WORKERS + 1]
                  for i in range(0, len(all_pages), len(all_pages) // WORKERS + 1)]

        # Einfachere Implementierung ohne Queue
        processes = []
        manager = mp.Manager()
        shared_results = manager.list()  # Gemeinsam nutzbares Ergebnis-Array

        for i, chunk in enumerate(chunks):
            logger.debug(f"Chunk {i + 1}: {len(chunk)} Seiten von {chunk[0]} bis {chunk[-1]}")
            p = mp.Process(
                target=barcode_worker_simple,
                args=(url, headers, chunk, wg1Code, wg2Code, shared_results, i)
            )
            processes.append(p)
            p.start()

        # Auf Abschluss der Prozesse warten
        for i, p in enumerate(processes):
            p.join(timeout=7200)  # 2 Stunden Timeout (erhöht von 1 Stunde)
            if p.is_alive():
                logger.warning(f"Prozess {i + 1} läuft noch nach Timeout, wird zwangsweise beendet")
                p.terminate()
                p.join()

        # Ergebnisse sammeln
        logger.info("Beginne Zusammenführung der Worker-Ergebnisse")
        all_results = first_page["entries"]  # Start mit erster Seite
        total_entries = len(all_results)
        logger.info(f"Ergebnisse aus Worker-Prozessen sammeln: {len(shared_results)} Teilmengen")

        for part_result in shared_results:
            if part_result:
                all_results.extend(part_result)
                total_entries += len(part_result)

        logger.info(f"Zusammenführung abgeschlossen, {len(all_results)} Einträge insgesamt")
        logger.info(f"Alle Prozesse abgeschlossen. Insgesamt {len(all_results)} Variations-Barcodes geladen")

        # Ergebnisse speichern
        variations_data = {
            "total_count": len(all_results),
            "timestamp": datetime.datetime.now().isoformat(),
            "entries": all_results
        }

        save_json_data(variations_data, "all_variation_barcodes.json")

        return all_results

    except Exception as e:
        logger.error(f"Allgemeiner Fehler beim Laden der Variations-Barcodes: {str(e)}")
        logger.error(traceback.format_exc())
        return []


def fetch_data_for_date(urlmain, shopid, authorization, target_date):
    """Lädt Daten für ein bestimmtes Datum"""
    logger.info(f"Hole Daten für Datum: {target_date.isoformat()}")

    # Datumsformatierung für API
    offset_d = target_date.strftime("%d").zfill(2)
    offset_m_pre = str(target_date.month)
    offset_m = target_date.strftime("%m").zfill(2)
    offset_y = target_date.strftime("%Y")

    # Datadirectory für dieses Datum
    date_dir = DATA_DIR / target_date.strftime("%Y-%m-%d")
    date_dir.mkdir(exist_ok=True)

    # Die drei Datentypen, die wir abrufen
    data_types = ["orderItems", "orders", "orderItemAmounts"]
    versions = ["4", "7", "3"]

    all_results = {}

    # Hier 'f' zu 'type_index' umbenannt, um Namenskonflikte zu vermeiden
    for type_index in range(len(data_types)):
        data_type = data_types[type_index]
        version = versions[type_index]

        logger.info(f"Verarbeite Datentyp {data_type} für {target_date.isoformat()}")

        # Prüfe auf vorhandene Metadaten, um fortzusetzen
        metadata_file = date_dir / f"{data_type}_metadata.json"
        start_page = 1

        if metadata_file.exists():
            try:
                with open(metadata_file, 'r') as metadata_file_handle:
                    metadata = json.load(metadata_file_handle)
                    pages_processed = metadata.get("pages_processed", 0)
                    if pages_processed > 0:
                        logger.info(
                            f"Vorhandene Metadaten gefunden: {pages_processed} Seiten bereits verarbeitet für {data_type}")
                        # Wir beginnen mit der nächsten Seite
                        start_page = pages_processed + 1
            except Exception as e:
                logger.error(f"Fehler beim Lesen der Metadaten für {data_type}: {str(e)}")

        type_results = []
        end_reached = False
        page = start_page

        while not end_reached:
            logger.debug(f"Verarbeite Seite {page} für {data_type} am {target_date.isoformat()}")

            url = f"https://{urlmain}/rest/bi/raw-data/file"
            path = f"report/rawData/{data_type}/{shopid}/{offset_y}/{offset_m_pre}/{data_type}--{offset_y}-{offset_m}-{offset_d}--p{page}--v{version}.csv.gz"
            params = {"path": path}
            full_url = f"{url}?{urllib.parse.urlencode(params)}"

            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": authorization,
            }

            t = safe_request('get', full_url, headers=headers)

            if t and t.status_code == 200:
                try:
                    decoded_content = t.content.decode("utf-8")
                    logger.debug(
                        f"Datei für {data_type} Seite {page} erfolgreich geladen, Größe: {len(t.content)} Bytes")

                    # CSV-Datei für diesen Datentyp und diese Seite speichern
                    csv_filename = date_dir / f"{data_type}_page_{page}.csv"
                    with open(csv_filename, "w", encoding="utf-8") as csv_file:
                        csv_file.write(decoded_content)

                    # Metadaten aktualisieren
                    metadata = {
                        "type": data_type,
                        "date": target_date.isoformat(),
                        "pages_processed": page,
                        "version": version,
                        "last_update": datetime.datetime.now().isoformat()
                    }
                    with open(date_dir / f"{data_type}_metadata.json", "w", encoding="utf-8") as metadata_file_handle:
                        json.dump(metadata, metadata_file_handle, indent=2)

                    page += 1

                except UnicodeDecodeError as e:
                    logger.error(f"Fehler beim Dekodieren der Daten für {data_type} Seite {page}: {str(e)}")
                    # Versuche mit anderen Encodings
                    try:
                        decoded_content = t.content.decode("latin-1")
                        logger.info(f"Fallback auf latin-1 Encoding erfolgreich für {data_type} Seite {page}")

                        csv_filename = date_dir / f"{data_type}_page_{page}_latin1.csv"
                        with open(csv_filename, "w", encoding="latin-1") as csv_file:
                            csv_file.write(decoded_content)

                        # Metadaten aktualisieren
                        metadata = {
                            "type": data_type,
                            "date": target_date.isoformat(),
                            "pages_processed": page,
                            "version": version,
                            "encoding": "latin-1",
                            "last_update": datetime.datetime.now().isoformat()
                        }
                        with open(date_dir / f"{data_type}_metadata.json", "w",
                                  encoding="utf-8") as metadata_file_handle:
                            json.dump(metadata, metadata_file_handle, indent=2)

                        page += 1
                    except Exception as e2:
                        logger.error(f"Auch Fallback-Encoding fehlgeschlagen: {str(e2)}")
                        page += 1

                except Exception as e:
                    logger.error(f"Allgemeiner Fehler bei {data_type} Seite {page}: {str(e)}")
                    logger.error(traceback.format_exc())
                    page += 1

            elif t and t.status_code == 404:
                logger.info(f"Ende erreicht für {data_type} bei Seite {page} (Status 404 - nicht gefunden)")
                end_reached = True

            elif t and t.status_code == 500:
                logger.info(f"Ende erreicht für {data_type} bei Seite {page} (Status 500)")
                end_reached = True

            elif t and t.status_code == 429:
                logger.warning(f"Ratengrenze überschritten für {data_type} Seite {page}. Warte 60 Sekunden.")
                time.sleep(60)  # Lange Pause bei Rate-Limiting
                # Seite nicht erhöhen, um es erneut zu versuchen

            elif t is None:
                logger.error(
                    f"Request fehlgeschlagen für {data_type} Seite {page}, gehe weiter zur nächsten Datenquelle")
                end_reached = True

            else:
                logger.warning(f"Unerwarteter Status-Code {t.status_code} für {data_type} Seite {page}")
                if t.status_code >= 400:
                    logger.error(f"Response-Body: {t.text[:500]}...")

                # Bei Client-Fehlern (4xx) gehen wir davon aus, dass die Seite nicht existiert
                if t.status_code >= 400 and t.status_code < 500 and t.status_code != 429:
                    page += 1
                else:
                    # Bei Server-Fehlern (5xx) außer 500 warten wir und versuchen es erneut
                    time.sleep(30)

                # Bei zu vielen Fehlern könnte ein Abbruch sinnvoll sein
                if page > 50:  # Arbiträre Grenze
                    logger.warning(f"Zu viele Seiten für {data_type}, möglicher Endlosloop. Breche ab.")
                    end_reached = True

        # Metadaten für diesen Datentyp speichern
        metadata = {
            "type": data_type,
            "date": target_date.isoformat(),
            "pages_processed": page - 1,
            "version": version,
            "completed": True,
            "last_update": datetime.datetime.now().isoformat()
        }

        with open(date_dir / f"{data_type}_metadata.json", "w", encoding="utf-8") as metadata_file_handle:
            json.dump(metadata, metadata_file_handle, indent=2)

        logger.info(f"Datentyp {data_type} für {target_date.isoformat()} abgeschlossen, {page - 1} Seiten verarbeitet")

        # Warte zwischen Datentypen
        if type_index < len(data_types) - 1:
            time.sleep(5)  # 5 Sekunden Pause zwischen verschiedenen Datentypen


def order_data_worker(urlmain, shopid, authorization, date_range):
    """Worker-Funktion für paralleles Abrufen von Bestelldaten"""
    worker_name = mp.current_process().name
    logger.info(
        f"Worker {worker_name} gestartet für Datumsbereich von {date_range[0].isoformat()} bis {date_range[-1].isoformat()}")

    for i, target_date in enumerate(date_range):
        try:
            logger.info(
                f"Worker {worker_name}: Verarbeite Tag {i + 1}/{len(date_range)} (Datum {target_date.isoformat()})")
            fetch_data_for_date(urlmain, shopid, authorization, target_date)

            # Warte zwischen den Tagen
            if i < len(date_range) - 1:
                time.sleep(10)  # 10 Sekunden Pause zwischen Tagen

        except Exception as e:
            logger.error(
                f"Worker {worker_name}: Fehler bei der Verarbeitung von Tag {target_date.isoformat()}: {str(e)}")
            logger.error(traceback.format_exc())
            # Längere Pause bei Fehlern
            time.sleep(30)

    logger.info(f"Worker {worker_name}: Alle Tage verarbeitet")


def load_all_historical_data(urlmain, shopid, authorization, start_date="2010-01-01"):
    """Lädt ALLE historischen Bestelldaten vom Startdatum bis gestern"""
    # Konvertiere Startdatum-String in Datumobjekt
    earliest_date = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
    yesterday = datetime.date.today() - datetime.timedelta(days=1)

    # Berechne Gesamtanzahl der zu verarbeitenden Tage
    total_days = (yesterday - earliest_date).days + 1

    # Erzeuge Datumsbereich (neueste zuerst)
    date_range = [yesterday - datetime.timedelta(days=i) for i in range(0, total_days)]

    logger.info(f"Hole ALLE historischen Daten: {total_days} Tage von {earliest_date} bis {yesterday}")

    # Erzeuge Status-Tracking-Datei für Wiederaufnahme
    status_file = DATA_DIR / "historical_fetch_status.json"
    processed_dates = set()

    # Lade bereits verarbeitete Daten, falls Status-Datei existiert
    if status_file.exists():
        try:
            with open(status_file, 'r') as status_file_handle:
                status_data = json.load(status_file_handle)
                processed_dates = set(status_data.get("processed_dates", []))
                logger.info(f"Status-Datei geladen: {len(processed_dates)} Tage bereits verarbeitet")
        except Exception as e:
            logger.error(f"Fehler beim Laden der Status-Datei: {str(e)}")
            logger.error(traceback.format_exc())

    # Verarbeite in Blöcken von 10 Tagen (reduziert von 30)
    chunk_size = 10
    num_chunks = (total_days + chunk_size - 1) // chunk_size

    for i in range(0, len(date_range), chunk_size):
        chunk = date_range[i:i + chunk_size]
        chunk_num = i // chunk_size + 1
        logger.info(
            f"Verarbeite Block {chunk_num}/{num_chunks}: {len(chunk)} Tage ({chunk[0].isoformat()} bis {chunk[-1].isoformat()})")

        # Verarbeite jeden Tag sequentiell (kein Multiprocessing für historische Daten)
        for target_date in chunk:
            date_str = target_date.isoformat()
            if date_str not in processed_dates:
                try:
                    date_dir = DATA_DIR / target_date.strftime("%Y-%m-%d")

                    # Überprüfe, ob Verzeichnis existiert und Daten enthält
                    if date_dir.exists() and list(date_dir.glob("*_metadata.json")):
                        all_complete = True
                        for data_type in ["orderItems", "orders", "orderItemAmounts"]:
                            metadata_file = date_dir / f"{data_type}_metadata.json"
                            if metadata_file.exists():
                                try:
                                    with open(metadata_file, 'r') as metadata_file_handle:
                                        metadata = json.load(metadata_file_handle)
                                        if not metadata.get("completed", False):
                                            all_complete = False
                                            break
                                except:
                                    all_complete = False
                                    break
                            else:
                                all_complete = False
                                break

                        if all_complete:
                            logger.info(f"Datum {date_str} wurde bereits vollständig verarbeitet, überspringe")
                            processed_dates.add(date_str)
                            continue
                        else:
                            logger.info(f"Datum {date_str} wurde teilweise verarbeitet, setze fort")

                    # Hole Daten für dieses Datum
                    fetch_data_for_date(urlmain, shopid, authorization, target_date)
                    processed_dates.add(date_str)

                    # Aktualisiere Status-Datei nach jedem Datum
                    with open(status_file, 'w') as status_file_handle:
                        json.dump({"processed_dates": list(processed_dates)}, status_file_handle)

                    # Warte zwischen Tagen
                    time.sleep(10)  # 10 Sekunden Pause zwischen Tagen

                except Exception as e:
                    logger.error(f"Fehler bei der Verarbeitung von {date_str}: {str(e)}")
                    logger.error(traceback.format_exc())
                    # Längere Pause bei Fehlern
                    time.sleep(60)  # 1 Minute warten bei Fehlern
            else:
                logger.debug(f"Datum {date_str} bereits verarbeitet, überspringe")

        # Längere Pause zwischen Blöcken
        if i + chunk_size < len(date_range):
            sleep_time = 60  # 1 Minute zwischen Blöcken (erhöht von 30 Sekunden)
            logger.info(f"Block {chunk_num} abgeschlossen, Pause von {sleep_time} Sekunden vor dem nächsten Block")
            time.sleep(sleep_time)

    logger.info(f"Historischer Datenabruf abgeschlossen: {len(processed_dates)}/{total_days} Tage verarbeitet")


def load_order_data(urlmain, shopid, authorization, days_to_fetch=1):
    """Lädt Bestelldaten für mehrere Tage mit Multiprocessing"""
    logger.info(f"Starte Abruf von Bestelldaten für die letzten {days_to_fetch} Tage")

    # Datumsliste erstellen
    current_date = datetime.date.today()
    date_range = [current_date - datetime.timedelta(days=i) for i in range(1, days_to_fetch + 1)]

    if len(date_range) == 0:
        logger.warning("Keine Tage zum Abrufen angegeben")
        return

    logger.info(f"Abrufdatum-Bereich: {date_range[0].isoformat()} bis {date_range[-1].isoformat()}")

    # Für alle Datumbereiche: Sequentiell verarbeiten
    logger.info(f"Verarbeite {len(date_range)} Tage sequentiell")
    for target_date in date_range:
        try:
            fetch_data_for_date(urlmain, shopid, authorization, target_date)
            # Kurze Pause zwischen Tagen
            time.sleep(10)
        except Exception as e:
            logger.error(f"Fehler beim Abrufen der Daten für {target_date.isoformat()}: {str(e)}")
            logger.error(traceback.format_exc())
            # Längere Pause bei Fehlern
            time.sleep(30)

    logger.info(f"Alle Bestelldaten für {len(date_range)} Tage erfolgreich abgerufen")


def load_external_csv(external_url):
    """Lädt externe CSV-Datei herunter"""
    logger.info(f"Lade externe CSV von {external_url}")

    t = safe_request('get', external_url, headers={})

    if not t or t.status_code != 200:
        logger.error(f"Fehler beim Abrufen der externen CSV: Status {t.status_code if t else 'None'}")
        if t:
            logger.debug(f"Response-Body: {t.text[:500]}...")
        return False

    logger.info(f"Externe CSV erfolgreich geladen, Größe: {len(t.content)} Bytes")

    try:
        # Versuche verschiedene Encodings
        try:
            content = t.content.decode("utf-8")
            encoding = "utf-8"
        except UnicodeDecodeError:
            try:
                content = t.content.decode("latin-1")
                encoding = "latin-1"
                logger.warning("UTF-8 Dekodierung fehlgeschlagen, Fallback auf latin-1")
            except Exception as e:
                logger.error(f"Fehler beim Dekodieren der externen CSV: {str(e)}")
                return False

        # CSV speichern
        csv_file = DATA_DIR / "external_data.csv"
        with open(csv_file, "w", encoding=encoding) as csv_file_handle:
            csv_file_handle.write(content)

        # Metadaten speichern
        metadata = {
            "url": external_url,
            "size_bytes": len(t.content),
            "encoding": encoding,
            "timestamp": datetime.datetime.now().isoformat()
        }

        metadata_file = DATA_DIR / "external_data_metadata.json"
        with open(metadata_file, "w", encoding="utf-8") as metadata_file_handle:
            json.dump(metadata, metadata_file_handle, indent=2)

        logger.info(f"Externe CSV erfolgreich gespeichert in {csv_file}")
        return True

    except Exception as e:
        logger.error(f"Fehler beim Speichern der externen CSV: {str(e)}")
        logger.error(traceback.format_exc())
        return False


def chunks(lst, n):
    """Teilt eine Liste in n ungefähr gleich große Teile"""
    k, m = divmod(len(lst), n)
    return (lst[i * k + min(i, m): (i + 1) * k + min(i + 1, m)] for i in range(n))


def main():
    # Argument-Parser
    parser = argparse.ArgumentParser(description="API-Daten aus PlentyMarkets abrufen und speichern")
    parser.add_argument("config_file", help="Pfad zur Konfigurationsdatei")
    parser.add_argument("-d", "--days", type=int, default=1,
                        help="Anzahl der Tage für die Bestelldaten abgerufen werden sollen (ab gestern rückwärts)")
    parser.add_argument("--skip-barcodes", action="store_true", help="Überspringe das Abrufen der Variations-Barcodes")
    parser.add_argument("--skip-orders", action="store_true", help="Überspringe das Abrufen der Bestelldaten")
    parser.add_argument("--skip-external", action="store_true", help="Überspringe das Abrufen der externen CSV")
    parser.add_argument("--all-historical", action="store_true",
                        help="Hole ALLE historischen Daten (von 2010 bis gestern)")
    parser.add_argument("--start-date", default="2010-01-01",
                        help="Startdatum für historischen Datenabruf (Format: YYYY-MM-DD)")

    args = parser.parse_args()
    logger.info(f"Programm gestartet mit: {vars(args)}")

    # Startzeit für Performance-Messung
    start_time = time.time()

    try:
        # Konfigurationsdatei laden
        config = configparser.ConfigParser()
        config.read(args.config_file)
        logger.info(f"Konfigurationsdatei {args.config_file} geladen")

        # API-Zugangsdaten
        urlmain = config.get("shop_variablen", "urlmain")
        username = config.get("shop_variablen", "username")
        password = config.get("shop_variablen", "password")
        shopid = config.get("shop_variablen", "shopid")

        logger.info(f"API-Konfiguration: URL={urlmain}, ShopID={shopid}, Username={username}")

        # Bearer-Token abrufen
        authorization = get_bearer_token(urlmain, username, password)

        # Variations-Barcodes abrufen
        if not args.skip_barcodes:
            logger.info("Beginne mit dem Abruf der Variations-Barcodes")
            wg1Code, wg2Code = get_barcode_ids(urlmain, authorization)
            variations = load_variations_barcodes(urlmain, authorization, wg1Code, wg2Code)
            logger.info(f"Variations-Barcodes Abruf abgeschlossen: {len(variations)} Einträge")
        else:
            logger.info("Überspringen des Variations-Barcodes Abrufs")

        # Bestelldaten abrufen: entweder alle historischen oder nur die neuesten
        if args.all_historical and not args.skip_orders:
            logger.info("Starte Abruf ALLER historischen Bestelldaten")
            load_all_historical_data(urlmain, shopid, authorization, args.start_date)
            logger.info("Abruf aller historischen Bestelldaten abgeschlossen")
        elif not args.skip_orders:
            logger.info(f"Beginne mit dem Abruf der Bestelldaten für die letzten {args.days} Tage")
            load_order_data(urlmain, shopid, authorization, args.days)
            logger.info("Bestelldaten Abruf abgeschlossen")
        else:
            logger.info("Überspringen des Bestelldaten Abrufs")

        # Externe CSV abrufen
        if not args.skip_external:
            try:
                external_url = config.get("misc", "externalcsv")
                logger.info(f"Externe CSV-URL konfiguriert: {external_url}")
                load_external_csv(external_url)
            except (configparser.NoSectionError, configparser.NoOptionError):
                logger.info("Keine externe CSV-URL konfiguriert, überspringe")
        else:
            logger.info("Überspringen des externen CSV Abrufs")

        # Performance-Messung und Zusammenfassung
        elapsed_time = time.time() - start_time
        logger.info(f"Programm erfolgreich beendet in {elapsed_time:.2f} Sekunden")

        # Abschluss-Statusdatei
        status = {
            "status": "success",
            "timestamp": datetime.datetime.now().isoformat(),
            "runtime_seconds": elapsed_time,
            "config_file": args.config_file,
            "days_processed": args.days if not args.all_historical else "all",
            "skipped": {
                "barcodes": args.skip_barcodes,
                "orders": args.skip_orders,
                "external": args.skip_external
            }
        }

        save_json_data(status, "api_fetch_status.json")

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

        save_json_data(status, "api_fetch_error.json")

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