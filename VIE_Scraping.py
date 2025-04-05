#!/usr/bin/env python
# coding: utf-8

# In[1]:


get_ipython().system('pip install gspread oauth2client beautifulsoup4 requests')


# In[28]:


import requests
import json
import os
import csv
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
from gspread.exceptions import SpreadsheetNotFound

# === CONFIGURATION ===
DATA_DIR = "/Users/akimguentas/Desktop/PROJET VIE"
SPREADSHEET_NAME = "Offres VIE - Database"
CREDENTIALS_FILE = "secrets/vie-project-455522-0c2604736a99.json"
MAX_WORKERS = 5

SEARCH_API_URL = "https://civiweb-api-prd.azurewebsites.net/api/Offers/search"
DETAILS_API_URL = "https://civiweb-api-prd.azurewebsites.net/api/Offers/details"

HEADERS = {
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'fr',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json;charset=UTF-8',
    'Origin': 'https://mon-vie-via.businessfrance.fr',
    'Referer': 'https://mon-vie-via.businessfrance.fr/',
    'User-Agent': 'Mozilla/5.0'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_offers(batch_size=50, max_offers=None):
    all_offers = []
    skip = 0
    total_count = None
    logger.info("🟡 Récupération des offres...")

    while True:
        if max_offers and len(all_offers) >= max_offers:
            break

        current_limit = min(batch_size, max_offers - len(all_offers)) if max_offers else batch_size

        payload = {
            "limit": current_limit,
            "skip": skip,
            "query": "",
            "activitySectorId": [],
            "missionsTypesIds": [],
            "missionsDurations": [],
            "gerographicZones": [],
            "countriesIds": [],
            "studiesLevelId": [],
            "companiesSizes": [],
            "specializationsIds": [],
            "entreprisesIds": [0],
            "missionStartDate": None
        }

        try:
            response = requests.post(SEARCH_API_URL, headers=HEADERS, json=payload, timeout=45)
            if response.status_code == 200:
                data = response.json()
                total_count = total_count or data.get('count')
                offers = data.get('result', [])
                all_offers.extend(offers)
                logger.info(f"✅ {len(all_offers)} / {total_count} offres récupérées")
                if len(offers) < current_limit:
                    break
                skip += len(offers)
                time.sleep(0.5)
            else:
                logger.error(f"❌ Erreur HTTP: {response.status_code}")
                break
        except Exception as e:
            logger.error(f"❌ Erreur: {e}")
            break

    os.makedirs(DATA_DIR, exist_ok=True)
    filename = f"raw_offers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(os.path.join(DATA_DIR, filename), 'w', encoding='utf-8') as f:
        json.dump({"count": total_count, "result": all_offers}, f, ensure_ascii=False, indent=4)

    return all_offers

def get_offer_details_by_id(offer_id):
    try:
        response = requests.get(f"{DETAILS_API_URL}/{offer_id}", headers=HEADERS, timeout=30)
        return response.json() if response.status_code == 200 else None
    except Exception as e:
        logger.error(f"Erreur récupération offre {offer_id} : {e}")
        return None

def get_detailed_offers(offer_ids, max_workers=MAX_WORKERS):
    logger.info("🟡 Récupération des détails des offres...")
    detailed = []
    batch_size = 20

    for i in range(0, len(offer_ids), batch_size):
        ids = offer_ids[i:i+batch_size]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(get_offer_details_by_id, oid): oid for oid in ids}
            for future in futures:
                result = future.result()
                if result:
                    detailed.append(result)
        logger.info(f"📦 {len(detailed)} / {len(offer_ids)} détails récupérés")
        time.sleep(1)
    return detailed

def clean_text(text):
    return text.replace('\n', ' ').replace('\r', ' ') if text else ""

def save_to_csv(data, filename=None):
    if not data:
        logger.warning("⚠️ Aucune donnée à sauvegarder.")
        return
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = filename or f"business_france_detailed_offers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    filepath = os.path.join(DATA_DIR, filename)
    columns = list(data[0].keys())
    for item in data:
        for key in item:
            if isinstance(item[key], str):
                item[key] = clean_text(item[key])
    with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=columns, delimiter=';', quoting=csv.QUOTE_ALL)
        writer.writeheader()
        writer.writerows(data)
    logger.info(f"💾 Données sauvegardées dans {filepath}")

def save_to_json(data, filename=None):
    os.makedirs(DATA_DIR, exist_ok=True)
    filename = filename or f"business_france_detailed_offers_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"💾 JSON sauvegardé dans {filepath}")

def append_to_gsheet(new_data_df, spreadsheet_name, credentials_path):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_path, scope)
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open(spreadsheet_name)
        sheet = spreadsheet.sheet1
        logger.info(f"📗 Feuille trouvée : {spreadsheet_name}")
    except SpreadsheetNotFound:
        logger.warning(f"📕 Feuille non trouvée. Création automatique de : {spreadsheet_name}")
        spreadsheet = client.create(spreadsheet_name)
        sheet = spreadsheet.sheet1
        client.insert_permission(
            spreadsheet.id,
            value='akimguentas13@gmail.com',
            perm_type='user',
            role='writer'
        )
        logger.info(f"✅ Feuille créée et partagée avec akimguentas13@gmail.com")

    try:
        existing_headers = sheet.row_values(1)
        if not existing_headers:
            headers = new_data_df.columns.tolist()
            sheet.append_row(headers)
            logger.info(f"✅ En-têtes ajoutés au tableau Google Sheets.")
            existing_data = []
        else:
            existing_data = sheet.get_all_records()
    except IndexError:
        headers = new_data_df.columns.tolist()
        sheet.append_row(headers)
        logger.info(f"✅ En-têtes ajoutés au tableau Google Sheets.")
        existing_data = []

    existing_df = pd.DataFrame(existing_data)

    if existing_df.empty and not new_data_df.empty:
        rows_to_add = new_data_df.values.tolist()
        if rows_to_add:
            sheet.append_rows(rows_to_add, value_input_option='RAW')
            logger.info(f"🟢 {len(rows_to_add)} nouvelles lignes ajoutées à Google Sheets.")
        return

    if 'id' in existing_df.columns and 'id' in new_data_df.columns:
        new_rows = new_data_df[~new_data_df['id'].isin(existing_df['id'])]
    else:
        new_rows = new_data_df

    if not new_rows.empty:
        rows_to_add = new_rows.values.tolist()
        sheet.append_rows(rows_to_add, value_input_option='RAW')
        logger.info(f"🟢 {len(new_rows)} nouvelles lignes ajoutées à Google Sheets.")
    else:
        logger.info("✅ Aucune nouvelle ligne à ajouter.")

def main():
    print("=== Scraper Business France ===")
    max_offers_input = input("Combien d’offres à récupérer ? (laisse vide pour tout) : ")
    max_offers = int(max_offers_input) if max_offers_input.strip().isdigit() else None

    offers = fetch_offers(max_offers=max_offers)
    if not offers:
        logger.error("❌ Aucune offre récupérée.")
        return

    offer_ids = [o['id'] for o in offers if 'id' in o]
    details = get_detailed_offers(offer_ids)

    if not details:
        logger.warning("❌ Aucune donnée détaillée.")
        return

    save_to_csv(details)
    save_to_json(details)

    df = pd.DataFrame(details)
    append_to_gsheet(df, spreadsheet_name=SPREADSHEET_NAME, credentials_path=CREDENTIALS_FILE)

if __name__ == "__main__":
    main()


# In[ ]:




