# anime_etl.py
# ETL по аниме-датасету
# Диана, курс "Инженер данных", итоговый проект
# версия 4.0 - тут уже без лишних кавычек в CSV, до этого Excel ломался

import pandas as pd
import numpy as np
from pathlib import Path
import sqlite3
import logging
import time
import os

# пути (надо будет поменять на свои при загрузке на GitHub)
INPUT_FOLDER = 'data'
OUTPUT_FOLDER = 'output'

# создаю папки, если их нет, а то скрипт упадет
Path(OUTPUT_FOLDER).mkdir(exist_ok=True)

# логирование, чтобы видеть что происходит
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(Path(OUTPUT_FOLDER) / 'etl_process.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# ---- проверка файлов ----
# сначала посмотрела глазами что лежит в папке, потом уже написала это
print("\n" + "=" * 80)
print("ПРОВЕРКА ФАЙЛОВ В ПАПКЕ")
print("=" * 80)
print("путь:", INPUT_FOLDER)

if os.path.exists(INPUT_FOLDER):
    files = os.listdir(INPUT_FOLDER)
    print("всего файлов:", len(files))
    for f in files:
        print("  ", f)

    csv_files = [f for f in files if f.endswith('.csv')]
    print("\nCSV файлов:", len(csv_files))
    for f in csv_files:
        print("  +", f)
else:
    print("папка не найдена!")
print("=" * 80 + "\n")


# ---- загрузка ----
# тут пытаюсь разные кодировки, вдруг файл не в utf-8
def load_csv_file(file_path):
    encodings = ['utf-8', 'windows-1251', 'latin-1', 'cp1252', 'utf-8-sig']
    for enc in encodings:
        try:
            return pd.read_csv(file_path, encoding=enc)
        except:
            continue
    raise Exception(f"не смогла прочитать {file_path}")


def load_all_files():
    logging.info("=" * 60)
    logging.info("ШАГ 1: ЗАГРУЗКА ФАЙЛОВ")
    logging.info("=" * 60)

    files = {
        'anime': 'anime.csv',
        'anime_characters': 'anime_characters.csv',
        'anime_companies': 'anime_companies.csv',
        'anime_genres': 'anime_genres.csv',
        'anime_staff': 'anime_staff.csv',
        'anime_voice_actors': 'anime_voice_actors.csv',
        'entities': 'entities.csv'
    }

    data = {}
    for name, filename in files.items():
        fp = Path(INPUT_FOLDER) / filename
        if fp.exists():
            try:
                data[name] = load_csv_file(fp)
                logging.info(f"  {filename}: {len(data[name]):,} строк")
            except Exception as e:
                logging.error(f"  ошибка {filename}: {e}")
                data[name] = None
        else:
            logging.warning(f"  файл не найден: {filename}")
            data[name] = None

    return data


# ---- чистка основной таблицы ----
def clean_anime(df):
    logging.info("=" * 60)
    logging.info("ШАГ 2.1: ОЧИСТКА anime")
    logging.info("=" * 60)

    if df is None:
        return None

    df_clean = df.copy()
    logging.info(f"строк было: {len(df_clean):,}")

    # названия колонок привожу к одному виду
    df_clean.columns = df_clean.columns.str.strip().str.lower().str.replace(' ', '_')

    # дубликаты по anime_id
    if 'anime_id' in df_clean.columns:
        before = len(df_clean)
        df_clean = df_clean.drop_duplicates(subset=['anime_id'])
        logging.info(f"  удалила дубликатов: {before - len(df_clean)}")

    # числовые колонки — там были строки типа "8.5" и "1,234"
    numeric_cols = ['score', 'scored_by', 'rank', 'popularity', 'members', 'favorites']
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str)
            df_clean[col] = df_clean[col].str.replace(r'[^0-9.]', '', regex=True)
            df_clean[col] = df_clean[col].replace('', '0')
            df_clean[col] = df_clean[col].replace(' ', '0')
            # беру целую часть, потому что дробные мне не нужны
            df_clean[col] = df_clean[col].str.split('.').str[0]
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0)
            df_clean[col] = df_clean[col].astype(float)
            logging.info(f"  {col}: преобразовала в число")

    # текстовые колонки
    text_cols = ['title', 'type', 'status', 'source', 'rating', 'synopsis']
    for col in text_cols:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip()
            df_clean[col] = df_clean[col].replace(['nan', 'None', 'NULL', ''], 'unknown')

    # год из aired (там строка типа "Apr 3, 2004 to ...")
    if 'aired' in df_clean.columns:
        df_clean['year'] = df_clean['aired'].str.extract(r'(\d{4})')[0]
        df_clean['year'] = pd.to_numeric(df_clean['year'], errors='coerce').fillna(0).astype(int)

    logging.info(f"итого строк: {len(df_clean):,}")
    return df_clean


def clean_entities(df):
    if df is None:
        return None
    df_clean = df.copy()
    df_clean.columns = df_clean.columns.str.strip().str.lower().str.replace(' ', '_')
    if 'entity_id' in df_clean.columns:
        df_clean = df_clean.drop_duplicates(subset=['entity_id'])
    if 'name' in df_clean.columns:
        df_clean['name'] = df_clean['name'].astype(str).str.strip()
        df_clean['name'] = df_clean['name'].replace(['nan', 'None', ''], 'unknown')
    return df_clean


def clean_linking_table(df, table_name):
    if df is None:
        return None
    df_clean = df.copy()
    df_clean.columns = df_clean.columns.str.strip().str.lower().str.replace(' ', '_')
    df_clean = df_clean.drop_duplicates()
    # все колонки, где в названии есть id — делаю int
    for col in df_clean.columns:
        if 'id' in col.lower():
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce').fillna(0).astype(int)
    return df_clean


# ---- объединение ----
# тут самое сложное. Сначала пыталась через merge по очереди,
# потом поняла, что для many-to-many надо сначала агрегировать.
def merge_all_data(anime_df, entities_df, linking_dfs):
    logging.info("=" * 60)
    logging.info("ШАГ 3: ОБЪЕДИНЕНИЕ")
    logging.info("=" * 60)

    if anime_df is None:
        logging.error("нет основной таблицы!")
        return None

    merged = anime_df.copy()

    # жанры
    if linking_dfs.get('anime_genres') is not None:
        df_genres = linking_dfs['anime_genres']
        if 'anime_id' in df_genres.columns and 'genre' in df_genres.columns:
            genres_agg = df_genres.groupby('anime_id')['genre'].agg(lambda x: ', '.join(x)).reset_index()
            genres_agg.columns = ['anime_id', 'genres_list']
            merged = merged.merge(genres_agg, on='anime_id', how='left')
            merged['genres_list'] = merged['genres_list'].fillna('')
            logging.info("  жанры добавлены")

    # компании — тут пришлось джойнить с entities чтобы получить названия
    if linking_dfs.get('anime_companies') is not None and entities_df is not None:
        df_companies = linking_dfs['anime_companies']
        if 'anime_id' in df_companies.columns and 'company_id' in df_companies.columns:
            companies_with_names = df_companies.merge(
                entities_df[['entity_id', 'name']],
                left_on='company_id',
                right_on='entity_id',
                how='left'
            )
            companies_agg = companies_with_names.groupby('anime_id')['name'].agg(
                lambda x: ', '.join(x.dropna().unique())
            ).reset_index()
            companies_agg.columns = ['anime_id', 'companies_list']
            merged = merged.merge(companies_agg, on='anime_id', how='left')
            merged['companies_list'] = merged['companies_list'].fillna('')
            logging.info("  компании добавлены")

    # персонажи — просто считаю сколько их
    if linking_dfs.get('anime_characters') is not None:
        df_chars = linking_dfs['anime_characters']
        if 'anime_id' in df_chars.columns:
            char_count = df_chars.groupby('anime_id').size().reset_index(name='characters_count')
            merged = merged.merge(char_count, on='anime_id', how='left')
            merged['characters_count'] = merged['characters_count'].fillna(0).astype(int)
            logging.info("  персонажи добавлены")

    # актёры озвучки
    if linking_dfs.get('anime_voice_actors') is not None:
        df_va = linking_dfs['anime_voice_actors']
        if 'anime_id' in df_va.columns:
            va_count = df_va.groupby('anime_id').size().reset_index(name='voice_actors_count')
            merged = merged.merge(va_count, on='anime_id', how='left')
            merged['voice_actors_count'] = merged['voice_actors_count'].fillna(0).astype(int)
            logging.info("  актёры озвучки добавлены")
        else:
            merged['voice_actors_count'] = 0
            logging.warning("  в anime_voice_actors нет anime_id")

    logging.info(f"итог: {len(merged):,} строк, {len(merged.columns):,} колонок")
    return merged


# ---- сохранение ----
# тут был баг: Excel ругался на кавычки в тексте.
# Решение нашла на stackoverflow: quoting=1 в to_csv.
def save_results(merged_df, data_dict):
    logging.info("=" * 60)
    logging.info("ШАГ 4: СОХРАНЕНИЕ")
    logging.info("=" * 60)

    if merged_df is None:
        logging.error("нечего сохранять!")
        return

    # на всякий случай ещё раз привожу числа к float,
    # потому что после merge типы могли съехать
    numeric_cols_final = ['score', 'rank', 'popularity', 'members', 'scored_by', 'favorites', 'characters_count', 'voice_actors_count']
    for col in numeric_cols_final:
        if col in merged_df.columns:
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce').fillna(0)
            merged_df[col] = merged_df[col].astype(float)
            logging.info(f"  {col}: ещё раз привела к числу")

    # csv
    csv_path = Path(OUTPUT_FOLDER) / 'anime_clean_data_v3.csv'
    merged_df.to_csv(csv_path, index=False, encoding='utf-8-sig', quoting=1)
    logging.info(f"  CSV: {csv_path}")

    # excel — может упасть, поэтому try
    try:
        excel_path = Path(OUTPUT_FOLDER) / 'anime_clean_data_v3.xlsx'
        merged_df.to_excel(excel_path, index=False, engine='openpyxl')
        logging.info(f"  Excel: {excel_path}")
    except Exception as e:
        logging.warning(f"  Excel не сохранился: {e}")

    # sqlite
    db_path = Path(OUTPUT_FOLDER) / 'anime_database.db'
    conn = sqlite3.connect(db_path)
    merged_df.to_sql('anime_master', conn, if_exists='replace', index=False)
    conn.close()
    logging.info(f"  SQLite: {db_path}")


def main():
    start = time.time()

    print("\n" + "=" * 80)
    print("ЗАПУСК ETL")
    print("=" * 80)

    data = load_all_files()

    anime_clean = clean_anime(data.get('anime'))
    entities_clean = clean_entities(data.get('entities'))

    linking_tables = {}
    for name in ['anime_genres', 'anime_companies', 'anime_staff', 'anime_characters', 'anime_voice_actors']:
        linking_tables[name] = clean_linking_table(data.get(name), name)

    merged = merge_all_data(anime_clean, entities_clean, linking_tables)

    save_results(merged, data)

    elapsed = time.time() - start
    print("\n" + "=" * 80)
    print("ИТОГО")
    print("=" * 80)
    if anime_clean is not None:
        print("  аниме:", len(anime_clean))
    if entities_clean is not None:
        print("  сущностей:", len(entities_clean))
    if merged is not None:
        print("  итоговая таблица:", len(merged), "строк,", len(merged.columns), "колонок")
    print("  время:", round(elapsed, 2), "сек")
    print("=" * 80)


if __name__ == "__main__":
    main()
