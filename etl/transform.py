import pandas as pd
from datetime import datetime
import pytz

def normalize_name(name: str) -> str:
    return ' '.join(name.strip().title().split())

def to_utc_iso(dt_str: str) -> str:
    try:
        dt = pd.to_datetime(dt_str, utc=True)
        return dt.isoformat()
    except Exception:
        return None

def clean_catalogs(df: pd.DataFrame, batch_id: int):
    cleaned = []
    bad = []
    for _, row in df.iterrows():
        record = row.to_dict()
        try:
            name = normalize_name(str(record['name']))
            created_at = to_utc_iso(str(record['created_at']))
            if pd.isna(record['catalog_id']) or not name or not created_at:
                raise ValueError('Missing required fields')
            cleaned.append({
                'catalog_id': record['catalog_id'],
                'name': name,
                'created_at': created_at,
                'batch_id': batch_id
            })
        except Exception as e:
            bad.append({'record_type': 'catalog', 'data': str(record), 'error': str(e)})
    return pd.DataFrame(cleaned), bad

def clean_products(df: pd.DataFrame, batch_id: int):
    cleaned = []
    bad = []
    for _, row in df.iterrows():
        record = row.to_dict()
        try:
            name = normalize_name(str(record['name']))
            updated_at = to_utc_iso(str(record['updated_at']))
            price = float(record['price'])
            if pd.isna(record['product_id']) or not name or pd.isna(record['catalog_id']) or pd.isna(price) or not updated_at:
                raise ValueError('Missing required fields')
            if price <= 0:
                raise ValueError('Price must be positive')
            cleaned.append({
                'product_id': record['product_id'],
                'name': name,
                'price': price,
                'catalog_id': record['catalog_id'],
                'updated_at': updated_at,
                'batch_id': batch_id
            })
        except Exception as e:
            bad.append({'record_type': 'product', 'data': str(record), 'error': str(e)})
    return pd.DataFrame(cleaned), bad 