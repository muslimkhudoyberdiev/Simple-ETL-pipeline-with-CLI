import pandas as pd
from datetime import datetime
import pytz

def normalize_name(name: str) -> str:
    return ' '.join(name.strip().title().split())

def to_utc_iso(dt_str: str) -> datetime:
    try:
        dt = pd.to_datetime(dt_str, utc=True)
        return dt.to_pydatetime()
    except Exception:
        return None

def clean_catalogs(df: pd.DataFrame, batch_id: int):
    cleaned = []
    bad = []
    seen_ids = set()
    for _, row in df.iterrows():
        record = row.to_dict()
        try:
            catalog_id = record.get('catalog_id')
            if pd.isna(catalog_id) or catalog_id == '':
                raise ValueError('Null or missing catalog_id')
            if catalog_id in seen_ids:
                raise ValueError('Duplicate catalog_id')
            seen_ids.add(catalog_id)
            name = normalize_name(str(record['name']))
            created_at = to_utc_iso(str(record['created_at']))
            if not name or not created_at:
                raise ValueError('Missing required fields or malformed date')
            cleaned.append({
                'catalog_id': int(catalog_id),
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
    seen_ids = set()
    for _, row in df.iterrows():
        record = row.to_dict()
        try:
            product_id = record.get('product_id')
            if pd.isna(product_id) or product_id == '':
                raise ValueError('Null or missing product_id')
            if product_id in seen_ids:
                raise ValueError('Duplicate product_id')
            seen_ids.add(product_id)
            name = normalize_name(str(record['name']))
            updated_at = to_utc_iso(str(record['updated_at']))
            price = record.get('price')
            catalog_id = record.get('catalog_id')
            if pd.isna(price) or price == '':
                raise ValueError('Missing price')
            price = float(price)
            if pd.isna(catalog_id) or catalog_id == '':
                raise ValueError('Missing catalog_id')
            if not name or not updated_at:
                raise ValueError('Missing required fields or malformed date')
            if price <= 0:
                raise ValueError('Price must be positive')
            cleaned.append({
                'product_id': int(product_id),
                'name': name,
                'price': price,
                'catalog_id': int(catalog_id),
                'updated_at': updated_at,
                'batch_id': batch_id
            })
        except Exception as e:
            bad.append({'record_type': 'product', 'data': str(record), 'error': str(e)})
    return pd.DataFrame(cleaned), bad 