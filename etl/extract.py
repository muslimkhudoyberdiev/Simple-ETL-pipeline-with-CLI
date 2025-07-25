import pandas as pd
from pathlib import Path

def extract_catalogs(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path)

def extract_products(file_path: str) -> pd.DataFrame:
    return pd.read_csv(file_path) 