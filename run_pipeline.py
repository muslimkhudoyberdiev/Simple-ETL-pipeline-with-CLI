import logging
from db.database import init_db, SessionLocal
from etl.extract import extract_catalogs, extract_products
from etl.transform import clean_catalogs, clean_products
from etl.load import load_catalogs, load_products

def main():
    logging.basicConfig(level=logging.INFO)
    try:
        logging.info('Initializing database...')
        init_db()
        session = SessionLocal()
        logging.info('Extracting data...')
        catalogs_df = extract_catalogs('data/catalogs.csv')
        products_df = extract_products('data/products.csv')
        logging.info('Transforming data...')
        catalogs_df = clean_catalogs(catalogs_df)
        products_df = clean_products(products_df)
        logging.info('Loading catalogs...')
        load_catalogs(catalogs_df, session)
        logging.info('Loading products...')
        load_products(products_df, session)
        session.close()
        logging.info('ETL pipeline completed successfully.')
    except Exception as e:
        logging.error(f'ETL pipeline failed: {e}')

if __name__ == '__main__':
    main() 