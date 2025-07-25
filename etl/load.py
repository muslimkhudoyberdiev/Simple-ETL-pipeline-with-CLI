import pandas as pd
from sqlalchemy.orm import Session
from db.models import Catalog, Product, PipelineLog, BadRecord
from datetime import datetime

def load_catalogs(df: pd.DataFrame, session: Session):
    for _, row in df.iterrows():
        catalog = session.query(Catalog).get(row['catalog_id'])
        if not catalog:
            catalog = Catalog(
                catalog_id=row['catalog_id'],
                name=row['name'],
                created_at=row['created_at'],
                batch_id=row['batch_id']
            )
            session.add(catalog)
        else:
            catalog.name = row['name']
            catalog.created_at = row['created_at']
            catalog.batch_id = row['batch_id']
    session.commit()

def load_products(df: pd.DataFrame, session: Session):
    for _, row in df.iterrows():
        product = session.query(Product).get(row['product_id'])
        if not product:
            product = Product(
                product_id=row['product_id'],
                name=row['name'],
                price=row['price'],
                catalog_id=row['catalog_id'],
                updated_at=row['updated_at'],
                batch_id=row['batch_id']
            )
            session.add(product)
        else:
            product.name = row['name']
            product.price = row['price']
            product.catalog_id = row['catalog_id']
            product.updated_at = row['updated_at']
            product.batch_id = row['batch_id']
    session.commit()

def save_bad_records(bad_records, batch_id, session: Session):
    now = datetime.utcnow()
    for rec in bad_records:
        session.add(BadRecord(
            record_type=rec['record_type'],
            data=rec['data'],
            error=rec['error'],
            batch_id=batch_id,
            created_at=now
        ))
    session.commit()

def save_pipeline_log(status, message, total_catalogs, total_products, bad_records_count, session: Session):
    log = PipelineLog(
        run_at=datetime.utcnow(),
        status=status,
        message=message,
        total_catalogs=total_catalogs,
        total_products=total_products,
        bad_records=bad_records_count
    )
    session.add(log)
    session.commit() 