import typer
from db.database import init_db, SessionLocal
from etl.extract import extract_catalogs, extract_products
from etl.transform import clean_catalogs, clean_products
from etl.load import load_catalogs, load_products, save_bad_records, save_pipeline_log
from db.models import Product, Catalog, PipelineLog, BadRecord
from sqlalchemy import func
from datetime import datetime

app = typer.Typer()

def get_next_batch_id(session):
    max_catalog = session.query(func.max(Catalog.batch_id)).scalar() or 0
    max_product = session.query(func.max(Product.batch_id)).scalar() or 0
    return max(max_catalog, max_product) + 1

@app.command()
def run_etl():
    print("Running ETL pipeline...")
    init_db()
    session = SessionLocal()
    batch_id = get_next_batch_id(session)
    print(f"Starting ETL run with batch_id={batch_id}...")
    try:
        catalogs_df_raw = extract_catalogs('data/catalogs.csv')
        products_df_raw = extract_products('data/products.csv')
        catalogs_df, bad_catalogs = clean_catalogs(catalogs_df_raw, batch_id)
        products_df, bad_products = clean_products(products_df_raw, batch_id)
        load_catalogs(catalogs_df, session)
        load_products(products_df, session)
        save_bad_records(bad_catalogs + bad_products, batch_id, session)
        save_pipeline_log(
            status='success',
            message='ETL completed',
            total_catalogs=len(catalogs_df),
            total_products=len(products_df),
            bad_records_count=len(bad_catalogs) + len(bad_products),
            session=session
        )
        print(f"ETL completed. Catalogs: {len(catalogs_df)}, Products: {len(products_df)}, Bad records: {len(bad_catalogs) + len(bad_products)}")
    except Exception as e:
        save_pipeline_log(
            status='failure',
            message=str(e),
            total_catalogs=0,
            total_products=0,
            bad_records_count=0,
            session=session
        )
        print(f"ETL failed: {e}")
    finally:
        session.close()

@app.command()
def query_products(catalog_id: int):
    print(f"Querying products for catalog {catalog_id}...")
    session = SessionLocal()
    products = session.query(Product).filter(Product.catalog_id == catalog_id).all()
    if not products:
        print(f"No products found for catalog {catalog_id}.")
    else:
        for p in products:
            print(f"{p.product_id}: {p.name} (${p.price}) [Updated: {p.updated_at}] (batch {p.batch_id})")
    session.close()

@app.command()
def top_products(n: int = typer.Option(5, help="Number of top products by price")):
    print(f"Showing top {n} products by price...")
    session = SessionLocal()
    products = session.query(Product).order_by(Product.price.desc()).limit(n).all()
    for p in products:
        print(f"{p.product_id}: {p.name} (${p.price}) [Catalog: {p.catalog_id}] (batch {p.batch_id})")
    session.close()

@app.command()
def catalog_counts():
    print("Counting products per catalog...")
    session = SessionLocal()
    results = (
        session.query(Catalog.catalog_id, Catalog.name, func.count(Product.product_id))
        .outerjoin(Product, Catalog.catalog_id == Product.catalog_id)
        .group_by(Catalog.catalog_id, Catalog.name)
        .all()
    )
    for c in results:
        print(f"Catalog {c[0]} ({c[1]}): {c[2]} products")
    session.close()

@app.command()
def show_logs(n: int = typer.Option(10, help="Number of recent pipeline logs to show")):
    print(f"Showing last {n} pipeline logs...")
    session = SessionLocal()
    logs = session.query(PipelineLog).order_by(PipelineLog.run_at.desc()).limit(n).all()
    for log in logs:
        print(f"[{log.run_at}] Status: {log.status} | Catalogs: {log.total_catalogs} | Products: {log.total_products} | Bad: {log.bad_records} | Message: {log.message}")
    session.close()

@app.command()
def show_bad_records(batch_id: int = typer.Option(None, help="Filter by batch_id (optional)")):
    print(f"Showing bad records{' for batch ' + str(batch_id) if batch_id else ''}...")
    session = SessionLocal()
    query = session.query(BadRecord)
    if batch_id is not None:
        query = query.filter(BadRecord.batch_id == batch_id)
    bads = query.order_by(BadRecord.created_at.desc()).all()
    for b in bads:
        print(f"[{b.created_at}] Batch: {b.batch_id} | Type: {b.record_type} | Error: {b.error} | Data: {b.data}")
    session.close()

if __name__ == "__main__":
    app() 