import typer
from db.database import init_db, SessionLocal
from etl.extract import extract_catalogs, extract_products
from etl.transform import clean_catalogs, clean_products
from etl.load import load_catalogs, load_products
from db.models import Product, Catalog
from sqlalchemy import func

app = typer.Typer()

@app.command()
def run_etl():
    """Run the ETL pipeline (extract, transform, load)."""
    init_db()
    session = SessionLocal()
    catalogs_df = clean_catalogs(extract_catalogs('data/catalogs.csv'))
    products_df = clean_products(extract_products('data/products.csv'))
    load_catalogs(catalogs_df, session)
    load_products(products_df, session)
    session.close()
    typer.echo("ETL pipeline completed.")

@app.command()
def query_products(catalog_id: int):
    """List products by catalog ID."""
    session = SessionLocal()
    products = session.query(Product).filter(Product.catalog_id == catalog_id).all()
    if not products:
        typer.echo(f"No products found for catalog {catalog_id}.")
    else:
        for p in products:
            typer.echo(f"{p.product_id}: {p.name} (${p.price}) [Updated: {p.updated_at}]")
    session.close()

@app.command()
def top_products(n: int = typer.Option(5, help="Number of top products by price")):
    """Show top N products by price."""
    session = SessionLocal()
    products = session.query(Product).order_by(Product.price.desc()).limit(n).all()
    for p in products:
        typer.echo(f"{p.product_id}: {p.name} (${p.price}) [Catalog: {p.catalog_id}]")
    session.close()

@app.command()
def catalog_counts():
    """Show product counts per catalog."""
    session = SessionLocal()
    results = (
        session.query(Catalog.catalog_id, Catalog.name, func.count(Product.product_id))
        .outerjoin(Product, Catalog.catalog_id == Product.catalog_id)
        .group_by(Catalog.catalog_id, Catalog.name)
        .all()
    )
    for c in results:
        typer.echo(f"Catalog {c[0]} ({c[1]}): {c[2]} products")
    session.close()

if __name__ == "__main__":
    app() 