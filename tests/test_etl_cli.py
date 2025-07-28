import pytest
from typer.testing import CliRunner
from cli import app
from db.database import init_db, SessionLocal
from db.models import PipelineLog, BadRecord, Product, Catalog

runner = CliRunner()

def setup_module(module):
    # Re-initialize the database for testing
    init_db()
    session = SessionLocal()
    session.query(PipelineLog).delete()
    session.query(BadRecord).delete()
    session.query(Product).delete()
    session.query(Catalog).delete()
    session.commit()
    session.close()

def test_run_etl():
    result = runner.invoke(app, ['run-etl'])
    assert result.exit_code == 0
    assert "ETL completed" in result.output or "ETL failed" in result.output
    session = SessionLocal()
    log = session.query(PipelineLog).order_by(PipelineLog.run_at.desc()).first()
    assert log is not None
    assert log.status in ('success', 'failure')
    session.close()

def test_show_logs():
    result = runner.invoke(app, ['show-logs', '--n', '1'])
    assert result.exit_code == 0
    assert "Status:" in result.output

def test_show_bad_records():
    result = runner.invoke(app, ['show-bad-records'])
    assert result.exit_code == 0

def test_etl_creates_batch_id():
    session = SessionLocal()
    product = session.query(Product).first()
    catalog = session.query(Catalog).first()
    assert product is not None and product.batch_id is not None
    assert catalog is not None and catalog.batch_id is not None
    session.close() 