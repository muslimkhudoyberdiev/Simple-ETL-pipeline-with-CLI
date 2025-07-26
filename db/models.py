from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class Catalog(Base):
    __tablename__ = 'catalogs'
    catalog_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    batch_id = Column(Integer, nullable=False, default=1)
    products = relationship('Product', back_populates='catalog')

    def __repr__(self):
        return f"<Catalog(id={self.catalog_id}, name='{self.name}', batch_id={self.batch_id})>"

class Product(Base):
    __tablename__ = 'products'
    product_id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    price = Column(Float, nullable=False)
    catalog_id = Column(Integer, ForeignKey('catalogs.catalog_id'), nullable=False)
    updated_at = Column(DateTime, nullable=False)
    batch_id = Column(Integer, nullable=False, default=1)
    catalog = relationship('Catalog', back_populates='products')

    def __repr__(self):
        return f"<Product(id={self.product_id}, name='{self.name}', price={self.price}, batch_id={self.batch_id})>"

class PipelineLog(Base):
    __tablename__ = 'pipeline_logs'
    id = Column(Integer, primary_key=True)
    run_at = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)
    message = Column(Text)
    total_catalogs = Column(Integer)
    total_products = Column(Integer)
    bad_records = Column(Integer)

class BadRecord(Base):
    __tablename__ = 'bad_records'
    id = Column(Integer, primary_key=True)
    record_type = Column(String, nullable=False) 
    data = Column(Text, nullable=False)
    error = Column(Text, nullable=False)
    batch_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False) 