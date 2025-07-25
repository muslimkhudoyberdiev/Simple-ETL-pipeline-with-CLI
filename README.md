# Data Ingestion Sample - CLI Only

## Quickstart with Docker (CLI)

1. Build and start the app and PostgreSQL:

```sh
docker-compose up --build
```

2. Open a shell in the app container:

```sh
docker-compose exec app bash
```

3. Run the ETL pipeline:

```sh
python cli.py run-etl
```

4. Query products by catalog:

```sh
python cli.py query-products --catalog-id 1
```

5. Show top N products by price:

```sh
python cli.py top-products --n 3
```

6. Show product counts per catalog:

```sh
python cli.py catalog-counts
```

7. Run tests:

```sh
pytest
```

---

## Notes
- Data in PostgreSQL is persisted in the `pgdata` Docker volume.
- Environment variables are set in `docker-compose.yml`.
- You can modify the sample data in `data/` and rerun the ETL pipeline. 