# DB benchmark
This respository contains scripts for benchmarking three data storage: MongoDB, Postgres, and Solr.

## Prerequisite
This guide assume you have the following installed in your system.
- Docker
- Python3
- Pip

## Get Started
Install all required dependencies with
```bash
pip install -r requirement.txt
```

### MongoDB
To benchmark MongoDB:
```bash
docker run -d -p 27017:27017 --name test-mongo mongo:4.0
python3 test-mongo.py
```
For more fine-grain control of which test to run, open `test-mongo.py` and toggle the global variables.

To re-run from fresh state, run this before re-running above commands:
```bash
docker stop test-mongo
docker rm test-mongo
```
Clean up properly by:
```bash
docker system df
docker volume prune # re-claim space
```

### Postgres
To benchmark Postgres:
```bash
docker run -d \
    --name test-postgres \
    -p 5432:5432 \
    -h 0.0.0.0 \
    -e POSTGRES_PASSWORD=1234 \
    -e POSTGRES_DB=test-database \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    postgres:12
python3 test-postgres.py
# check if Postgres is running
docker exec -it test-postgres bash
~ psql -U postgres
~~ \list                    # show all databases
~~ \connect test-database   # connect to test database
~~ \dt                      # show tables
~~ \d <table>               # show table schema
```

### Apache Solr
