# DB benchmark
This respository contains reproducible script for benchmarking data storage for metadata-based cloud object search project. Benchmarked data store include MongoDB, Postgres, and Apache Solr. These scripts aim to look at insert time and specific querying speed.

## Get Started
This guide assume you have the following installed in your system.
- Docker
- Python3
- Pip

Install all required dependencies with
```bash
pip3 install -r requirement.txt
```

### MongoDB
To benchmark MongoDB:
```bash
docker run -d -p 27017:27017 --name test-mongo mongo:4.0
python3 scripts/mongo.py
```
To re-run from fresh state, run the following commands before re-running the above commands:
```bash
docker stop test-mongo
docker rm test-mongo
```

### PostgreSQL
To benchmark PostgreSQL:
```bash
docker run -d \
    --name test-postgres \
    -p 5432:5432 \
    -h 0.0.0.0 \
    -e POSTGRES_PASSWORD=1234 \
    -e POSTGRES_DB=test-database \
    -e PGDATA=/var/lib/postgresql/data/pgdata \
    postgres:12
# check if Postgres is running
docker exec -it test-postgres bash
~ psql -U postgres
~~ \list                    # show all databases
~~ \connect test-database   # connect to test database
~~ \dt                      # show tables
~~ \d <table>               # show table schema
# run test script after confirming Postgres is running
python3 scripts/postgres.py
```

### Apache Solr
To benchmark Solr:
```bash
# This runs a single instance of Solr core instead of Solr cloud
# May need to wait a few seconds for core to be created
docker run -d -p 8983:8983 --name test-solr solr:8.6 solr-precreate example_core
# Make sure core is created before running benchmark script.
# To confirm core is running, visit http://localhost:8983/solr
python3 scripts/solr.py
```
One can copy a new `managed-schema` file to override Solr schema by doing (note that `./managed-schema` is not included in this repository):
```bash
docker cp test-solr:/opt/solr/server/solr/configsets/_default/conf/managed-schema ./managed-schema
```

### Note
For more fine-grain control of which test to run, open `scripts/*.py` and toggle the global variables before running scripts.

### Cleanup
To clean up properly, do:
```bash
docker system df
docker volume prune # re-claim space
```

## Result
The sample result is run on the ASUS Zenbook UX430UA with CPU i5-7200U @ 2.5GHz (2 core 4 thread) and 8GB of ram. Sample result can be found in `outputs/` folder. We have also recorded the following CPU and memory usage:
|          | max CPU | avg CPU | max MEM |
|----------|---------|---------|---------|
| MongoDB  | 28.77%  | ~25%    | 117.2MB |
| Postgres | 73.14%  | ~30%    | 17.2MB  |
| Solr     | 245.79% | ~80%    | 941.6MB |
