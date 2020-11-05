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
pip3 install -r requirement.txt
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
To benchmark Solr:
```bash
# This runs single instance of Solr core instead of Solr cloud
# May need to wait a few second for core to be created
docker run -d -p 8983:8983 --name test-solr solr:8.6 solr-precreate example_core
# Make sure core is created before running next command by visiting http://localhost:8983/solr
python3 test-solr.py
```

override schema: https://stackoverflow.com/questions/60659470/add-field-to-solr-when-running-in-docker

## Result
The sample result is run on the ASUS Zenbook UX430UA with CPU i5-7200U @ 2.5GHz (2 core 4 thread) and 8GB of ram. Sample result can be found in `outputs/` folder. We have also recorded the following CPU and memory usage:
|          | max CPU | avg CPU | max MEM |
|----------|---------|---------|---------|
| MongoDB  | 28.77%  | ~25%    | 117.2MB |
| Postgres | 73.14%  | ~30%    | 17.2MB  |
| Solr     | 245.79% | ~80%    | 941.6MB |
