# DB benchmark
This respository contains scripts for benchmarking three data storage: MongoDB, Postgres, and Solr.

# Prerequisite
This guide assume you have the following installed in your system.
- Docker
- Python3
- Pip

## Get StartedInstall all required dependencies with
```bash
pip install -r requirement.txt
```

To benchmark MongoDB:
```bash
docker run -p 27017:27017 mongo:4.0
python3 test-mongo.py
```
To re-run from fresh state, run this before re-running above commands:
```bash
docker ps # find previous created container
docker stop <container-id>
```