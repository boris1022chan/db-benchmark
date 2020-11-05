import sys
import random
import time
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING
from sample import *

QUERY_NUM = 20
LOG = "PLAN" # DEBUG | PLAN | INFO

INSERT_DOC                    = False

TEST_QUERY_NAME               = True
TEST_QUERY_FILE_EXTENSTION    = True
TEST_QUERY_TAG                = True
TEST_QUERY_DATE               = True
TEST_QUERY_E                  = True
TEST_QUERY_CCC                = True
TEST_QUERY_LENGTH             = True
TEST_QUERY_C                  = True
TEST_QUERY_BBB                = True

def process_doc(doc):
  ret = []
  for k, v in doc.items():
    if k != "user-metadata":
      ret.append({
        "key": k,
        "val": v
      })
    else:
      for u_k, u_v in v.items():
        ret.append({
          "key": f"user-meta-{u_k}",
          "val": u_v
        })
  return {"properties": ret}

def print_query_plan(plan, pretty=True):
  if LOG not in ["DEBUG", "PLAN"]:
    return
  if pretty:
    method = plan["executionStats"]["executionStages"]["inputStage"]["stage"]
    num_doc = plan["executionStats"]["executionStages"]["docsExamined"]
    print(f"    query explain: {method}, doc_examine={num_doc}")
  else:
    print(f"    query explain: {plan}")

def print_count(cursor, info):
  c = 0
  for _ in cursor:
    c += 1
  if LOG   == "DEBUG":
    print(f"    found {c} with {info}")

def print_pef(start, end):
  print(f"  == TOTAL QUERY TIME: {end - start}s")
  print(f"  == AVG QUERY TIME: {(end - start) / QUERY_NUM}s")

print("Establishing connection with MongoDB")
client = MongoClient("localhost", 27017)
db = client["test-database"]
collections = db.list_collection_names()
if INSERT_DOC and len(collections) != 0:
  print(f"Database is not empty, found collections: {collections}")
  print(f"You should properly clean up docker file for new tests")
  sys.exit()
metadatas = db.metadatas
metadatas.create_index([("properties.key", ASCENDING), ("properties.val", ASCENDING)])
print("All set, ready to test")

print("Insert 10k documents in db")
if INSERT_DOC: 
  start = time.time()
  for i in range(10000):
    doc = gen_meta()
    p_doc = process_doc(doc)
    metadatas.insert_one(p_doc)
  end = time.time()
  print("Finish inserting 10k documents")
  print(f"  == INSERT TIME: {end - start}s")

print("Confirming number of documenets")
start = time.time()
c = metadatas.count_documents({})
end = time.time()
print(f"Number of documents={c}")
print(f"  == COUNT TIME: {end - start}s")

print("Query prefix - name")
if TEST_QUERY_NAME:
  def query(name_prefix):
    return {
      "properties": {
        "$elemMatch": {
          "key": "name",
          "val": { "$regex": f"^{name_prefix}" }
        }
      }
    }
  # query plan
  r = metadatas.find(query("aaaaa")).explain()
  print_query_plan(r)
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    name_prefix = random.choice(string.ascii_lowercase) * 5
    cursor = metadatas.find(query(name_prefix))
    print_count(cursor, f"{name_prefix}")
  end = time.time()
  print_pef(start, end)

print("Query exact - file extension")
if TEST_QUERY_FILE_EXTENSTION: 
  def query(file_extension):
    return {
      "properties": {
        "$elemMatch": {
          "key": "user-meta-g",
          "val": file_extension
        }
      }
    }
  # query plan
  r = metadatas.find(query("xlsx")).explain()
  print_query_plan(r)
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    file_extension = random.choice(["xlsx", "csv", "txt", "other"])
    cursor = metadatas.find(query(file_extension))
    print_count(cursor, f"{file_extension}")
  end = time.time()
  print_pef(start, end)

print("Query exact - tag")
if TEST_QUERY_TAG: 
  def query(tag):
    return {
      "properties": {
        "$elemMatch": {
          "key": "user-meta-h",
          "val": tag
        }
      }
    }
  # query plan
  r = metadatas.find(query("A")).explain()
  print_query_plan(r)
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    tag = random.choice(["A", "B", "C", "D", "E", "F", "G"])
    cursor = metadatas.find(query(tag))
    print_count(cursor, f"{tag}")
  end = time.time()
  print_pef(start, end)

print("Query date range - date")
if TEST_QUERY_DATE: 
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return {
      "properties": {
        "$elemMatch": {
          "key": "date",
          "val": {
            "$gte": start_date,
            "$lte": end_date
          }
        }
      }
    }
  # query play
  start_date, end_date = gen_test_date()
  r = metadatas.find(query(start_date, end_date)).explain()
  print_query_plan(r)
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    start_date, end_date = gen_test_date()
    cursor = metadatas.find(query(start_date, end_date))
    print_count(cursor, f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - e")
if TEST_QUERY_E:
  print("  == SKIP: similar to date")

print("Query date range - ccc")
if TEST_QUERY_CCC:
  print("  == SKIP: similar to date")

print("Query int range - content length")
if TEST_QUERY_LENGTH:
  def gen_bound():
    start_int = random.randint(1000, 9000)
    return (start_int, start_int + 500)
  def query(start_int, end_int):
    return {
      "properties": {
        "$elemMatch": {
          "key": "content-length",
          "val": {
            "$gte": start_int,
            "$lte": end_int
          }
        }
      }
    }
  # query play
  start_int, end_int = gen_bound()
  r = metadatas.find(query(start_int, end_int)).explain()
  print_query_plan(r)
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    start_int, end_int = gen_bound()
    cursor = metadatas.find(query(start_int, end_int))
    print_count(cursor, f"s: {start_int}, e: {end_int}")    
  end = time.time()
  print_pef(start, end)

print("Query int range - c")
if TEST_QUERY_C:
  print("  == SKIP: similar to content-length")
  pass

print("Query int range - bbb")
if TEST_QUERY_BBB:
  print("  == SKIP: similar to content-length")



