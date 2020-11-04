import json
from bson import json_util
import psycopg2
import time
from sample import *

QUERY_NUM = 20
LOG = "DEBUG" # DEBUG | PLAN | INFO

CREATE_TABLE = False
INSERT_DOC = False

TEST_QUERY_NAME = True
TEST_QUERY_FILE_EXTENSTION = True
TEST_QUERY_TAG = True
TEST_QUERY_DATE = True
TEST_QUERY_E = True
TEST_QUERY_CCC = True
TEST_QUERY_LENGTH = True
TEST_QUERY_C = True
TEST_QUERY_BBB = True

QUERY_PLAN_PREFIX = "EXPLAIN ANALYZE "

create_table_query = """
CREATE TABLE metadatas (
  name varchar(50) PRIMARY KEY,
  acceptRanges varchar(10),
  contentLength integer,
  contentType varchar(30),
  date timestamp,
  etag varchar(100),
  lastModified timestamp,
  amzId varchar(100),
  amzRequestId varchar(100),
  amzVersionId varchar(100),
  userMeta jsonb
);
"""
create_index_queries = """
CREATE INDEX ON metadatas(name);
CREATE INDEX ON metadatas(acceptRanges);
CREATE INDEX ON metadatas(contentLength);
CREATE INDEX ON metadatas(contentType);
CREATE INDEX ON metadatas(date);
CREATE INDEX ON metadatas(etag);
CREATE INDEX ON metadatas(lastModified);
CREATE INDEX ON metadatas(amzId);
CREATE INDEX ON metadatas(amzRequestId);
CREATE INDEX ON metadatas(amzVersionId);
CREATE INDEX ON metadatas USING GIN (userMeta);
"""
insert_query = """
INSERT INTO metadatas (
  name,
  acceptRanges,
  contentLength,
  contentType,
  date,
  etag,
  lastModified,
  amzId,
  amzRequestId,
  amzVersionId,
  userMeta
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

def print_query_plan(plan):
  if LOG not in ["DEBUG", "PLAN"]:
    return
  print(f"    query explain: {plan}")

def print_pef(start, end):
  print(f"  == TOTAL QUERY TIME: {end - start}s")
  print(f"  == AVG QUERY TIME: {(end - start) / QUERY_NUM}s")

print("Establishing connection with Postgres")
conn = psycopg2.connect(database='test-database',
       user='postgres',
       password='1234',
       host='127.0.0.1',
       sslmode='disable')
cursor = conn.cursor()
if CREATE_TABLE:
  cursor.execute(create_table_query)
  cursor.execute(create_index_queries)
print("All set, ready to test")

print("Insert 10k documents in db")
if INSERT_DOC:
  start = time.time()
  for i in range(10000):
    doc = gen_meta()
    user_metadata = doc["user-metadata"]
    params = (
      doc["name"],
      doc["accept-ranges"],
      doc["content-length"],
      doc["content-type"],
      doc["date"],
      doc["etag"],
      doc["last-modified"],
      doc["x-amz-id-2"],
      doc["x-amz-request-id"],
      doc["x-amz-version-id"],
      json.dumps(doc["user-metadata"], default=json_util.default)
    )
    cursor.execute(insert_query, params)
  end = time.time()
  print("Finish inserting 10k documents")
  print(f"  == INSERT TIME: {end - start}s")
  
print("Confirming number of rows")
query = "SELECT count(*) FROM metadatas;"
start = time.time()
cursor.execute(query)
c = cursor.fetchone()[0]
end = time.time()
print(f"Number of documents={c}")
print(f"  == COUNT TIME: {end - start}s")

print("Query prefix - name")
if TEST_QUERY_NAME:
  pass

print("Query exact - file extension")
if TEST_QUERY_FILE_EXTENSTION:
  def query(file_extension):
    return f"SELECT * FROM metadatas WHERE userMeta @> '{{\"g\": \"{file_extension}\"}}';"
  # query plan
  cursor.execute(QUERY_PLAN_PREFIX + query("xlsx"))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    file_extension = random.choice(["xlsx", "csv", "txt", "other"])
    cursor.execute(query(file_extension))
    r = cursor.fetchall()
    print(f"    found {len(r)} with {file_extension}")
  end = time.time()
  print_pef(start, end)

print("Query exact - tag")
if TEST_QUERY_TAG:
  def query(tag):
    return f"SELECT * FROM metadatas WHERE userMeta @> '{{\"h\": \"{tag}\"}}';"
  # query plan
  cursor.execute(QUERY_PLAN_PREFIX + query("A"))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    tag = random.choice(["A", "B", "C", "D", "E", "F", "G"])
    cursor.execute(query(tag))
    r = cursor.fetchall()
    print(f"    found {len(r)} with {tag}")
  end = time.time()
  print_pef(start, end)

print("Query date range - date")
if TEST_QUERY_DATE:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return f"SELECT * FROM metadatas WHERE date BETWEEN '{start_date}' and '{end_date}';"
  # query plan
  start_date, end_date = gen_test_date()
  cursor.execute(QUERY_PLAN_PREFIX + query(start_date, end_date))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    start_date, end_date = gen_test_date()
    cursor.execute(query(start_date, end_date))
    r = cursor.fetchall()
    print(f"    found {len(r)} with s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - ccc")
if TEST_QUERY_CCC:
  pass

print("query int range - content length")
if TEST_QUERY_LENGTH:
  pass

conn.commit()
cursor.close()
conn.close()
