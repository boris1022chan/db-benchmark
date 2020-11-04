import json
import psycopg2
import random
import string
import time
from bson import json_util
from sample import *

QUERY_NUM = 20
LOG = "PLAN" # DEBUG | PLAN | INFO

CREATE_TABLE  = False
INSERT_DOC    = False

TEST_QUERY_NAME               = True
TEST_QUERY_FILE_EXTENSTION    = True
TEST_QUERY_TAG                = True
TEST_QUERY_DATE               = True
TEST_QUERY_E                  = True
TEST_QUERY_CCC                = True
TEST_QUERY_LENGTH             = True
TEST_QUERY_C                  = True
TEST_QUERY_BBB                = True

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

def print_count(cnt, info):
  if LOG != "DEBUG":
    return
  print(f"    found {cnt} with {info}")

def print_query_plan(plan):
  if LOG not in ["DEBUG", "PLAN"]:
    return
  print(f"    query explain: {plan}")

def print_pef(start, end):
  print(f"  == TOTAL QUERY TIME: {end - start}s")
  print(f"  == AVG QUERY TIME: {(end - start) / QUERY_NUM}s")

def process_doc(user_metadata):
  ret = {}
  for k, v in user_metadata.items():
    if isinstance(v, datetime):
      ret[k] = v.isoformat()
    else:
      ret[k] = v
  return ret

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
    user_metadata = process_doc(doc["user-metadata"])
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
      json.dumps(user_metadata)
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
  def query(name_prefix):
    return f"SELECT * from metadatas where name like '{name_prefix}%'"
  # query
  cursor.execute(QUERY_PLAN_PREFIX + query("aaaaa"))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    prefix = random.choice(string.ascii_lowercase) * 5
    cursor.execute(query(prefix))
    r = cursor.fetchall()
    print_count(len(r), prefix)
  end = time.time()
  print_pef(start, end)
  # https://niallburkley.com/blog/index-columns-for-like-in-postgres/

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
    print_count(len(r), file_extension)
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
    print_count(len(r), tag)
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
    print_count(len(r), f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - e")
if TEST_QUERY_E:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return ("SELECT * FROM metadatas WHERE "
      f"(usermeta->>'e')::timestamp >= '{start_date.isoformat()}'::timestamp "
      f"AND (usermeta->>'e')::timestamp >= '{end_date.isoformat()}'::timestamp;")
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
    print_count(len(r), f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - ccc")
if TEST_QUERY_CCC:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return ("SELECT * FROM metadatas WHERE "
      f"(usermeta->>'ccc')::timestamp >= '{start_date.isoformat()}'::timestamp "
      f"AND (usermeta->>'ccc')::timestamp >= '{end_date.isoformat()}'::timestamp;")
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
    print_count(len(r), f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("query int range - content length")
if TEST_QUERY_LENGTH:
  def gen_bound():
    lower = random.randint(1000, 30000)
    return (lower, lower + 100)
  def query(lower, upper):
    return f"SELECT contentlength FROM metadatas WHERE contentlength BETWEEN {lower} AND {upper};"
  # query plan
  lower, upper = gen_bound()
  cursor.execute(QUERY_PLAN_PREFIX + query(lower, upper))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    lower, upper = gen_bound()
    cursor.execute(query(lower, upper))
    r = cursor.fetchall()
    print_count(len(r), f"l:{lower}, u:{upper}")
  end = time.time()
  print_pef(start, end)

print("Query int range - c")
if TEST_QUERY_C:
  def gen_bound():
    lower = random.randint(0, 100)
    return (lower, lower + 3)
  def query(lower, upper):
    return f"SELECT * FROM metadatas WHERE (userMeta->>'c')::int <= {upper} AND (userMeta->>'c')::int >= {lower};"
  # query plan
  lower, upper = gen_bound()
  cursor.execute(QUERY_PLAN_PREFIX + query(lower, upper))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    lower, upper = gen_bound()
    cursor.execute(query(lower, upper))
    r = cursor.fetchall()
    print_count(len(r), f"l:{lower}, u:{upper}")
  end = time.time()
  print_pef(start, end)

print("Query int range - bbb")
if TEST_QUERY_BBB:
  def gen_bound():
    lower = random.randint(0, 1000)
    return (lower, lower + 10)
  def query(lower, upper):
    return f"SELECT * FROM metadatas WHERE (userMeta->>'bbb')::int <= {upper} AND (userMeta->>'bbb')::int >= {lower};"
  # query plan
  lower, upper = gen_bound()
  cursor.execute(QUERY_PLAN_PREFIX + query(lower, upper))
  r = cursor.fetchone()[0]
  print_query_plan(r)
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    lower, upper = gen_bound()
    cursor.execute(query(lower, upper))
    r = cursor.fetchall()
    print_count(len(r), f"l:{lower}, u:{upper}")
  end = time.time()
  print_pef(start, end)

conn.commit()
cursor.close()
conn.close()
