import datetime
import json
import pysolr
import requests
import string
import time
from sample import *

base_url = 'http://localhost:8983/solr/example_core/'
search_handler = './select'

QUERY_NUM = 20
LOG = "PLAN" # DEBUG | PLAN | INFO

INSERT_DOC                    = True

TEST_QUERY_NAME               = True
TEST_QUERY_FILE_EXTENSTION    = True
TEST_QUERY_TAG                = True
TEST_QUERY_DATE               = True
TEST_QUERY_E                  = True
TEST_QUERY_CCC                = True
TEST_QUERY_LENGTH             = True
TEST_QUERY_C                  = True
TEST_QUERY_BBB                = True

def search(query):
  res = requests.get(f"{base_url}{search_handler}?q={query}")
  return json.loads(res.text)

def process_doc(doc):
  ret = {}
  for k, v in doc.items():
    if k != "user-metadata":
      if isinstance(v, datetime):
        ret[k] = v.isoformat()
      else:
        ret[k] = v
    else:
      for u_k, u_v in v.items():
        if isinstance(u_v, datetime):
          ret[f"user-meta-{u_k}"] = u_v.isoformat()
        else:
          ret[f"user-meta-{u_k}"] = u_v
  return ret

def print_count(cnt, info):
  if LOG != "DEBUG":
    return
  print(f"    found {cnt} with {info}")

def print_pef(start, end):
  print(f"  == TOTAL QUERY TIME: {end - start}s")
  print(f"  == AVG QUERY TIME: {(end - start) / QUERY_NUM}s")

print("Establishing connection with Solr")
solr = pysolr.Solr(base_url, search_handler='/select', always_commit=True)
print("All set, ready to test")

print("Insert 10k documents in solr")
if INSERT_DOC:
  start = time.time()
  for i in range(10000):
    doc = gen_meta()
    p_doc = process_doc(doc)
    solr.add(p_doc)
  end = time.time()
  print("Finish inserting 10k documents")
  print(f"  == INSERT TIME: {end - start}s")

print("Confirming number of documents")
start = time.time()
res = search("*:*")
c = res["response"]["numFound"]
end = time.time()
print(f"Number of documents={c}")
print(f"  == COUNT TIME: {end - start}s")

print("Query prefix - name")
if TEST_QUERY_NAME:
  def query(name_prefix):
    return f'name:"{name_prefix}*"'
  res = search(query("aaaaa"))
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    prefix = random.choice(string.ascii_lowercase) * 5
    res = search(query(prefix))
    c = res["response"]["numFound"]
    print_count(c, prefix)
  end = time.time()
  print_pef(start, end)

print("Query exact - file extension")
if TEST_QUERY_FILE_EXTENSTION:
  def query(file_extension):
    return f'user-meta-g:"{file_extension}*"'
  res = search(query("csv"))
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    file_extension = random.choice(["xlsx", "csv", "txt", "other"])
    res = search(query(file_extension))
    c = res["response"]["numFound"]
    print_count(c, file_extension)
  end = time.time()
  print_pef(start, end)

print("Query exact - tag")
if TEST_QUERY_TAG:
  def query(tag):
    return f'user-meta-h:"{tag}*"'
  res = search(query("A"))
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    tag = random.choice(["A", "B", "C", "D", "E", "F", "G"])
    res = search(query(tag))
    c = res["response"]["numFound"]
    print_count(c, tag)
  end = time.time()
  print_pef(start, end)

print("Query date range - date")
if TEST_QUERY_DATE:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return f'date:["{start_date.isoformat()}Z" TO "{end_date.isoformat()}Z"]'
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    start_date, end_date = gen_test_date()
    res = search(query(start_date, end_date))
    c = res["response"]["numFound"]
    print_count(c, f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - e")
if TEST_QUERY_E:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return f'user-meta-e:["{start_date.isoformat()}Z" TO "{end_date.isoformat()}Z"]'
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    start_date, end_date = gen_test_date()
    res = search(query(start_date, end_date))
    c = res["response"]["numFound"]
    print_count(c, f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("Query date range - ccc")
if TEST_QUERY_CCC:
  def gen_test_date():
    start_date = gen_datetime()
    return (start_date, start_date + timedelta(days=20))
  def query(start_date, end_date):
    return f'user-meta-ccc:["{start_date.isoformat()}Z" TO "{end_date.isoformat()}Z"]'
  # benchmark
  start = time.time()
  for _ in range(QUERY_NUM):
    start_date, end_date = gen_test_date()
    res = search(query(start_date, end_date))
    c = res["response"]["numFound"]
    print_count(c, f"s:{start_date}, e:{end_date}")
  end = time.time()
  print_pef(start, end)

print("query int range - content length")
if TEST_QUERY_LENGTH:
  def gen_bound():
    start_int = random.randint(1000, 9000)
    return (start_int, start_int + 500) 
  def query(start_int, end_int):
    return f'content-length:[{start_int} TO {end_int}]'
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    start_int, end_int = gen_bound()
    res = search(query(start_int, end_int))
    c = res["response"]["numFound"]
    print_count(c, f"s: {start_int}, e: {end_int}")    
  end = time.time()
  print_pef(start, end)

print("Query int range - c")
if TEST_QUERY_C:
  def gen_bound():
    lower = random.randint(0, 100)
    return (lower, lower + 3)
  def query(start_int, end_int):
    return f'user-meta-c:[{start_int} TO {end_int}]'
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    start_int, end_int = gen_bound()
    res = search(query(start_int, end_int))
    c = res["response"]["numFound"]
    print_count(c, f"s: {start_int}, e: {end_int}")    
  end = time.time()
  print_pef(start, end)

print("Query int range - bbb")
if TEST_QUERY_BBB:
  def gen_bound():
    lower = random.randint(0, 1000)
    return (lower, lower + 10)
  def query(start_int, end_int):
    return f'user-meta-bbb:[{start_int} TO {end_int}]'
  # benchmark
  start = time.time()
  for i in range(QUERY_NUM):
    start_int, end_int = gen_bound()
    res = search(query(start_int, end_int))
    c = res["response"]["numFound"]
    print_count(c, f"s: {start_int}, e: {end_int}")    
  end = time.time()
  print_pef(start, end)
