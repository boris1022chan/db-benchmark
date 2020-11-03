import random
import string
import sys
import time
import uuid
from datetime import datetime, timedelta
from pymongo import MongoClient, ASCENDING

QUERY_NUM = 20

def gen_datetime():
  d = random.randint(0, 1001)
  h = random.randint(0, 25)
  m = random.randint(0, 60)
  return datetime.now() - timedelta(days=d, hours=h, minutes=m)

def gen_tag():
  return str(uuid.uuid4())

def gen_str(length=10):
  letters = string.ascii_letters
  return ''.join(random.choice(letters) for i in range(length))

def gen_usermeta():
  def gen_rand(data_type):
    if data_type == 1: # string
      key = "a" * random.randint(2, 6)
      val = gen_str(length=random.randint(10, 30))
    elif data_type == 2: # int
      key = "b" * random.randint(2, 6)
      val = random.randint(0, 1000)
    else: # datetime
      key = "c" * random.randint(2, 6)
      val = gen_datetime()
    return (key, val) 
  
  meta = {
    "a": gen_str(),
    "b": gen_str(length=20),
    "c": random.randint(0, 100),
    "d": random.randint(0, 500),
    "e": gen_datetime(),
    "f": gen_datetime(),
    "g": random.choice(["xlsx", "csv", "txt", "other"]),
    "h": random.choice(["A", "B", "C", "D", "E", "F", "G"])
  }
  for i in range(10):
    if random.randint(0, 2) == 2:
      key, val = gen_rand(data_type=random.randint(1,3))
      meta[key] = val
  return meta


def gen_meta():
  date = gen_datetime()
  return {
    "accept-ranges": "bytes",
    "content-length": random.randint(1000, 60000),
    "content-type": "binary/octet-stream",
    "date": date,
    "etag": gen_tag(),
    "last-modified": date,
    "x-amz-id-2": gen_tag(),
    "x-amz-request-id": gen_tag(),
    "x-amz-version-id": gen_tag(),
    "user-metadata": gen_usermeta()
  }

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

print("Establishing connection with MongoDB")
client = MongoClient("localhost", 27017)
db = client["test-database"]
collections = db.list_collection_names()
# if len(collections) != 0:
#   print(f"Database is not empty, found collections: {collections}")
#   sys.exit()
metadatas = db.metadatas
# metadatas.create_index([("properties.key", ASCENDING), ("properties.val", ASCENDING)])
print("All set, ready to test")

# print("Insert 10k documents in db")
# start = time.time()
# for i in range(10000):
#   doc = gen_meta()
#   p_doc = process_doc(doc)
#   metadatas.insert_one(p_doc)
# end = time.time()
# print("Finish inserting 10k documents")
# print(f"== INSERT TIME: {end - start}s")

print("Confirming number of documenets")
start = time.time()
c = metadatas.count_documents({})
end = time.time()
print(f"Number of documents={c}")
print(f"== COUNT TIME: {end - start}s")

print("Query exact - file extension")
print(f"    query explain: {metadatas.find({'properties.key': 'user-meta-g', 'properties.val': 'xlsx'}).explain()}")
start = time.time()
for i in range(QUERY_NUM):
  file_extension = random.choice(["xlsx", "csv", "txt", "other"])
  cursor = metadatas.find({"properties.key": "user-meta-g", "properties.val": file_extension})
  c = 0
  for _ in cursor:
    c += 1
  print(f"    found {c} with {file_extension}")
end = time.time()
print(f"== TOTAL QUERY TIME: {end - start}s")
print(f"== TOTAL QUERY TIME: {(end - start) / QUERY_NUM}s")

print("Query exact - tag")
print(f"    query explain: {metadatas.find({'properties.key': 'user-meta-h', 'properties.val': 'A'}).explain()}")
start = time.time()
for i in range(QUERY_NUM):
  file_extension = random.choice(["A", "B", "C", "D", "E", "F", "G"])
  cursor = metadatas.find({"properties.key": "user-meta-h", "properties.val": file_extension})
  c = 0
  for _ in cursor:
    c += 1
  print(f"    found {c} with {file_extension}")
end = time.time()
print(f"== TOTAL QUERY TIME: {end - start}s")
print(f"== TOTAL QUERY TIME: {(end - start) / QUERY_NUM}s")


print("Query date range - date")

print("Query date range - e")

print("Query date range - ccc")

print("query int range - content length")

print("query int range - c")

print("query int range - bbb")



