import random
import string
import uuid
from datetime import datetime, timedelta

def gen_name():
  prefix_letter = random.choice(string.ascii_lowercase)
  id = str(uuid.uuid4())
  return f"{prefix_letter * 5}-{id}"

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
    "name": gen_name(),
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