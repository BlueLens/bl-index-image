import numpy as np
import time
import faiss
import signal
from multiprocessing import Process

import uuid
import logging
import json
import redis
import os
import stylelens_index
from bluelens_spawning_pool import spawning_pool

STR_BUCKET = "bucket"
STR_STORAGE = "storage"
STR_CLASS_CODE = "class_code"
STR_NAME = "name"
STR_FORMAT = "format"

AWS_ACCESS_KEY = os.environ['AWS_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = os.environ['AWS_SECRET_ACCESS_KEY']

REDIS_SERVER = os.environ['REDIS_SERVER']
SUBSCRIBE_TOPIC = os.environ['SUBSCRIBE_TOPIC']
DATA_SOURCE = os.environ['DATA_SOURCE']
DATA_SOURCE_QUEUE = 'REDIS_QUEUE'
DATA_SOURCE_DB = 'DB'

REDIS_IMAGE_FEATURE_QUEUE = 'bl:image:feature:queue'
REDIS_IMAGE_HASH = 'bl:image:hash'
REDIS_IMAGE_LIST = 'bl:image:list'

logging.basicConfig(filename='./log/main.log', level=logging.DEBUG)
rconn = redis.StrictRedis(REDIS_SERVER, port=6379)

g_images = []


def spawn_indexer(uuid):

  time.sleep(60)
  pool = spawning_pool.SpawningPool()

  project_name = 'bl-image-indexer-' + uuid
  print('spawn_indexer: ' + project_name)

  pool.setServerUrl('bl-mem-store-master')
  # pool.setServerUrl('35.187.244.252')
  pool.setApiVersion('v1')
  pool.setKind('Pod')
  pool.setMetadataName(project_name)
  pool.setMetadataNamespace('index')
  pool.addMetadataLabel('name', project_name)
  pool.addMetadataLabel('SPAWN_ID', uuid)
  container = pool.createContainer()
  pool.setContainerName(container, project_name)
  pool.addContainerEnv(container, 'AWS_ACCESS_KEY', AWS_ACCESS_KEY)
  pool.addContainerEnv(container, 'AWS_SECRET_ACCESS_KEY', AWS_SECRET_ACCESS_KEY)
  pool.addContainerEnv(container, 'REDIS_SERVER', REDIS_SERVER)
  pool.addContainerEnv(container, 'SPAWN_ID', uuid)
  pool.setContainerImage(container, 'bluelens/bl-image-indexer:latest')
  pool.addContainer(container)
  pool.setRestartPolicy('Never')
  pool.spawn()

def start_index():
  if DATA_SOURCE == DATA_SOURCE_QUEUE:
    load_from_queue()
  elif DATA_SOURCE == DATA_SOURCE_DB:
    load_from_db()

def load_from_queue():
  print('load_from_queue')
  VECTOR_SIZE = 2048
  index = faiss.IndexFlatL2(VECTOR_SIZE)
  index2 = faiss.IndexIDMap(index)

  def items():
    while True:
      yield rconn.blpop([REDIS_IMAGE_FEATURE_QUEUE])

  def request_stop(signum, frame):
    print('stopping')
    rconn.connection_pool.disconnect()
    print('connection closed')

  signal.signal(signal.SIGINT, request_stop)
  signal.signal(signal.SIGTERM, request_stop)


  i = 0
  for item in items():
    key, image_data = item
    if type(image_data) is str:
      image_info = json.loads(image_data)
    elif type(image_data) is bytes:
      image_info = json.loads(image_data.decode('utf-8'))

    # print(image_info)
    logging.debug('save_index')
    feature = image_info['feature']
    xb = np.expand_dims(np.array(feature, dtype=np.float32), axis=0)
    image_info['feature'] = None
    rconn.lpush(REDIS_IMAGE_LIST, image_info['name'])
    rconn.lpush(REDIS_IMAGE_HASH, json.dumps(image_info))

    # xb = np.array(features)
    id_num = rconn.llen(REDIS_IMAGE_LIST)
    id_array = []
    id_array.append(id_num)
    id_set = np.array(id_array)
    logging.debug(xb.shape)
    # print(xb)
    # print(np.shape(xb))
    # print(id_set)
    print('-----')
    print(xb.shape)
    print(id_set.shape)
    index2.add_with_ids(xb, id_set)
    faiss.write_index(index, 'faiss.index')
    i = i + 1
    print('index done')

    # ToDo:
    # save_to_db()

def load_from_db():
  print('load_from_db')
  # Need to implement

def save_to_db():
  print('save_to_db')

def index(image_info):
  print('index')
  logging.debug('save_index')
  feature = image_info['feature']
  xb = np.expand_dims(np.array(feature, dtype=np.float32), axis=0)
  image_info['feature'] = None
  rconn.lpush(REDIS_IMAGE_LIST, image_info['name'])
  rconn.lpush(REDIS_IMAGE_HASH, json.dumps(image_info))

  # xb = np.array(features)
  id_num = rconn.llen(REDIS_IMAGE_LIST)
  id_array = []
  id_array.append(id_num)
  id_set = np.array(id_array)
  logging.debug(xb.shape)
  print(xb)
  print(np.shape(xb))
  print(id_set)
  print('-----')
  print(xb.shape)
  print(id_set.shape)
  index2.add_with_ids(xb, id_set)
  print('index done')
  # faiss.write_index(index, 'faiss.index')

def save_index():
  print('save_index')
  logging.debug('save_index')
  features = []
  for image in g_images:
    features.append(image['feature'])
    rconn.lpush(REDIS_IMAGE_LIST, image)

  xb = np.array(features)
  id_num = len(features)
  id_set = np.array(np.arange(id_num))
  print(xb.shape)
  logging.debug(xb.shape)
  # index = faiss.IndexFlatL2(xb.shape[1])
  # index2 = faiss.IndexIDMap(index)
  # index2.add_with_ids(xb, id_set)
  # faiss.write_index(index, 'faiss.index')

def sub(rconn, name):
  logging.debug('start subscription')

  pubsub = rconn.pubsub()
  pubsub.subscribe([SUBSCRIBE_TOPIC, 'index'])

  for item in pubsub.listen():

    channel = item['channel']
    data = item['data']
    logging.debug(data)
    print(data)

    if channel == b'crop':
      if data == b'START':
        spawn_indexer(str(uuid.uuid4()))
    elif channel == b'index':
      if data == b'DONE':
        print('DONE')
        # save_index()

if __name__ == '__main__':
  Process(target=sub, args=(rconn, 'xxx')).start()
  start_index()
