import os
import shutil
import zipfile
import tensorflow as tf
import numpy as np


CLASSIFY_GRAPH = os.environ['CLASSIFY_GRAPH']

class Feature:
  def __init__(self, path, dataset_id):
    with tf.gfile.FastGFile(CLASSIFY_GRAPH, 'rb') as f:
      graph_def = tf.GraphDef()
      graph_def.ParseFromString(f.read())
      _ = tf.import_graph_def(graph_def, name='')

    with tf.Session() as sess:
      self._sess = sess
      self._pool3 = sess.graph.get_tensor_by_name('pool_3:0')

  def extract(self, file):
    with tf.gfile.GFile(file, 'rb') as fid:
      image_data = fid.read()
      pool3_features = self._sess.run(self._pool3,{'DecodeJpeg/contents:0': image_data})
      feature = np.squeeze(pool3_features)
      return feature
