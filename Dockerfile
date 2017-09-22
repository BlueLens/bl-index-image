FROM bluelens/faiss:ubuntu16-py2

RUN mkdir -p /usr/src/app
RUN mkdir -p /dataset/deepfashion

WORKDIR /usr/src/app

COPY . /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH $PYTHONPATH:/usr/src/app/faiss
ENV CLASSIFY_GRAPH ./classify_image_graph_def.pb

CMD ["python2", "main.py"]
