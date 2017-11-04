FROM bluelens/bl-index-image-base:latest

RUN mkdir -p /usr/src/app

COPY . /usr/src/app

WORKDIR /usr/src/app
RUN pip install --no-cache-dir -r requirements.txt

CMD ["python2", "main.py"]

