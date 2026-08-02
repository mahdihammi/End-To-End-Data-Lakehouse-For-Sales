FROM astrocrpublic.azurecr.io/runtime:3.1-9


COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir -r /tmp/requirements.txt