FROM python:3.7

WORKDIR /app

RUN pip install singer-tools

ADD tap_agilecrm tap_agilecrm
ADD setup.py .

RUN pip install .

ADD agilecrm_config.json .

CMD tap-agilecrm -c agilecrm_config.json | singer-check-tap
# CMD tap-agilecrm -c agilecrm_config.json > /data/out.ndjson