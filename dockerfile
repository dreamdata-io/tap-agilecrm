FROM python:3.7

WORKDIR /app

ADD tap_agilecrm tap_agilecrm
ADD setup.py .

ADD schemas schemas

RUN pip install .

ADD agilecrm_config.json .

CMD tap-agilecrm -c agilecrm_config.json