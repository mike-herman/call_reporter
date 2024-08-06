FROM python:3.12.4-bookworm

SHELL ["/bin/bash", "-c"]

WORKDIR /call_reporter_etl
COPY . .
RUN python3 -m venv .venv
RUN .venv/bin/pip install --upgrade pip
RUN .venv/bin/pip install -r requirements.txt

# VOLUME ["/call_reporter_etl/shared"]

# ARG AWS_ACCESS_KEY_ID
# ARG AWS_SECRET_ACCESS_KEY

# VOLUME shared

# RUN .venv/bin/python data_loader.py

# CMD /bin/bash
RUN chmod +x /call_reporter_etl/entrypoint.sh
ENTRYPOINT ["/call_reporter_etl/entrypoint.sh"]