FROM python:3.12.4-bookworm

SHELL ["/bin/bash", "-c"]

WORKDIR /call_reporter_etl
COPY . .
RUN python -m venv .venv
RUN .venv/bin/pip install --upgrade pip
RUN .venv/bin/pip install -r requirements.txt

# && \
#     source .venv/bin/activate && \
#     pip install --upgrade pip && \
#     pip install -r requirements.txt

# Install dependencies

# COPY pyproject.toml .

# RUN python -m pip install --upgrade pip && python -m pip install pipx && \
#     python -m pipx ensurepath && \
#     source ~/.bashrc && \
#     python -m pipx install poetry && \
#     poetry install

# Add the rest of the files
# COPY data_loader.py .
# COPY ncua_ddl.sql .

# ARG DB_FILE
# ENV DB_FILE=${DB_FILE}
VOLUME ["/call_reporter_etl/shared"]

# RUN python3 -m venv .venv && chmod -R 777 .venv
# # RUN chmod -R 777 .venv
# RUN .venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt
# ENV AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID
CMD /bin/bash
