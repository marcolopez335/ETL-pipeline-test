# AMMM Jira ETL image. Built by the build-image job in .gitlab-ci.yml, run by
# daily_run. Mirrors the FMM pipeline: base image from the internal Harbor
# mirror, corporate proxy + trusted Nexus host for pip.
ARG PYTHON_BASE=python:3.12
FROM ${PYTHON_BASE}

ARG http_proxy
ARG https_proxy
ARG no_proxy
ARG PIP_TRUSTED_HOST

ENV http_proxy=${http_proxy} \
    https_proxy=${https_proxy} \
    no_proxy=${no_proxy} \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONUNBUFFERED=1 \
    FORCE_SPINNER=0

# --- ODBC -------------------------------------------------------------------
# unixODBC is the driver manager. The Tibco ODBC driver itself is site-specific:
# copy the install steps from the FMM Dockerfile here (driver package + odbcinst.ini).
RUN apt-get update -qq \
    && apt-get install -y -qq --no-install-recommends unixodbc unixodbc-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# --- Python dependencies ------------------------------------------------------
# csm-commonlib is cloned by the build job into vendor/csm-commonlib (the runner
# already trusts GitLab), so the image needs no git, no token and no TLS exception.
COPY requirements.txt .
COPY vendor/csm-commonlib ./vendor/csm-commonlib
RUN grep -v -i "csm-commonlib" requirements.txt > /tmp/requirements.txt \
    && pip install --no-cache-dir ${PIP_TRUSTED_HOST:+--trusted-host ${PIP_TRUSTED_HOST}} \
        -r /tmp/requirements.txt ./vendor/csm-commonlib \
    && rm -rf ./vendor

# --- Project ------------------------------------------------------------------
COPY . .
RUN chmod +x entrypoint.sh && mkdir -p cache output backups logs

ENTRYPOINT ["./entrypoint.sh"]
