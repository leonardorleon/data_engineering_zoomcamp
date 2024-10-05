# Custom dockerfile to prepare the airflow image

FROM apache/airflow:2.10.2

# /opt/airflow is the standard airflow home folder. The folders in it are set to volumes on local machine
ENV AIRFLOW_HOME=/opt/airflow 

USER root
RUN apt-get update -qq && apt-get install vim -qqq

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Details on google cloud SDK instalation in docker
# Ref: https://airflow.apache.org/docs/docker-stack/recipes.html

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

ARG CLOUD_SDK_VERSION=322.0.0
ENV GCLOUD_HOME=/opt/google-cloud-sdk

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       --additional-components alpha beta kubectl \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && gcloud --version

WORKDIR ${AIRFLOW_HOME}

# Bring the scripts into the airflow container
#COPY scripts scripts
#RUN chmod +x scripts

USER $AIRFLOW_UID