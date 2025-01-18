## Setup (Official)

### Pre-Reqs

1. For the sake of standardization across this workshop's config,
    rename your gcp-service-accounts-credentials file to `google_credentials.json` & store it in your `$HOME` directory
    ``` bash
        cd ~ && mkdir -p ~/.google/credentials/
        mv <path/to/your/service-account-authkeys>.json ~/.google/credentials/google_credentials.json
    ```

2. You may need to upgrade your docker-compose version to v2.x+, and set the memory for your Docker Engine to minimum 5GB
(ideally 8GB). If enough memory is not allocated, it might lead to airflow-webserver continuously restarting.

3. Python version: 3.7+


### Airflow Setup

1. Create a new sub-directory called `airflow` in your `project` dir (such as the one we're currently in)

2. **Set the Airflow user**:

    On Linux, the quick-start needs to know your host user-id and needs to have group id set to 0. 
    Otherwise the files created in `dags`, `logs` and `plugins` will be created with root user. 
    You have to make sure to configure them for the docker-compose:

    ```bash
    mkdir -p ./dags ./logs ./plugins
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```
   
3. **Import the official docker setup file** from the latest Airflow version:
   ```shell
   curl -LfO 'https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml'
   ```
   
4. It could be overwhelming to see a lot of services in here. 
   But this is only a quick-start template, and as you proceed you'll figure out which unused services can be removed.
   Eg. I've chosen to use the LocalExecutor, as it is the executor for a single machine. CeleryExecutor is used to distribute tasks across a multi-machine setup.

### Changes in docker-compose file

1. The default file will start using an "image" `image: ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.2}`. In my case, I am changing it to a "build", which indicates a dockerfile instead.

2. Mount the `google_credentials` directory as read-only on the `volumes`section.

3. Set the executor as LocalExecutor as we're working on a single machine.

4. Set environment variables: GCP_PROJECT_ID, GCP_GCS_BUCKET, GOOGLE_APPLICATION_CREDENTIALS & AIRFLOW_CONN_GOOGLE_CLOUD_DEFAULT, as per your config.

5. Change AIRFLOW__CORE__LOAD_EXAMPLES to false (optional)

### Prepare dockerfile

When running airflow locally, you might want to customize the image by adding some extra dependencies, such as python packages and more.

Create the dockerfile using as starting point the image which the downloaded docker-compose file was pointing to. 

Then customize the image with the packages you need, such as `gcloud` to connect with GCS bucket/data lake.

Additionally, integrate `requirements.txt` to instal libraries via pip.