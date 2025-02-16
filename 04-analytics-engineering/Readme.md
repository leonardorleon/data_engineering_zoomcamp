# Analytics engineering

First, as a requirement for this module, we have yellow taxi and green taxi data, as well as fhv datasets. The first two were ingested to big query with the previous modules. And the fhv data was ingested with the kestra flow shown in the following file: [09_gcp_fhv_scheduled.yml](09_gcp_fhv_scheduled.yml).

# Setting up Dbt core with docker and BigQuery

First, we need to use this [Dockerfile](docker_setup/Dockerfile) based on the official dbt git [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/04-analytics-engineering/docker_setup/Dockerfile). Note that I've changed the image version to the current dbt big query image according to [this](https://github.com/dbt-labs/dbt-bigquery/pkgs/container/dbt-bigquery)

This Dockerfile contains various "targets" meaning that you can indicate in your docker-compose which part of the file it should run. They are defined in the docker file with something like `FROM base AS target_name`

Afterwards, set up a [docker-compose file](docker_setup/docker-compose.yaml) with context to where the Dockerfile is and the target for big query portion of the dockerfile.

Make sure to mount three volumes as needed for dbt:

- for persisting dbt data
- path to the dbt `profiles.yml`
- path to the `google_credentials.json` file which should be in the `~/.google/credentials/` path

Create `profiles.yml` file in `~/.dbt/` in your local machine or add the following code in your existing `profiles.yml` 

* NOTE: Despite the profiles.yml being in your local device, the `keyfile` path should be the one on the container (if they're not the same) since this runs inside the container. 

  ```yaml
  bq-dbt-workshop:
    outputs:
      dev:
        dataset: <bigquery-dataset>
        fixed_retries: 1
        keyfile: /.google/credentials/google_credentials.json
        location: EU
        method: service-account
        priority: interactive
        project: <gcp-project-id>
        threads: 4
        timeout_seconds: 300
        type: bigquery
    target: dev
  ```


Run the following commands -
  - ```bash 
    docker compose build 
    ```
  - ```bash 
    docker compose run dbt-bq-dtc init
    ``` 
    - **Note:** We are essentially running `dbt init` above because the `ENTRYPOINT` in the [Dockerfile](Dockerfile) is `['dbt']`.
    - Input the required values. Project name will be `taxi_rides_ny`
    - This should create `dbt/taxi_rides_ny/` and you should see `dbt_project.yml` in there.
    - In `dbt_project.yml`, replace `profile: 'taxi_rides_ny'` with `profile: 'bq-dbt-workshop'` as we have a profile with the later name in our `profiles.yml`
  - ```bash
    docker compose run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc debug
     ``` 
    - to test your connection. This should output `All checks passed!` in the end.

    - **Note:** The automatic path conversion in Git Bash will cause the commands to fail with `--workdir` flag. It can be fixed by prefixing the path with `//` as is done above. The solution was found [here](https://github.com/docker/cli/issues/2204#issuecomment-638993192).
    - Also, we change the working directory to the dbt project because the `dbt_project.yml` file should be in the current directory. Else it will throw `1 check failed: Could not load dbt_project.yml`


## Some extra setups I did to use this docker configuration

This is more of a workaround, but in the current set up you have to execute the docker-compose from the folder where the docker files are located and it is a cumbersome command to type up:

```bash
docker compose run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc debug
``` 

So another option is to create an executable file which does this for us:

```bash
#!/bin/bash
docker-compose -f /home/leo/leo_data_engineering/04-analytics-engineering/docker_setup/docker-compose.yaml run --workdir="//usr/app/dbt/taxi_rides_ny" dbt-bq-dtc "$@"
```

And to make it even easier, I've also set up an alias in `~/.bashrc`

```bash
alias d-dbt='/home/leo/leo_data_engineering/04-analytics-engineering/docker_setup/dbt.sh'
```

This is more similar to running dbt installed in the host computer and saves a lot of hassle when running dbt. Either way, the best way would probably be to run it locally and use things like dbt power user extension like I do in my work set up. That being said, for the purposes of the course and the limited space on the vm I am working on, I will settle with the docker configuration.