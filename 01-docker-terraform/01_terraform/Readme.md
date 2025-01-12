# Terraform <!-- omit in toc -->

Terraform is an open-source tool used for provisioning infrastructure resources. 

- [First steps](#first-steps)
  - [Creating an infrastructure for the project using Terraform](#creating-an-infrastructure-for-the-project-using-terraform)
  - [Setting up permissions for service account](#setting-up-permissions-for-service-account)
  - [Enabling APIs for the project](#enabling-apis-for-the-project)
- [Configuring Terraform](#configuring-terraform)
  - [Declarations](#declarations)
  - [Execution steps](#execution-steps)
  - [Terraform State](#terraform-state)
  - [Conslusions and Summary](#conclusions-and-summary)

## First steps

The first thing to do is to create a GCP project, followed by a service account and a key for said service account. After this, it needs to be exported to an environment variable (Ideally use a virtual environment for the project itself).

For this, gcloud cli needs to be installed, and then set up the environment:
```sh
export GOOGLE_APPLICATION_CREDENTIALS="<path/to/service-account-keys>.json"

# Refresh token and verify authentication
gcloud auth application-defalut login
```

**Side note:** The best way I've found to keep environment variables confined to specific environments while using conda is:

```sh
# Go to the environment folder and create the following directories:
(my_environment)$ cd $CONDA_PREFIX
(my_environment)$ mkdir -p ./etc/conda/activate.d
(my_environment)$ mkdir -p ./etc/conda/deactivate.d
# Then create the following files:
(my_environment)$ touch ./etc/conda/activate.d/env_vars.sh
(my_environment)$ touch ./etc/conda/deactivate.d/env_vars.sh
# Inside the activate.d/env_vars.sh file export your env variables
    #!/bin/sh

    export MY_VARIABLE=some_value
# Inside the deactivate.d/env_vars.sh file unset your env variables
    #!/bin/sh

    unset MY_VARIABLE

# Now the variables will be available when the environment is active and they will be unset when it is not.
```

### Creating an infrastructure for the project using Terraform

The project needs:

* Google Cloud Storage (GCS): Data Lake
* BigQuery: Data Warehouse

### Setting up permissions for service account

The service account needs certain roles and permissions to make changes in the project. For this it is necessary to add roles to the service account. Normally in production, you'd create customized roles with a more specific scope, but for simplicity, we will add:

*  The "Storage Admin" role (for GCS permissions) and "Storage Object Admin" role (for permission to the objects in GCS).
*  The "BigQuery Admin" role for the Data Lake.

This can be done directly from the GCP console on a web browser.

### Enabling APIs for the project

These APIs need to be enabled to allow the communication betweeen local and cloud environment. One for IAM and one for IAM credentials.

* https://console.cloud.google.com/apis/library/iam.googleapis.com
* https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com


## Configuring Terraform

The terraform configuration consists of the following files:

* .terraform-version: To ensure compatibility
* main.tf: Where we define the resources or providers terraform will use
* variables.tf: Where we store variables that can be called inside main.tf

### Declarations

* `terraform`: configure basic Terraform settings to provision your infrastructure
    * `required_version`: minimum Terraform version to apply to your configuration
    * `backend`: stores Terraform's "state" snapshots, to map real-world resources to your configuration.
        * `local`: stores state file locally as terraform.tfstate
    * `required_providers`: specifies the providers required by the current module
* `provider`:
    * adds a set of resource types and/or data sources that Terraform can manage
    * The Terraform Registry is the main directory of publicly available providers from most major infrastructure platforms.
* `resource`
    * blocks to define components of your infrastructure
    * Project modules/resources: google_storage_bucket, google_bigquery_dataset, google_bigquery_table
* `variable` & `locals`
    * runtime arguments and constants

### Execution steps

1. `terraform init`: 
    * Initializes & configures the backend, installs plugins/providers, & checks out an existing configuration from a version control 
2. `terraform plan`:
    * Matches/previews local changes against a remote state, and proposes an Execution Plan.
3. `terraform apply`: 
    * Asks for approval to the proposed plan, and applies changes to cloud
4. `terraform destroy`
    * Removes your stack from the Cloud (Recommended to avoid leaving resources on stand-by)

### Terraform state

Terraform saves the state of the resources it creates. In my case, I had worked on this in the past, then deleted everything manually and I came back to it later. 

This caused my terraform state to be out of sync, giving me issues as it didn't have permissions or the resources it was trying to reach didn't exist. The way to solve this was to clean the terraform state (be mindful of this, as terraform forgetting of a resource will prevent it from destroying it also. Potentially leaving resources active you are not aware of)

```bash
terraform state list
```

example to remove a resource identified:
```bash
terraform state rm 'bigquery.BigQueryDataset'
```

* Side note: Another issue I've found since I've run this on multiple configurations (On local PC and on a VM on GCP cloud) was that sometimes trying to do a terraform apply would run into an 'insufficient privilidges' error. This was because of the default Google APIs access when creating the virtual machine.

A solution is to shut down the virtual machine if it is on and click on "Edit". Somewhere down the page where it says "Cloud API access scopes" change the default option to "Allow full access to all Cloud APIs".

### Conclusions and Summary

This is an introduction topic and it is mostly theory although there is a small practice element.

A big part of the effort on this module is the configuration and setup needed to get everything running. I will not add all the details here since it actually takes a good amount of effort to get everything running and it is dependent on system to system. It's probably best to follow the course video if it is needed to re-do the set up.

I've done the set up a couple of times when starting the course since I changed computers and took breaks in between. My recommendation is to do it directly on a virtual machine since it facilitates many things, especially if you are coming from windows. 