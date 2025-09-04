This directory contains various scripts and tools related to cluster deployment, such as building the OS images, configuring networking, etc.

Each subdirectory roughly corresponds to each segment of the deployment process - see the README within each subdirectory for specific details.

**Run Cluster-Deployment on MacOs**

* install the dependencies:
    * [Docker Desktop](https://docs.docker.com/desktop/setup/install/mac-install/)
    * [Git LFS](https://docs.github.com/en/repositories/working-with-files/managing-large-files/installing-git-large-file-storage?platform=mac)(I used the Homebrew version)
    * Astyle:
        ```bash
        brew install astyle
        ```

* clone the monolith repo:

    ```bash
    git clone git@github.com:Cerebras/monolith.git
    ```

* setup a python venv

    ```bash
    python3 -m venv clsdep
    ```

    ```bash
    source clsdep/bin/activate
    ```

    ```bash
    pip3 install annotated-types asgiref attrs bcrypt certifi cffi charset-normalizer \
             click cmd2 cryptography decorator Django docker idna iniconfig Jinja2 jsonpatch \
             jsonpath-rw jsonpointer jsonschema jsonschema-specifications MarkupSafe packaging \
             paramiko pluggy ply pycparser pydantic pydantic_core PyNaCl pyperclip pytest pytz \
             PyYAML redfish referencing requests requests-toolbelt requests-unixsocket rpds-py \
             six sqlparse tabulate typing_extensions urllib3 wcwidth setuptools
    ```

* build the docker image for Apple’s CPU:

    ```bash
    export GITTOP="<PATH_TO_MONOLITH>
    ```

    ```bash
    cd monolith/src/cluster_deployment/deployment/deployment_manager/tests/docker
    ```
    
    ```bash
    make local
    ```

* run the tests:

    ```bash
    cd monolith/src/cluster_deployment/deployment/deployment_manager
    ```

    ```bash
    pytest tests/test_*
    ```

    ```bash
    pytest network_config/tests/test_*
    ```

**Known errors and fixes**  

Error:

```bash
docker.errors.APIError: 403 Client Error for http+docker://localhost/v1.40/networks ...
Forbidden ("error while removing network: network ... has active endpoints")
```

Fix:

* Inspect the Network to Identify Attached Containers:

    ```bash
    docker network inspect <NW_ID_FROM_ERROR_MSG>
    ```

* Look for the "Containers" section in the output, which will list container IDs and names.
* For each container identified:

    ```bash
    docker stop <CONTAINER_ID>
    ```

    ```bash
    docker rm <CONTAINER_ID>
    ```

* Remove the Network:

    ```bash
    docker network rm <NW_ID_FROM_ERROR_MSG>
    ```
