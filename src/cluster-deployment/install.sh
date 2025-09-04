#!/usr/bin/env bash

# Installer for cluster deployment tools.
# This should be part of cluster-deploy-<version>.tar.gz

set -e
PYTHON=python3.11
BASE_DIR=$(dirname $(readlink -fm $0))
CLUSTER_DIR=/opt/cerebras/cluster
PKG_DIR=/opt/cerebras/packages
DEPLOYMENT_DIR=/opt/cerebras/cluster-deployment

# create deployment dirs
mkdir -p ${CLUSTER_DIR}
mkdir -p ${PKG_DIR}
mkdir -p ${DEPLOYMENT_DIR}/meta/.locks
mkdir -p ${DEPLOYMENT_DIR}/logs
mkdir -p ${DEPLOYMENT_DIR}/packages
mkdir -p ${DEPLOYMENT_DIR}/backups

install_python_deps() {
    # Install deployment python's if not previously set up
    IS_PYTHON_INSTALL_REQUIRED=$(which $PYTHON && echo "" || echo "1")
    if [ -z "$IS_PYTHON_INSTALL_REQUIRED" ]; then
        echo "$PYTHON not detected, installing python from ${BASE_DIR}/packages/python311/"
        rpm --force -i "${BASE_DIR}"/packages/python311/*.rpm
    fi
    ln -sf "$(which $PYTHON)" /usr/bin/python
    # Set up python path
    SITEDIR=$($PYTHON -m site --user-site)
    mkdir -p "${SITEDIR}"
    echo "${DEPLOYMENT_DIR}/venv/lib/${PYTHON}/site-packages" > "${SITEDIR}/cluster_deployment.pth"

    # Install ansible as an executable if required
    if [ -z "$IS_PYTHON_INSTALL_REQUIRED" ] || ! which ansible > /dev/null; then
        echo "$PYTHON upgraded or ansible not found, installing ansible from ${BASE_DIR}/packages/ansible/"
        ${PYTHON} -m pip install "${BASE_DIR}/packages/ansible/ansible-4.10.0.tar.gz" -f "${BASE_DIR}/packages/ansible" --no-index
    fi
}

initial_db_migration() {
  # perform one-time schema migration if installing-to codebase is using legacy auto-generated migration strategy
  local migrations_dir=${DEPLOYMENT_DIR}/deployment/deployment_manager/db/migrations
  if [ ! -d ${migrations_dir} ] || [ -f ${migrations_dir}/0001_baseline.py ]; then
    # existence of 0001_baseline means that this installer has already completed once before implying already migrated
    return 0
  fi

  local backup_dir=${DEPLOYMENT_DIR}/backups/initial_db_migration
  mkdir -p ${backup_dir}
  echo "Backing up DB and migration files to ${backup_dir}"
  cp -r ${migrations_dir} ${backup_dir}
  cp ${DEPLOYMENT_DIR}/dm.db ${backup_dir}

  local workdir=$(mktemp -d)
  echo "Performing one-time migration to migration files in ${workdir}"
  cp -r "${BASE_DIR}/schema_migrator" "${workdir}"
  cp -r ${migrations_dir} "${workdir}/schema_migrator/schema_migrator/db/"
  pushd "${workdir}/schema_migrator" &>/dev/null
  ${PYTHON} ./main.py ${DEPLOYMENT_DIR}/dm.db
  popd &>/dev/null
  rm -rf "${workdir}"
  echo "Completed one-time DB migration"
}

initial_db_migration

# Create cscfg config file
CONFIG_FILE=${DEPLOYMENT_DIR}/cscfg.json
if [ ! -f $CONFIG_FILE ]; then
    echo "{}" > $CONFIG_FILE
    chmod 777 $CONFIG_FILE
fi

if [ -d "${BASE_DIR}/venv" ]; then
  echo "Syncing python packages"
  rsync -ar --delete "${BASE_DIR}/venv/" "${DEPLOYMENT_DIR}/venv"
fi

echo "Syncing scripts"
for src in deployment os-provision etc; do
  # etc directory does not exist in pytest environment
  if [ -d "${BASE_DIR}/${src}" ]; then
    mkdir -p ${DEPLOYMENT_DIR}/${src}
    rsync -ar --delete "${BASE_DIR}/${src}/" "${DEPLOYMENT_DIR}/${src}"
  fi
done

echo "Setting up environment"
install_python_deps

# If the installer package included python dependencies (make config-tool-pkg), for offline install,
# then this deploy directory will exist and should be used to install the dependencies without the internet.
if [ -d ${BASE_DIR}/packages/deployment ]; then
  ${PYTHON} -m pip install -e ${DEPLOYMENT_DIR}/deployment --find-links file://${BASE_DIR}/packages/deployment --no-index
else
  ${PYTHON} -m pip install -e ${DEPLOYMENT_DIR}/deployment
fi

cp ${DEPLOYMENT_DIR}/deployment/deployment_manager/bin/cscfg /usr/bin

pushd ${DEPLOYMENT_DIR}
export PYTHONPATH=${DEPLOYMENT_DIR}/deployment
export CLUSTER_DEPLOYMENT_BASE=${DEPLOYMENT_DIR}
echo "Running database migrations..."
${PYTHON} ${DEPLOYMENT_DIR}/deployment/deployment_manager/manage_db.py migrate --verbosity=2
if [ $? -ne 0 ]; then
    echo "Database migration failed!"
    exit 1
fi
echo "Database migrations completed successfully"
popd

echo "Deployment tools installed successfully"
