#
# COPYRIGHT 2025 Cerebras Systems, Inc.
#
"""
Module to push data to CBDA database
"""
import time
import enum
import logging
from datetime import datetime, timezone
from sqlalchemy import create_engine, select, insert, update, func
from sqlalchemy import (
    Table,
    MetaData,
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    ARRAY,
)

# metadata object to hold table schema
METADATA_OBJ = MetaData()


class JobStatus(enum.Enum):
    """
    Enum class for job status
    """

    IN_PROGRESS = "in progress"
    PASS = "pass"
    FAIL = "fail"


class JobName(enum.Enum):
    """
    Enum class for job names
    """

    IT_SANITY = "it_sanity"
    ASIC_BRINGUP = "asic_bringup"
    THERMALS = "thermals"
    COMMISSIONING = "commissioning"
    ONLINE_REPAIR = "repair"
    CLUSTER_DEPLOY = "cluster_deploy"
    QA_BRINGUP = "qa_bringup"
    PRESHIPPING_CLEANUP = "preshipping_cleanup"
    OFFLINE_REPAIR = "offline_repair"


class Retry:  # pylint: disable=logging-fstring-interpolation
    """
    Utility class implementing retry decorators
    """

    LOG = None

    @staticmethod
    def on_exception(
        retries=3, delay_sec=1, exceptions=(Exception,)
    ):  # pylint: disable=inconsistent-return-statements
        """
        wrapper to retry a function call on exception.
        """

        def retry_wrapper(method):
            def wrapper(*args, **kwargs):
                retry_counter = 0
                if Retry.LOG is None:
                    log = logging.getLogger(__name__)
                    log.setLevel(logging.DEBUG)
                else:
                    log = Retry.LOG
                for retry_counter in range(retries):
                    try:
                        return method(*args, **kwargs)
                    except exceptions as e:
                        if retry_counter < retries - 1:
                            log.debug(f"function failed with {e}. Retrying")
                            time.sleep(delay_sec)
                        else:
                            log.warning(
                                f"Failed for all {retries} attempts: {e}"
                            )
                            return False

            return wrapper

        return retry_wrapper


class CbdaBaseInterface:  # pylint: disable=logging-fstring-interpolation
    """
    This Class is the System Bringup and Data Analytics Interface
    class, consists of API's to transfer/query metadata to/from
    Postgres SQL DB.
    """

    # Default to dev database
    POSTGRES_SQL_DB_HOST = "cbda-postgres-dev.cerebras.aws"
    POSTGRES_SQL_USERNAME = "system_bring_up_writer"
    POSTGRES_SQL_PASSWORD = "writer0976"
    POSTGRES_SQL_DB_NAME = "cbdapg"
    SQL_TBL_SYSTEM = Table(
        "system",
        METADATA_OBJ,
        Column("system_id", Integer, primary_key=True),
        Column("name", String),
        schema="component_info",
    )
    SQL_TBL_RUN = Table(
        "system_bring_up_run",
        METADATA_OBJ,
        Column("system_bring_up_run_id", Integer, primary_key=True),
        Column("system_id", Integer, nullable=False),
        Column("run_start_ts", DateTime, nullable=False),
        Column("run_stop_ts", DateTime),
        Column("job_name", String, nullable=False),
        Column("status", String, nullable=False),
        Column("debug", Boolean),
        Column("errors", ARRAY(String)),
        Column("warnings", ARRAY(String)),
        Column("location", String),
        Column("rack_name", String),
        Column("jenkins_url", String),
        Column("operator", String),
        schema="component_testing",
    )

    def __init__(
        self,
        system_name: str,
        job_name: JobName,
        exec_timeout_msec=300000,  # timeout of 5 minutes
        log_handler: logging.Logger = None,
    ):
        self.system_name = system_name
        self.job_name = job_name
        self.pg_engine = None
        self.system_bring_up_run_id = None
        self.exec_timeout_msec = exec_timeout_msec
        if log_handler is None:
            self.log = logging.getLogger(__name__)
            self.log.setLevel(logging.DEBUG)
            Retry.LOG = self.log
        else:
            self.log = log_handler
            Retry.LOG = log_handler
        self._init_db_engine()

    def _init_db_engine(self) -> None:
        """
        Initialize postgres database engine
        """
        pg_connection = (
            f"postgresql://{self.POSTGRES_SQL_USERNAME}:"
            f"{self.POSTGRES_SQL_PASSWORD}@"
            f"{self.POSTGRES_SQL_DB_HOST}/"
            f"{self.POSTGRES_SQL_DB_NAME}"
        )
        self.log.debug(f"CBDA - PG Engine Connect Cmd - {pg_connection}")
        try:
            self.pg_engine = create_engine(
                pg_connection,
                pool_pre_ping=True,
                pool_recycle=60,
                connect_args={
                    "options": f"-c statement_timeout={self.exec_timeout_msec}"
                },
            )
        except Exception as e:  # pylint: disable=broad-except
            self.log.error(
                f"CBDA - Failed to initialize Postgres DB engine: {e}"
            )

    def _check_job_name(self) -> bool:
        """
        Checks if job_name is valid.
        """
        success = True
        if not isinstance(self.job_name, JobName):
            self.log.error(f"Expecting job_name to be of instance {JobName}.")
            success = False
        return success

    def _check_system(self) -> bool:
        """
        Checks if system name is present in system table
        """
        if self.pg_engine is None:
            self.log.error("CBDA - Postgres SQL engine not initialized.")
            success = False
        else:
            query = (
                select(func.count("*"))  # pylint: disable=not-callable
                .select_from(self.SQL_TBL_SYSTEM)
                .where(self.SQL_TBL_SYSTEM.c.name == self.system_name)
            )
            self.log.debug(f"CBDA - Check System Query - {query}")
            with self.pg_engine.connect() as conn:
                count = conn.execute(query).first()[0]
                if count == 1:
                    success = True
                elif count == 0:
                    success = False
                    self.log.error(
                        f"{self.system_name} not found in "
                        f"CBDA table {self.SQL_TBL_SYSTEM}. "
                        "Contact SBA team."
                    )
                else:
                    success = False
                    self.log.error(
                        f"Multiple entries for {self.system_name} "
                        "found in system table. "
                        "Cannot resolve unique system_id."
                        "Contact SBA team."
                    )
        return success

    def _get_system_id(self) -> int:
        """
        Get system_id from system table
        """
        system_id = None
        if self.pg_engine is None:
            self.log.error("CBDA - Postgres SQL engine not initialized.")
        else:
            query = select(self.SQL_TBL_SYSTEM.c.system_id).where(
                self.SQL_TBL_SYSTEM.c.name == self.system_name
            )
            self.log.debug(f"CBDA - Get System ID Query - {query}")
            with self.pg_engine.connect() as conn:
                system_id = conn.execute(query).first()
                if system_id is not None:
                    system_id = system_id[0]
                    self.log.debug(f"CBDA - System ID - {system_id}")
        return system_id

    def _get_run_id(self) -> int:
        """
        Create new run_id for a job on system.
        This also populates start time of the run.
        """
        run_id = None
        if self.pg_engine is None:
            self.log.error("CBDA - Postgres SQL engine not initialized.")
        elif (sys_id := self._get_system_id()) is None:
            self.log.error(
                f"CBDA - Could not get system_id of {self.system_name}, "
                f"from table {self.SQL_TBL_SYSTEM}. "
            )
        else:
            # insert required fields into run table
            data = {
                self.SQL_TBL_RUN.c.system_id.name: sys_id,
                self.SQL_TBL_RUN.c.run_start_ts.name: datetime.now(
                    timezone.utc
                ),
                self.SQL_TBL_RUN.c.job_name.name: self.job_name.value,
                self.SQL_TBL_RUN.c.status.name: JobStatus.IN_PROGRESS.value,
            }
            # insert entry into table and return run_id
            insert_query = insert(self.SQL_TBL_RUN).returning(
                self.SQL_TBL_RUN.c.system_bring_up_run_id
            )
            self.log.debug(
                f"CBDA - Running insert query - {insert_query} "
                f"with data - {data}"
            )
            # INSERT queries should use begin() context manager to
            # commit transaction on success or rollback on failure
            with self.pg_engine.begin() as conn:
                result_row = conn.execute(insert_query, [data]).first()
                if result_row is not None:
                    run_id = result_row[0]
                    self.log.debug(f"CBDA - Run ID - {run_id}")
        return run_id

    def _update_row_with_fields(self, fields: dict) -> bool:
        """
        Update row in SQL_TBL_RUN with fields in dictionary.
        NOTE: system_bring_up_run_id needs to be defined as
              it is used as primary key for the row.
        """
        if not fields:
            self.log.info("CBDA - No additional fields to update.")
            success = True
        else:
            db_columns = [
                col.name
                for col in self.SQL_TBL_RUN.columns  # pylint: disable=not-an-iterable
            ]
            for key in fields.keys():
                if key not in db_columns:
                    self.log.warning(
                        f"CBDA - Skipping field {key} as it was "
                        f"not found in table {self.SQL_TBL_RUN}. "
                        "Refer to <document> for valid fields."
                    )
                    del fields[key]
            if fields:
                query = update(self.SQL_TBL_RUN).where(
                    self.SQL_TBL_RUN.c.system_bring_up_run_id
                    == self.system_bring_up_run_id
                )
                self.log.debug(
                    f"CBDA - Update Query - {query} with data - {fields}"
                )
                with self.pg_engine.begin() as conn:
                    conn.execute(query, [fields])
                    success = True
                    self.log.info(
                        f"CBDA - Updated {self.system_bring_up_run_id} "
                        f"with fields - {fields}"
                    )
            else:
                self.log.warning("CBDA - No valid fields to update row.")
        return success

    @Retry.on_exception(retries=3, delay_sec=60)
    def start_run(self, additional_fields: dict = None) -> bool:
        """
        Verify job name and start job run
        by creating a new run_id in database.
        Method also sets the job status to IN_PROGRESS

        Keyword Arguments:
        additional_fields -- Dictionary of additional fields to be
                            updated for newly created run_id. Dictionary
                            should have keys as column names of SQL_TBL_RUN
                            and values as data to be updated.
        """
        if self._check_system() and self._check_job_name():
            if run_id := self._get_run_id():
                self.system_bring_up_run_id = run_id
                self.log.info(
                    f"CBDA - Started run for {self.job_name.value} "
                    f" with run_id - {run_id}"
                )
                success = True
            else:
                self.log.warning("CBDA - Could not generate new run_id.")
                success = False

            if (
                success
                and self.system_bring_up_run_id is not None
                and not self._update_row_with_fields(additional_fields)
            ):
                self.log.warning(
                    "CBDA - Could not update "
                    f"run_id: {self.system_bring_up_run_id}, "
                    "with additional fields."
                )
        else:
            success = False

        return success

    @Retry.on_exception(retries=3, delay_sec=60)
    def end_run(
        self, job_status: JobStatus, additional_fields: dict = None
    ) -> bool:
        """
        End job run by updating end timestamp of run_id.
        Method also sets the job status to the value of
        job_status argument.

        Keyword Arguments:
        additional_fields -- Dictionary of additional fields to be
                            updated for run_id. Dictionary should have
                            keys as column names of SQL_TBL_RUN and values
                            as data to be updated.
                            Common fields to update at
                            end of run are 'errors' and 'warnings'.
                            example: additional_fields = {
                                        'errors': ['error1','error2'],
                                        'warnings':['warning1']
                                     }
        """
        if self.system_bring_up_run_id is None:
            self.log.warning(
                "CBDA - Can't set end timestamp for run "
                "as start timestamp was not set."
            )
            success = False
        elif not isinstance(job_status, JobStatus):
            self.log.error(
                f"CBDA - job_status has type({type(job_status)}). "
                f"Expecting instance of {JobStatus}. "
            )
            success = False
        else:
            data = {
                self.SQL_TBL_RUN.c.run_stop_ts.name: datetime.now(timezone.utc),
                self.SQL_TBL_RUN.c.status.name: job_status.value,
            }
            query = update(self.SQL_TBL_RUN).where(
                self.SQL_TBL_RUN.c.system_bring_up_run_id
                == self.system_bring_up_run_id
            )
            self.log.debug(f"CBDA - update query - {query} with data - {data}")
            with self.pg_engine.begin() as conn:
                conn.execute(query, [data])
                success = True
                self.log.info(
                    f"CBDA - Ended run for {self.job_name.value}, "
                    f"run_id: {self.system_bring_up_run_id} "
                    f"with job status '{job_status.value}'."
                )

            if success and not self._update_row_with_fields(additional_fields):
                self.log.warning(
                    "CBDA - Could not update "
                    f"run_id: {self.system_bring_up_run_id}, "
                    "with additional fields."
                )
                # Don't change success status
                # when additional fields update fails
            self.system_bring_up_run_id = None
        return success