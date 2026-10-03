import base64
import json
import logging
import os
import re
import threading
import time
import uuid
from itertools import groupby
from pathlib import Path

from kubernetes import client as k8s_client, config, watch
from kubernetes.client.rest import ApiException
from socketio import Client as SocketIO

from vantage6.common import logger_name
from vantage6.common.client.node_client import NodeClient
from vantage6.common.dataclass import TaskDB
from vantage6.common.docker.addons import (
    get_digest,
    get_image_name_wo_tag,
)
from vantage6.common.enum import AlgorithmStepType, RunStatus, TaskDatabaseType
from vantage6.common.globals import (
    APPNAME,
    DATAFRAME_BETWEEN_GROUPS_SEPARATOR,
    DATAFRAME_WITHIN_GROUP_SEPARATOR,
    DEFAULT_ALPINE_IMAGE,
    ENV_VAR_EQUALS_REPLACEMENT,
    SESSION_STATE_FILENAME,
    STRING_ENCODING,
    ContainerEnvNames,
    NodePolicy,
)
from vantage6.common.kubernetes.utils import database_env_label

from vantage6.cli.context.node import NodeContext
from vantage6.cli.node.common.task_cleanup import (
    ResourceDeletionResult,
    delete_run_generation_resources,
)
from vantage6.cli.utils_kubernetes import replace_localhost_for_k8s

from vantage6.node.enum import KillInitiator
from vantage6.node.globals import (
    CLEANUP_RETRY_INTERVAL_SECONDS,
    DATABASE_BASE_PATH,
    DEFAULT_PROXY_PORT,
    ENV_VARS_NOT_SETTABLE_BY_NODE,
    K8S_EVENT_STREAM_LOOP_TIMEOUT,
    TASK_JOB_INPUT_PATH,
    TASK_JOB_OUTPUT_PATH,
    TASK_JOB_SESSION_FOLDER_PATH,
    TASK_START_RETRIES,
)
from vantage6.node.k8s.data_classes import KilledRun, Result, ToBeKilled
from vantage6.node.k8s.exceptions import (
    DataFrameNotFound,
    PermanentAlgorithmStartFail,
)
from vantage6.node.k8s.jobpod_state_to_run_status_mapper import (
    compute_run_pod_status,
)
from vantage6.node.k8s.network_isolation import validate_algorithm_isolation
from vantage6.node.k8s.run_io import RunIO
from vantage6.node.k8s.run_registry import (
    ACTIVE_STATE_KILLING,
    CLEANUP_REASON_COMPLETED,
    CLEANUP_REASON_KILLED,
    CLEANUP_REASON_RECONCILE,
    CLEANUP_REASON_SHUTDOWN,
    ActiveRun,
    CleanupRecord,
    RunRegistry,
)
from vantage6.node.util import get_parent_id

# Label/annotation keys identifying the run generation owning a Kubernetes object.
LABEL_RUN_ID = "run_id"
LABEL_ATTEMPT_TAG = "attempt_tag"
LABEL_NODE_ID = "node_id"
ANN_ATTEMPT_TAG = "attempt_tag"
ANN_DOCKER_LOGIN_SECRET = "docker_login_secret_name"


class ContainerManager:
    def __init__(self, ctx: NodeContext, client: NodeClient):
        """
        Initialization of ``ContainerManager`` that handles communication with the
        Kubernetes cluster.

        Parameters
        ----------
        ctx: NodeContext
            Context object from which some settings are obtained
        """
        self.log = logging.getLogger(logger_name(__name__))
        self.log.debug("Initializing ContainerManager")
        self.ctx = ctx
        self.client = client
        self.socket_io = None
        self.num_active_tasks = 0

        # When a pod runs in Kubernetes with a ServiceAccount, Kubernetes automatically
        # mounts the service account credentials at
        # /var/run/secrets/kubernetes.io/serviceaccount/.
        try:
            config.load_incluster_config()
        except Exception:
            self.log.exception("Error loading Kubernetes configuration")
            raise

        # Get the location where the file is stored on the host system,
        self.host_data_dir = self.ctx.config["task_dir"]

        self.databases = self._get_database_metadata()

        # before a task is executed it gets exposed to these policies
        self._policies = self._setup_policies(self.ctx.config)

        # K8S Batch API
        self.batch_api = k8s_client.BatchV1Api()
        # K8S Core API instance
        self.core_api = k8s_client.CoreV1Api()
        # namespace to run the tasks in
        self.task_namespace = self.ctx.config["task_namespace"]

        # labels to identify the task jobs of this node
        self.task_job_labels = {LABEL_NODE_ID: self.ctx.identifier}
        self.task_job_label_selector = ",".join(
            [f"{k}={v}" for k, v in self.task_job_labels.items()]
        )

        # Persistent registry of run generations and pending cleanups. Survives
        # node restarts so that retried or delayed cleanup never removes a newer
        # generation.
        self._task_dir_extension = self.ctx.config.get("dev", {}).get(
            "task_dir_extension"
        )
        self.registry = RunRegistry(
            self.host_data_dir, task_dir_extension=self._task_dir_extension
        )

        # whether to share or not algorithm logs with HQ
        # TODO: config loading could be centralized in a class, then validate,
        # set defaults, warn about dangers, etc
        self.share_algorithm_logs = self.ctx.config.get("share_algorithm_logs", True)
        if self.share_algorithm_logs:
            self.log.warning("Algorithm logs and errors will be shared with HQ.")

        self._start_cleanup_reconciler()

    def set_socket(self, socket_io: SocketIO) -> None:
        """
        Set the socketIO object.
        """
        self.socket_io = socket_io

    def ensure_task_namespace(self) -> bool:
        """
        Ensure that the namespace for the task exists and jobs can be created in it.
        If it does not exist, try to create it

        Returns
        -------
        bool
            True if the namespace exists or was created and is writeable, False
            otherwise
        """
        # TODO consider using hierarchical namespaces, it may be easier to create
        # those in a large cluster environment
        # https://kubernetes.io/blog/2020/08/14/introducing-hierarchical-namespaces/
        task_namespace_exists = False
        try:
            self.core_api.read_namespace(self.task_namespace)
            task_namespace_exists = True
        except ApiException as exc:
            # return False if the namespace exists but we cannot read it
            if exc.status != 404:
                return False

        if not task_namespace_exists:
            self.log.warning(
                "Task namespace '%s' does not exist, creating it now.",
                self.task_namespace,
            )
            namespace = k8s_client.V1Namespace(
                metadata=k8s_client.V1ObjectMeta(name=self.task_namespace)
            )
            try:
                self.core_api.create_namespace(namespace)
            except ApiException as exc:
                self.log.error(
                    "Failed to create task namespace '%s': %s", self.task_namespace, exc
                )
                return False

        # try to see if jobs can be created in the cluster - if not, tasks cannot be
        # created so we return False
        test_pod_name = "v6-test-pod-" + str(uuid.uuid4())
        try:
            self.core_api.create_namespaced_pod(
                namespace=self.task_namespace,
                body=k8s_client.V1Pod(
                    metadata=k8s_client.V1ObjectMeta(name=test_pod_name),
                    spec=k8s_client.V1PodSpec(
                        containers=[
                            k8s_client.V1Container(
                                name="test-container",
                                image=DEFAULT_ALPINE_IMAGE,
                            )
                        ]
                    ),
                ),
            )
        except ApiException as exc:
            self.log.error(
                "Failed to create test pod in task namespace '%s': %s",
                self.task_namespace,
                exc,
            )
            return False
        # clean up the test pod
        self.core_api.delete_namespaced_pod(test_pod_name, self.task_namespace)

        return True

    def validate_algorithm_isolation(self) -> tuple[bool, str]:
        """
        Verify that algorithm containers cannot reach the public internet.

        Returns
        -------
        tuple[bool, str]
            (is_isolated, message)
        """
        return validate_algorithm_isolation(
            core_api=self.core_api,
            task_namespace=self.task_namespace,
            log=self.log,
        )

    def _setup_policies(self, config: dict) -> dict:
        """
        Set up policies for the node.

        Parameters
        ----------
        config: dict
            Configuration dictionary

        Returns
        -------
        dict
            Dictionary with the policies
        """
        policies = config.get("policies", {})
        if not policies or (
            not policies.get(NodePolicy.ALLOWED_ALGORITHMS.value)
            and not policies.get(NodePolicy.ALLOWED_ALGORITHM_STORES.value)
        ):
            self.log.warning(
                "No policies on allowed algorithms have been set for this node!"
            )
            self.log.warning(
                "This means that all algorithms are allowed to run on this node."
            )
        return policies

    def _get_image_pull_policy(self) -> str:
        """
        Determine the Kubernetes image pull policy from the node's policies.

        Returns
        -------
        str
            "Always" if the algorithm image must always be pulled (the
            default), otherwise "IfNotPresent" so a locally built image can
            be used without a registry.
        """
        require_algorithm_pull = self._policies.get(
            NodePolicy.REQUIRE_ALGORITHM_PULL.value, True
        )
        return "Always" if require_algorithm_pull else "IfNotPresent"

    def _get_database_metadata(self) -> dict:
        """
        Collect information about the databases.

        This function will return a dictionary with contains a key for every data
        source. Each key will have a dictionary with the following keys:
        - uri: the URI of the database
        - local_uri: the local URI of the database
        - is_file: whether the database is a file
        - is_dir: whether the database is a directory
        - type: the type of the database
        - env: the environment variables to be passed to the algorithm container

        It reads these from the environment variables that are passed to the node.

        Returns
        -------
        dict
            Dictionary with the information about the databases

        Examples
        --------
        When the environment variables are set as follows:
        ```bash
        DATABASE_LABELS="db1"
        DATABASE_DB1_URI="postgresql://user:password@host:port/db"
        DATABASE_DB1_TYPE="sql"
        DATABASE_DB1_SOME_OTHER_KEY="some_other_value"
        ```

        The function will return the following dictionary:
        ```python
        >>> self._get_database_metadata()
        {
            "db1": {
                "uri": "postgresql://user:password@host:port/db",
                "local_uri": "/data/db1",
                "is_file": false,
                "is_dir": false,
                "type": "sql",
                "env": {
                    "some_other_key": "some_other_value",
                }
            }
        }
        ```
        """
        db_labels = [
            label.strip()
            for label in os.environ.get("DATABASE_LABELS", "").split(",")
            if label.strip()
        ]
        if not db_labels:
            self.log.warning("No databases found in the environment variables")
            return {}

        databases = {}
        for label in db_labels:
            env_label = database_env_label(label)
            # The URI on the host system. This can either be a path to a file or folder
            # or an address to a service.
            uri_env_var = f"DATABASE_{env_label}_URI"
            uri = os.environ.get(uri_env_var, "")

            db_type_env_var = f"DATABASE_{env_label}_TYPE"
            db_type = os.environ.get(db_type_env_var, "")
            # In case we are dealing with a file or directory and when running the node
            # instance in a POD, our internal path to that file or folder is different.
            # At this point we are not sure what the type of database is so we just
            # see wether it exists or not. In case it does, we create a `local_uri` that
            # points to the file or folder in the POD. We keep the original URI as we
            # need it when we are creating new volume mounts for the algorithm
            # containers
            local_uri = uri
            # Note that this path is specified in the helm chart so that we should be
            # able to find the database here.
            tmp_uri = Path(DATABASE_BASE_PATH) / f"{label}.{db_type}"

            db_is_file = tmp_uri.exists() and tmp_uri.is_file()
            db_is_dir = tmp_uri.exists() and tmp_uri.is_dir()

            if db_is_file or db_is_dir:
                local_uri = str(tmp_uri)

            # Get additional environment variables and remove the DATABASE_[LABEL]_
            # prefix
            env = {}
            for key in os.environ:
                if key.startswith(f"DATABASE_{env_label}_") and key not in [
                    uri_env_var,
                    db_type_env_var,
                ]:
                    env[key.replace(f"DATABASE_{env_label}_", "")] = os.environ[key]

            databases[label] = TaskDB(
                label=label,
                uri=uri,
                local_uri=local_uri,
                is_file=db_is_file,
                is_dir=db_is_dir,
                type=db_type,
                env=env,
            )

        self.log.debug("Databases: %s", databases)
        return databases

    def run(
        self,
        run_id: int,
        task_info: dict,
        image: str,
        function_arguments: bytes,
        session_id: int,
        token: str,
        databases_to_use: list[dict],
        action: AlgorithmStepType,
    ) -> RunStatus:
        """
        Run a vantage6 algorithm on the Kubernetes cluster.

        Parameters
        ----------
        run_id: int
            Algorithm run ID
        task_info: dict
            Dictionary with task information *** Includes parent-algorithm id
        image: str
            Docker image name
        function_arguments: bytes
            Base64-encoded arguments that can be read by the function
        session_id: int
            ID of the session
        token: str
            Bearer token that the container can use
        databases_to_use: list[dict]
            Metadata of the databases to use.
        action: AlgorithmStepType
            The action to perform

        Returns
        -------
        RunStatus
            Returns the status of the run
        """
        init_org_id = task_info["init_org"]["id"]
        self.log.debug(
            "[Algorithm run %s - requested by org %s] Setting up algorithm run",
            run_id,
            init_org_id,
        )
        # In case we are dealing with a data-extraction or prediction task, we need to
        # know the dataframe that is being created or modified by the algorithm.
        df_details = task_info.get("dataframe", {})

        # Verify that an allowed image is used
        if not self.is_image_allowed(image, task_info):
            self.log.critical(
                "[Algorithm run %s requested by org %s] Docker image %s is not allowed "
                "on this Node!",
                run_id,
                init_org_id,
                image,
            )
            return RunStatus.NOT_ALLOWED

        # Check that this task is not already running, in any (older or newer)
        # generation
        if self.is_run_active(run_id):
            self.log.warning(
                "[Algorithm run %s requested by org %s] Task is already being "
                "executed, discarding task",
                run_id,
                init_org_id,
            )
            return RunStatus.ACTIVE

        # Each execution attempt gets its own generation tag. Resources are named
        # and labelled with it, so a delayed cleanup of an old generation can never
        # remove resources of a newer one.
        attempt_tag = self.registry.next_attempt_tag()

        run_io = RunIO(
            run_id,
            session_id,
            action,
            self.client,
            df_details,
            task_dir_extension=self._task_dir_extension,
            attempt_tag=attempt_tag,
        )

        task_id = task_info["id"]
        parent_task_id = get_parent_id(task_info)

        try:
            _volumes, _volume_mounts, env_vars, secrets = self._create_volume_mounts(
                run_io=run_io,
                function_arguments=function_arguments,
                databases_to_use=databases_to_use,
            )
        except PermanentAlgorithmStartFail as e:
            self.log.warning(e)
            return RunStatus.FAILED
        except DataFrameNotFound as e:
            self.log.info(e)
            return RunStatus.DATAFRAME_NOT_FOUND
        except Exception:
            self.log.exception("Unknown error in creating volume mounts")
            return RunStatus.UNKNOWN_ERROR

        # Set environment variables for the algorithm client. This client is used
        # to communicate from the algorithm to HQ through the proxy.
        # The V6_PROXY_HOST env. variable is assumed to be set at this
        # point (no need to check here again), as its presence is validated when the
        # node is initialized (node/__init__.py)
        env_vars[ContainerEnvNames.HOST.value] = os.environ.get("V6_PROXY_HOST")
        env_vars[ContainerEnvNames.PORT.value] = os.environ.get(
            "V6_PROXY_PORT", str(DEFAULT_PROXY_PORT)
        )

        if action == AlgorithmStepType.CENTRAL_COMPUTE:
            secrets[ContainerEnvNames.CONTAINER_TOKEN.value] = token

        self.log.debug(
            "[Algorithm run %s] Setting V6_PROXY_HOST=%s and V6_PROXY_PORT=%s env "
            "variables on the algorithm job pod",
            run_id,
            env_vars[ContainerEnvNames.HOST.value],
            env_vars[ContainerEnvNames.PORT.value],
        )

        env_vars[ContainerEnvNames.API_PATH.value] = ""

        env_vars[ContainerEnvNames.ALGORITHM_METHOD.value] = task_info["method"]
        env_vars[ContainerEnvNames.FUNCTION_ACTION.value] = action.value
        env_vars[ContainerEnvNames.TASK_ID.value] = str(task_id)
        env_vars[ContainerEnvNames.NODE_ID.value] = str(self.client.whoami.id_)
        env_vars[ContainerEnvNames.ORGANIZATION_ID.value] = str(
            self.client.whoami.organization_id
        )
        env_vars[ContainerEnvNames.COLLABORATION_ID.value] = str(
            self.client.collaboration_id
        )

        for key, value in self.ctx.config.get("algorithm_env", {}).items():
            if key in env_vars or key.upper() in env_vars:
                self.log.warning(
                    "[Algorithm run %s requested by org %s] Environment variable %s"
                    " is requested to be set as algorithm environment variable, but it "
                    "vantage6 infrastructure is already setting it.",
                    run_id,
                    init_org_id,
                    key,
                )
                self.log.warning(
                    "This will result in the algorithm not receiving the correct value,"
                    " which may result in unexpected behavior.",
                )
                continue
            env_vars[key] = value

        # Encode the environment variables to avoid issues with special characters and
        # for security reasons. The environment variables are encoded in base64.
        io_env_vars = []
        env_vars = self._encode_environment_variables(env_vars)
        for key, value in env_vars.items():
            io_env_vars.append(k8s_client.V1EnvVar(name=key, value=value))
        try:
            self._validate_environment_variables(env_vars)
            self._validate_environment_variables(secrets)
        except PermanentAlgorithmStartFail as e:
            self.log.warning(
                "[Algorithm run %s requested by org %s] Validation of environment "
                "variables failed: %s",
                run_id,
                init_org_id,
                e,
            )
            return RunStatus.FAILED

        if secrets:
            secret = k8s_client.V1Secret(
                metadata=k8s_client.V1ObjectMeta(name=run_io.container_name),
                string_data=secrets,
            )
            self.core_api.create_namespaced_secret(
                namespace=self.task_namespace, body=secret
            )

        # Create a Docker login secret for private Docker registries if any are defined.
        docker_login_secret = None
        if priv_regs := self.ctx.config.get("private_docker_registries"):
            docker_login_secret = self._create_docker_login_secret(
                priv_regs, image, run_io.docker_login_secret_name
            )

        container = k8s_client.V1Container(
            name=run_io.container_name,
            image=image,
            image_pull_policy=self._get_image_pull_policy(),
            tty=True,
            volume_mounts=_volume_mounts,
            env=io_env_vars,
            env_from=(
                [
                    k8s_client.V1EnvFromSource(
                        secret_ref=k8s_client.V1SecretReference(
                            name=run_io.container_name
                        )
                    )
                ]
                if secrets
                else []
            ),
        )

        task_job_labels = {
            **self.task_job_labels,
            LABEL_RUN_ID: str(run_io.run_id),
            LABEL_ATTEMPT_TAG: attempt_tag,
        }
        task_job_metadata = k8s_client.V1ObjectMeta(
            name=run_io.container_name,
            annotations={
                "run_id": str(run_io.run_id),
                "task_id": str(task_id),
                "task_parent_id": str(parent_task_id) if parent_task_id else "",
                "init_org_id": str(init_org_id),
                "action": action.value,
                "session_id": str(session_id),
                "df_name": df_details.get("name") if df_details else "",
                "df_id": str(df_details.get("id")) if df_details else "",
                "df_label": df_details.get("label") if df_details else "",
                ANN_ATTEMPT_TAG: attempt_tag,
                ANN_DOCKER_LOGIN_SECRET: (run_io.docker_login_secret_name),
            },
            labels=task_job_labels,
        )

        image_pull_secrets = None
        if docker_login_secret:
            image_pull_secrets = [
                k8s_client.V1LocalObjectReference(
                    name=docker_login_secret.metadata.name
                )
            ]

        # Define the job
        task_job = k8s_client.V1Job(
            api_version="batch/v1",
            kind="Job",
            metadata=task_job_metadata,
            spec=k8s_client.V1JobSpec(
                template=k8s_client.V1PodTemplateSpec(
                    metadata=k8s_client.V1ObjectMeta(
                        labels={
                            **task_job_labels,
                            "app": run_io.container_name,
                            "role": action,
                        }
                    ),
                    spec=k8s_client.V1PodSpec(
                        containers=[container],
                        volumes=_volumes,
                        restart_policy="Never",
                        image_pull_secrets=image_pull_secrets,
                    ),
                ),
                backoff_limit=TASK_START_RETRIES,
            ),
        )

        self.log.info(
            "[Algorithm run %s requested by org %s] Creating namespaced K8S job for"
            " task_id=%s (attempt=%s).",
            run_id,
            init_org_id,
            task_id,
            attempt_tag,
        )
        try:
            self.batch_api.create_namespaced_job(
                namespace=self.task_namespace, body=task_job
            )
        except Exception:
            # Make sure no partially created resources of this generation survive a
            # failed Job creation, then let the caller mark the run as failed.
            self.log.exception(
                "Failed to create Job %s for run_id=%s",
                run_io.container_name,
                run_id,
            )
            self._enqueue_cleanup_from_names(
                run_id=run_id,
                attempt_tag=attempt_tag,
                job_name=run_io.container_name,
                docker_secret_name=(
                    docker_login_secret.metadata.name if docker_login_secret else None
                ),
                reason=CLEANUP_REASON_RECONCILE,
            )
            raise

        # Register this generation only after the Job exists. The registry entry is
        # the anchor to which kill requests and cleanup tombstones are bound.
        self.registry.activate(
            ActiveRun(
                run_id=run_id,
                attempt_tag=attempt_tag,
                job_name=run_io.container_name,
                task_id=task_id,
                parent_id=parent_task_id,
                init_org_id=init_org_id,
            )
        )

        # Kubernetes will automatically retry the job ``backoff_limit`` times. According
        # to the Pod's backoff failure policy, it will have a failed status only after
        # the last failed retry.

        # Wait until the POD is running. The following method is blocking until the POD
        # is running or
        # TODO we could make this non-blocking by keeping track of the started jobs
        #     and checking their status in a separate thread
        # TODO in the previous `DockerTaskManager` a few checks where performed to
        #      tackle any issues with the prerequisites. For example it checked wether
        #      the dataframe requested by the user was available. We should re-implement
        #      these checks here.

        # stackoverflow.com/questions/57563359/how-to-properly-update-the-status-of-a-job
        # kubernetes.io/docs/concepts/workloads/controllers/job/#pod-backoff-failure-
        # policy

        status = self.__wait_until_pod_running(
            run_io=run_io,
            label=f"app={run_io.container_name}",
        )

        # start streaming logs to HQ
        self._stream_logs(run_io=run_io, task_id=task_id)

        self.num_active_tasks += 1

        return status

    def _create_docker_login_secret(
        self, priv_regs: list[dict], image: str, secret_name: str
    ) -> k8s_client.V1Secret | None:
        """
        Create a Docker login secret.
        """

        dockerconfig = {
            "auths": {
                reg["registry"]: {
                    "username": reg["username"],
                    "password": reg["password"],
                    "auth": base64.b64encode(
                        f"{reg['username']}:{reg['password']}".encode()
                    ).decode(),
                }
                for reg in priv_regs
            }
        }

        secret = k8s_client.V1Secret(
            metadata=k8s_client.V1ObjectMeta(
                name=secret_name,
                namespace=self.task_namespace,
            ),
            type="kubernetes.io/dockerconfigjson",
            string_data={".dockerconfigjson": json.dumps(dockerconfig)},
        )

        try:
            self.core_api.create_namespaced_secret(
                namespace=self.task_namespace, body=secret
            )
        except Exception as exc:  # noqa: BLE001
            self.log.error(
                f"Error creating Docker login secret for image {image}: {exc}"
            )
            return None
        return secret

    def _stream_logs(self, run_io: RunIO, task_id: int) -> None:
        """
        Stream logs to HQ.

        Parameters
        ----------
        run_io: RunIO
            RunIO object that contains information about the run
        task_id: int
            Task ID
        """
        self.log.info(f"Starting thread to stream logs for run {run_io.run_id}")
        thread = threading.Thread(target=self.__log_stream, args=(run_io, task_id))
        thread.start()

    def __log_stream(self, run_io: RunIO, task_id: int) -> None:
        """
        Stream logs to HQ.

        Parameters
        ----------
        run_io: RunIO
            RunIO object that contains information about the run
        task_id: int
            Task ID
        """
        if not self.share_algorithm_logs:
            self.log.info(
                "Algorithm logs will not be shared with HQ for run %s because "
                "share_algorithm_logs is disabled.",
                run_io.run_id,
            )
            return

        w = watch.Watch()
        try:
            pod_list = self.core_api.list_namespaced_pod(
                namespace=self.task_namespace,
                label_selector=f"app={run_io.container_name}",
            ).items
            if not len(pod_list):
                self.log.error(f"No POD found for run {run_io.run_id}")
                return

            pod = pod_list[0]
            for log_line in w.stream(
                func=self.core_api.read_namespaced_pod_log,
                name=pod.metadata.name,
                namespace=self.task_namespace,
            ):
                self.socket_io.emit(
                    "algorithm_log",
                    {
                        "collaboration_id": self.client.collaboration_id,
                        "run_id": run_io.run_id,
                        "task_id": task_id,
                        "log": log_line,
                    },
                    namespace="/tasks",
                )
        except Exception as e:  # noqa: BLE001
            self.log.error(f"Error while streaming logs for run {run_io.run_id}: {e}")
        finally:
            w.stop()

    def __wait_until_pod_running(self, run_io: RunIO, label: str) -> RunStatus:
        """"
        This method monitors the status of a Kubernetes POD created by a task job and
        waits until it transitions to a 'Running' state or another terminal state
        (e.g., 'Failed', 'Succeeded') for up to K8S_EVENT_STREAM_LOOP_TIMEOUT seconds.
        After the timeout, this method will have additional status-event waiting windows
        only if the reason for such timeout is an (large) image that is already being
        pulled.

                               - Succeeded
                              /
        Pending - Running ------ Failed
                              \
                               - Unknown

        Parameters
        ----------
        run_io: RunIO
            RunIO object that contains information about the run
        label : str
            Label selector to identify the POD associated with the task job.

        Returns
        -------
        RunStatus
            The status of the run:
            - RunStatus.ACTIVE: If the pod is already with a Running status
            - RunStatus.FAILED: If the pod reported a Failed status
            - RunStatus.NO_DOCKER_IMAGE: If the pod is pending due to a missing or
              problematic Docker image.
            - RunStatus.CRASHED: If the pod is still in pending status but has reported
              a runtime crash.
            - RunStatus.UNKNOWN_ERROR: If the pod is in an unexpected or unknown state.
            - RunStatus.COMPLETED: Not expected to happen but still possible: the task
              pod is reported as Succeded shortly after being created.
        """

        # Wait until the POD is created
        w = watch.Watch()

        try:
            while True:
                # Non-busy waiting for status changes on the task pod through K8S' event
                # stream watch. This inner for loop will process events until a terminal
                # status is identifed (Running, Failed, Succeded) or timeout_seconds is
                # reached.
                #
                # Given that after the timeout the status would be uncertain, the task
                # pod status is checked (directly) again to decide whether a new
                # stream-watch iteration is performed (e.g, an image may be too large),
                # or an error is reported.
                for event in w.stream(
                    func=self.core_api.list_namespaced_pod,
                    namespace=self.task_namespace,
                    label_selector=label,
                    timeout_seconds=K8S_EVENT_STREAM_LOOP_TIMEOUT,
                ):
                    pod = event["object"]

                    pod_phase: RunStatus = compute_run_pod_status(
                        pod=pod,
                        label=label,
                        log=self.log,
                        task_namespace=self.task_namespace,
                    )

                    if pod_phase != RunStatus.INITIALIZING:
                        return pod_phase

                # POD event-watch TIMEOUT (timeout_seconds) was reached.
                self.log.debug(
                    "Task run (label %s, namespace %s) event stream timeout reached. "
                    "Checking the cause...",
                    label,
                    self.task_namespace,
                )

                # Getting the POD again to check its post-timeout status
                pod_list = self.core_api.list_namespaced_pod(
                    namespace=self.task_namespace, label_selector=label
                ).items
                if not pod_list or len(pod_list) == 0:
                    self.log.error(f"No POD found for run {run_io.run_id}")
                    return RunStatus.UNKNOWN_ERROR
                pod = pod_list[0]

                pod_phase: RunStatus = compute_run_pod_status(
                    pod=pod,
                    label=label,
                    log=self.log,
                    task_namespace=self.task_namespace,
                )

                # Another iteration on the outer loop is performed if the pod is
                # pending for reasons other than missing/invalid Docker image (which is
                # reported as INITIALIZING).
                if pod_phase != RunStatus.INITIALIZING:
                    return pod_phase

                self.log.debug(
                    "Task run (label %s, namespace %s) still pulling the algorithm "
                    "image. Waiting again for a new status update...",
                    label,
                    self.task_namespace,
                )

        finally:
            w.stop()

    def _create_run_mount(
        self,
        volume_name: str,
        host_path: str | Path,
        mount_path: str,
        type_: str | None = None,
        read_only: bool = False,
    ) -> tuple[k8s_client.V1Volume, k8s_client.V1VolumeMount]:
        """
        Create a volume and its corresponding volume mount

        Parameters
        ----------
        volume_name: str
            Name of the volume
        host_path: str | Path
            Path to the host, could be a file or a folder
        mount_path: str
            Path where the ``host_path`` is going to be mounted
        type_: str, optional
            Type of the volume
        read_only: bool
            Whether the volume is read-only or not in the mount

        Returns
        -------
        V1Volume, V1VolumeMount
            Tuple with the volume and volume mount
        """
        host_path = str(host_path)

        volume = k8s_client.V1Volume(
            name=volume_name,
            host_path=k8s_client.V1HostPathVolumeSource(path=host_path, type=type_),
        )

        vol_mount = k8s_client.V1VolumeMount(
            name=volume_name,
            mount_path=mount_path,
            read_only=read_only,
        )

        return (volume, vol_mount)

    def _create_volume_mounts(
        self,
        run_io: RunIO,
        function_arguments: bytes,
        databases_to_use: list[dict],
    ) -> tuple[
        list[k8s_client.V1Volume],
        list[k8s_client.V1VolumeMount],
        dict[str, str],
        dict[str, str],
    ]:
        """
        Create all volumes and volume mounts required by the algorithm run.

        Parameters
        ----------
        run_io: RunIO
            RunIO object that contains information about the run
        function_arguments: bytes
            Base64-encoded arguments that can be read by the function
        databases_to_use: list[dict]
            Metadata of the databases to use.

        Returns
        -------
        Tuple[
            list[client.V1Volume],
            list[client.V1VolumeMount],
            dict[str, str],
            dict[str, str],
        ]
            a tuple with (1) the created volume names and (2) their corresponding volume
            mounts and (3) the list of the environment variables required by the
            algorithms to use such mounts and (4) the secrets to be passed to the
            container.

        Notes
        -----
        In this method volume-claims could be used instead of 'host_path' volumes to
        decouple vantage6 file management from the storage provider (NFS, GCP, etc).
        However, persistent-volumes (from which volume-claims are be created), present
        a risk when used on local file systems. In particular, if two VC are created
        from the same PV, both would end sharing the same files. e.g. define the volume
        for temporal data:

        ```python
        tmp_volume = client.V1Volume(
            name=f'task-{str_run_id}-tmp',
            persistent_volume_claim=client.V1PersistentVolumeClaimVolumeSource(
                claim_name=tmp_vol_name
            )
        )
        ```
        """
        volumes: list[k8s_client.V1Volume] = []
        vol_mounts: list[k8s_client.V1VolumeMount] = []

        # Create algorithm's input and output files before creating volume mounts with
        # them (relative to the node's file system: task pod or host)
        input_file_path, output_file_path = run_io.create_files(function_arguments, b"")

        # Create the volumes and corresponding volume mounts for the input and output
        # files.
        output_volume, output_mount = self._create_run_mount(
            volume_name=run_io.output_volume_name,
            host_path=Path(self.host_data_dir) / output_file_path,
            mount_path=TASK_JOB_OUTPUT_PATH,
            type_="File",
            read_only=False,
        )

        input_volume, input_mount = self._create_run_mount(
            volume_name=run_io.input_volume_name,
            host_path=Path(self.host_data_dir) / input_file_path,
            type_="File",
            mount_path=TASK_JOB_INPUT_PATH,
        )

        session_volume, session_mount = self._create_run_mount(
            volume_name=run_io.session_name,
            host_path=Path(self.host_data_dir)
            / run_io.session_file_manager.session_folder,
            type_="Directory",
            mount_path=TASK_JOB_SESSION_FOLDER_PATH,
        )

        volumes.extend([output_volume, input_volume, session_volume])
        vol_mounts.extend([output_mount, input_mount, session_mount])

        # The environment variables are expected by the algorithm containers in order
        # to access the input and output files.
        environment_variables = {
            ContainerEnvNames.OUTPUT_FILE.value: TASK_JOB_OUTPUT_PATH,
            ContainerEnvNames.INPUT_FILE.value: TASK_JOB_INPUT_PATH,
            # TODO we only do not need to pass this when the action is `data extraction`
            ContainerEnvNames.SESSION_FOLDER.value: TASK_JOB_SESSION_FOLDER_PATH,
            ContainerEnvNames.SESSION_FILE.value: os.path.join(
                TASK_JOB_SESSION_FOLDER_PATH,
                run_io.session_file_manager.session_state_file_name,
            ),
        }

        # Some environment variables should be passed as secrets because they are
        # sensitive.
        secrets = {}

        # Bind-mounting all the CSV files (read only) defined on the configuration file
        # TODO bind other input data types
        # TODO include only the ones given in the 'databases_to_use parameter
        # TODO distinguish between the different actions
        if run_io.action == AlgorithmStepType.DATA_EXTRACTION:
            # In case we are dealing with a file based database, we need to create an
            # additional volume mount for the database file. In case it is an URI the
            # URI should be reachable from the container.
            self._validate_source_database(databases_to_use)
            # A always has 1 source database to use in the extraction step. This
            # is validated in the previous method.
            source_database = databases_to_use[0]
            db: TaskDB = self.databases[source_database["label"]]
            if db.is_file or db.is_dir:
                if db.mount_mode == "ro":
                    self.log.info(
                        f"Binding database {source_database['label']} as read-only"
                    )
                else:
                    self.log.info(
                        f"Binding database {source_database['label']} as read-write"
                    )
                db_volume, db_mount = self._create_run_mount(
                    volume_name=f"task-{run_io.run_id}-db-{source_database['label']}",
                    host_path=db.uri,
                    mount_path=db.local_uri,
                    type_="File" if db.is_file else "Directory",
                    read_only=db.mount_mode == "ro",
                )
                volumes.append(db_volume)
                vol_mounts.append(db_mount)

            if db.mount_mode == "ro" and not db.is_file and not db.is_dir:
                self.log.error(
                    f"Database {source_database['label']} is of type {db.type} but "
                    "mount_mode is 'ro'. This is not supported."
                )
                raise PermanentAlgorithmStartFail(
                    f"Database {source_database['label']} is of type {db.type} but "
                    "mount_mode is 'ro'. This is not supported."
                )

            environment_variables[ContainerEnvNames.DATABASE_URI.value] = db.local_uri
            environment_variables[ContainerEnvNames.DATABASE_TYPE.value] = db.type

            # additional environment variables for the database. These will be stored
            # as {PREFIX}{KEY}=value in the container. These are read by the
            # `@source_database` decorator in the (data extraction) algorithm.
            if db.env:
                for key in db.env:
                    env_key = f"{ContainerEnvNames.DB_PARAM_PREFIX.value}{key.upper()}"
                    secrets[env_key] = db.env[key]

        else:
            # In the other cases (preprocessing, compute, ...) we are dealing with a
            # dataframe in a session. So we only need to validate that the dataframe is
            # available in the session.
            self._validate_dataframes(databases_to_use, run_io)

            # group and sort on the position of the database in the argument list
            databases_to_use_sorted = sorted(
                databases_to_use, key=lambda x: x["position"]
            )
            grouped_databases = groupby(
                databases_to_use_sorted, key=lambda x: x["position"]
            )

            # # Groups are separated by ';' and the dataframes are separated by ','
            # # so we need to join the dataframes and the groups
            environment_variables[ContainerEnvNames.USER_REQUESTED_DATAFRAMES.value] = (
                DATAFRAME_BETWEEN_GROUPS_SEPARATOR.join(
                    [
                        DATAFRAME_WITHIN_GROUP_SEPARATOR.join(
                            [db["dataframe_name"] for db in db_group]
                        )
                        for _, db_group in grouped_databases
                    ]
                )
            )

        return volumes, vol_mounts, environment_variables, secrets

    def _validate_environment_variables(self, environment_variables: dict) -> None:
        """
        Check whether environment variables don't contain any illegal
        characters

        Parameters
        ----------
        environment_variables: dict
            Environment variables required to run algorithm

        Raises
        ------
        PermanentAlgorithmStartFail
            If environment variables contain illegal characters
        """
        msg = None
        for key in environment_variables:
            if not key.isidentifier():
                msg = (
                    f"Environment variable '{key}' is invalid: environment "
                    " variable names should only contain number, letters and "
                    " underscores, and start with a letter."
                )
            elif key in ENV_VARS_NOT_SETTABLE_BY_NODE:
                msg = (
                    f"Environment variable '{key}' cannot be set: this "
                    "variable is set in the algorithm Dockerfile and cannot "
                    "be overwritten."
                )
            if msg:
                raise PermanentAlgorithmStartFail(msg)

    def _validate_dataframes(self, databases_to_use: list[dict], run_io: RunIO) -> None:
        """
        Validates that all provided databases are of type 'dataframe' and that the
        requested names exist in the session folder.

        Parameters
        ----------
        databases_to_use: list[dict]
            A list of dictionaries where each dictionary represents a database
            with a 'type' and 'label'.
        run_io: RunIO
            RunIO object that contains information about the run

        Raises
        ------
        PermanentAlgorithmStartFail:
            If any database is not of type 'dataframe'.
        DataFrameNotFound:
            If any requested df name is not found in the session folder.
        """

        if not all(df["type"] == TaskDatabaseType.DATAFRAME for df in databases_to_use):
            self.log.error(
                "All databases used in the algorithm must be of type '%s'.",
                TaskDatabaseType.DATAFRAME,
            )
            raise PermanentAlgorithmStartFail()

        # Validate that the requested dataframes exist. At this point they need to as
        # we are about to start the task.
        requested_dataframes = {db["dataframe_name"] for db in databases_to_use}
        available_dataframes = {
            file_.stem
            for file_ in Path(run_io.session_file_manager.local_session_folder).glob(
                "*.parquet"
            )
            if file_.stem != SESSION_STATE_FILENAME
        }
        # check that requested dataframes are a subset of available dataframes
        if requested_dataframes and not requested_dataframes.issubset(
            available_dataframes
        ):
            self.log.error("Requested dataframe(s) not found in session folder.")
            self.log.debug("Requested dataframes: %s", requested_dataframes)
            self.log.debug("Available dataframes: %s", available_dataframes)
            problematic_dfs = requested_dataframes - available_dataframes
            raise DataFrameNotFound(
                f"User requested dataframes '{problematic_dfs}' which are not available"
                " in the session folder. Available dataframes are: "
                f"{available_dataframes}."
            )

    def _validate_source_database(self, databases_to_use: list[dict]) -> None:
        """
        Validates the source database configuration for the data extraction step.
        This method checks if the provided list of databases contains exactly one
        database, and if that database is of type 'source' and exists in the
        available databases.

        Parameters
        ----------
        databases_to_use : list[dict]
            A list of dictionaries where each dictionary represents a database
            configuration.

        Raises
        ------
        PermanentAlgorithmStartFail
            If the validation fails due to any of the conditions not being met.
        """

        ok = True
        if len(databases_to_use) > 1:
            self.log.error("Only one database can be used in the data extraction step.")
            ok = False

        if len(databases_to_use) == 0:
            self.log.error("No database is provided for the data extraction step.")
            ok = False

        source_database = databases_to_use[0]

        if source_database["type"] != TaskDatabaseType.SOURCE:
            self.log.error(
                "The database used in the data extraction step must be of type '%s'.",
                TaskDatabaseType.SOURCE,
            )
            ok = False

        if source_database["label"] not in self.databases:
            self.log.error(
                "The database used in the data extraction step does not exist."
            )
            ok = False

        if not ok:
            raise PermanentAlgorithmStartFail()

    def is_image_allowed(self, evaluated_img: str, task_info: dict) -> bool:
        """
        Checks that the (container) image name is allowed.

        Against a list of regular expressions as defined in the configuration file. If
        no expressions are defined, all container images are accepted.

        Parameters
        ----------
        evaluated_img: str
            URI of the image of which we are checking if it is allowed
        task_info: dict
            Dictionary with information about the task

        Returns
        -------
        bool
            Whether the image is allowed or not
        """
        # check if algorithm matches any of the regex cases
        allowed_algorithms = self._policies.get(NodePolicy.ALLOWED_ALGORITHMS.value)
        allowed_stores = self._policies.get(NodePolicy.ALLOWED_ALGORITHM_STORES.value)
        allow_either_whitelist_or_store = self._policies.get(
            "allow_either_whitelist_or_store", False
        )

        # check if user or their organization is allowed
        allowed_users = self._policies.get(NodePolicy.ALLOWED_USERS.value, [])
        allowed_orgs = self._policies.get(NodePolicy.ALLOWED_ORGANIZATIONS.value, [])
        if allowed_users or allowed_orgs:
            is_allowed = self.client.check_user_allowed_to_send_task(
                allowed_users,
                allowed_orgs,
                task_info["init_org"]["id"],
                task_info["init_user"]["id"],
            )
            if not is_allowed:
                self.log.warning(
                    "A task was sent by a user or organization that this node does not "
                    "allow to start tasks."
                )
                return False

        algorithm_whitelisted = False
        if allowed_algorithms:
            if isinstance(allowed_algorithms, str):
                allowed_algorithms = [allowed_algorithms]
            try:
                evaluated_img_wo_tag = get_image_name_wo_tag(evaluated_img)
            except Exception as exc:  # noqa: BLE001
                self.log.warning(
                    "Could not parse image with name %s: %s",
                    evaluated_img,
                    exc,
                )
                evaluated_img_wo_tag = None
            for allowed_algo in allowed_algorithms:
                if not self._is_regex_pattern(allowed_algo):
                    try:
                        allowed_wo_tag = get_image_name_wo_tag(allowed_algo)
                    except Exception as exc:  # noqa: BLE001
                        self.log.warning(
                            "Could not parse allowed_algorithm policy with name %s: %s",
                            allowed_algo,
                            exc,
                        )
                        self.log.warning("Skipping policy as it cannot be parsed")
                        continue  # skip policy as it cannot be parsed
                    if allowed_algo == evaluated_img:
                        # OK if allowed algorithm and provided algorithm match exactly
                        algorithm_whitelisted = True
                        break
                    elif allowed_algo == evaluated_img_wo_tag:
                        # OK if allowed algorithm is an image name without a tag, and
                        # the provided image is the same but includes extra tag
                        algorithm_whitelisted = True
                        break
                    elif allowed_wo_tag == evaluated_img_wo_tag:
                        # The allowed image and the evaluated image are indeed the same
                        # image but the allowed image only allows certain tags or sha's.
                        # Gather the digests of the images and compare them - if they
                        # are the same, the image is allowed.
                        # Note that by comparing the digests, we also take into account
                        # the situation where e.g. the allowed image has a tag, but the
                        # evaluated image has a sha256.
                        digest_evaluated_image = get_digest(evaluated_img)
                        if not digest_evaluated_image:
                            self.log.warning(
                                "Could not obtain digest for image %s",
                                evaluated_img,
                            )
                        digest_policy_image = get_digest(allowed_algo)
                        if not digest_policy_image:
                            self.log.warning(
                                "Could not obtain digest for image %s", allowed_algo
                            )
                        if (
                            digest_evaluated_image
                            and digest_policy_image
                            and (digest_evaluated_image == digest_policy_image)
                        ):
                            algorithm_whitelisted = True
                            break
                else:
                    expr_ = re.compile(allowed_algo)
                    if expr_.match(evaluated_img):
                        algorithm_whitelisted = True

        store_whitelisted = False
        if allowed_stores:
            # get the store from the task_info
            try:
                store_id = task_info["algorithm_store"]["id"]
            except Exception:  # noqa: BLE001
                store_id = None
            if store_id:
                store_info = self.client.algorithm_store.get(store_id)
                store_from_task = store_info["url"]
                # check if the store matches any of the regex cases
                if isinstance(allowed_stores, str):
                    allowed_stores = [allowed_stores]
                store_url_whitelisted = False
                for allowed_store in allowed_stores:
                    if not self._is_regex_pattern(allowed_store):
                        # check if string matches exactly
                        if allowed_store == store_from_task:
                            store_url_whitelisted = True
                    else:
                        expr_ = re.compile(allowed_store)
                        if expr_.match(store_from_task):
                            store_url_whitelisted = True

                if store_url_whitelisted:
                    # The store's URL is whitelisted, but that alone does not prove
                    # that the evaluated image is actually a registered, approved
                    # algorithm in that store - HQ is the one asserting that
                    # association, and HQ is not fully trusted here. Re-verify
                    # directly with the store itself instead of trusting HQ's claim.
                    #
                    # If the store is registered under a `localhost` URL (dev env)
                    # translate it to an address reachable from inside this pod
                    reachable_store_url = replace_localhost_for_k8s(
                        store_info["url"], os.environ.get("V6_K8S_NODE_NAME") or None
                    )
                    self.client.algorithm_store.url = (
                        f"{reachable_store_url}{store_info['api_path']}"
                    )
                    self.client.algorithm_store.store_id = store_id
                    algorithm = self.client.algorithm_store.get_algorithm(evaluated_img)
                    if algorithm:
                        store_whitelisted = True
                    else:
                        self.log.warning(
                            "Algorithm store %s did not confirm that image %s is a "
                            "registered, approved algorithm there. Denying the "
                            "allowed_algorithm_stores policy for this image. This "
                            "can also happen if the algorithm store does not yet "
                            "support node verification requests.",
                            store_from_task,
                            evaluated_img,
                        )

        allowed_from_whitelist = not allowed_algorithms or algorithm_whitelisted
        allowed_from_store = not allowed_stores or store_whitelisted
        if allow_either_whitelist_or_store:
            # if we allow an algorithm if it is defined in the whitelist or the store,
            # we return True if either the algorithm or the store is whitelisted
            allowed = allowed_from_whitelist or allowed_from_store
        else:
            # only allow algorithm if it is allowed for both the allowed_algorithms and
            # the allowed_algorithm_stores
            allowed = allowed_from_whitelist and allowed_from_store

        if not allowed:
            self.log.warning(
                "This node does not allow the algorithm %s to run!", evaluated_img
            )

        return allowed

    @staticmethod
    def _is_regex_pattern(pattern: str) -> bool:
        """
        Check if a string just a string or if it is a regex pattern. Note that there is
        no failsafe way to do this so we make a best effort.

        Note, for instance, that if a user provides the allowed algorithm "some.name",
        we will interpret this as a regular string. This prevents that "someXname" is
        allowed as well. The user is thus not able to define a regex pattern with only
        a dot as special character. However we expect that this use case is extremely
        rare - not doing so is likely to lead to regex's that lead to unintended
        algorithms passing the filter criteria.

        Parameters
        ----------
        pattern: str
            String to be checked

        Returns
        -------
        bool
            Returns False if the pattern is a normal string, True if it is a regex.
        """
        # Inspired by
        # https://github.com/corydolphin/flask-cors/blob/a5003f391e56f74f11a3e509cd180787c75eb6b0/flask_cors/core.py#L266
        common_regex_chars = [
            "*",
            "\\",
            "?",
            "$",
            "^",
            "[",
            "]",
            "(",
            ")",
            "{",
            "}",
            "|",
            "+",
        ]
        # Use common characters used in regular expressions as a proxy
        # for if this string is in fact a regex.
        return any(c in pattern for c in common_regex_chars)

    def is_run_active(self, run_id: int) -> bool:
        """
        Check whether any generation of a run currently has pods in the cluster.

        Both generation-labelled resources and legacy resources (created before
        generation labels existed) are considered.

        Parameters
        ----------
        run_id: int
            ID of the run

        Returns
        -------
        bool
            Whether an algorithm run is currently active for this run id
        """
        if self.registry.get_active(run_id) is not None:
            return True
        try:
            pods = self.core_api.list_namespaced_pod(
                namespace=self.task_namespace,
                label_selector=(
                    f"{self.task_job_label_selector},{LABEL_RUN_ID}={run_id}"
                ),
            )
            if pods.items:
                return True
            # legacy pods only carry the app label
            legacy_pods = self.core_api.list_namespaced_pod(
                namespace=self.task_namespace,
                label_selector=f"app={APPNAME}-run-{run_id}",
            )
            return bool(legacy_pods.items)
        except ApiException:
            self.log.exception(
                "Could not determine whether run_id=%s is active", run_id
            )
            # fail safe: assume it is active so the run is not started twice
            return True

    def process_next_completed_run(self) -> Result:
        """
        Wait until a task run is completed and process it.

        This method is blocking until a task run is completed. It checks the status of
        the Kubernetes jobs and returns the results of the first completed task run.
        The results are then sent back to HQ.

        Returns
        -------
        Result
            Result of the completed run
        """
        self.log.info("Waiting for a completed run to process")
        # wait until there is at least one completed job available (successful or
        # failed) other statuses (pending/running/unknown) are ignored
        completed_job = False
        while not completed_job:
            # Get all jobs from the task namespace for this node
            jobs = self.batch_api.list_namespaced_job(
                self.task_namespace, label_selector=self.task_job_label_selector
            )
            if not jobs.items:
                time.sleep(1)
                continue

            # Create a list of failed or succeeded jobs
            finished_jobs = [
                job for job in jobs.items if job.status.succeeded or job.status.failed
            ]

            if not finished_jobs:
                time.sleep(1)
                continue

            # Check if any of the jobs is completed
            for job in finished_jobs:
                annotations = job.metadata.annotations
                job_name = job.metadata.name
                # Create helper object to process the output of the job
                run_io = RunIO.from_dict(
                    annotations,
                    self.client,
                    task_dir_extension=self._task_dir_extension,
                )

                # Skip generations whose resources are being (or have been)
                # cleaned up. In particular, a killed Job often reports as failed
                # while it is deleted; its outcome must not be reported.
                if self.registry.get_cleanup(job_name) is not None:
                    self.log.debug(
                        "Skipping finished job %s (run_id=%s): cleanup already "
                        "registered for this generation",
                        job_name,
                        run_io.run_id,
                    )
                    continue
                active = self.registry.get_active(run_io.run_id)
                if active is not None and active.state == ACTIVE_STATE_KILLING:
                    self.log.debug(
                        "Skipping finished job %s (run_id=%s): run is being killed",
                        job_name,
                        run_io.run_id,
                    )
                    continue

                results, status = (
                    run_io.process_output()
                    if job.status.succeeded
                    else (b"", RunStatus.CRASHED)
                )

                logs = self._get_logs(run_io)

                self.log.info(
                    "Sending results of run_id=%s and task_id=%s back to HQ",
                    run_io.run_id,
                    annotations["task_id"],
                )

                parent_id = annotations["task_parent_id"]
                result = Result(
                    run_id=run_io.run_id,
                    task_id=int(annotations["task_id"]),
                    logs=logs,
                    data=results,
                    status=status,
                    parent_id=int(parent_id) if parent_id else None,
                    init_org_id=int(annotations["init_org_id"]),
                    attempt_tag=run_io.attempt_tag,
                )

                self.num_active_tasks -= 1

                self._enqueue_completed_cleanup(run_io)
                completed_job = True
                break

            if not completed_job:
                # Every finished job belongs to a generation being cleaned up;
                # wait for new completions instead of re-processing the same list.
                time.sleep(1)

        return result

    def _get_logs(self, run_io: RunIO) -> str:
        """
        Get the logs generated by the PODs created by a job.
        """
        try:
            logs = self.__get_job_pod_logs(run_io=run_io)
        except Exception as e:  # noqa: BLE001
            self.log.warning(
                f"Error while getting logs of job {run_io.container_name}: {e}"
            )
            logs = ["error while getting logs"]

        # logs are saved as string instead of list[str]
        return "\n".join(logs)

    def __get_job_pod_logs(self, run_io: str) -> list[str]:
        """
        Get the logs generated by the PODs created by a job.

        If there are multiple PODs created by the job (e.g., due to multiple failed
        execution attempts -see backofflimit setting-) all the POD logs are merged as
        one.
        """

        pods_tty_logs = []

        job_selector = f"job-name={run_io.container_name}"
        job_pods_list = self.core_api.list_namespaced_pod(
            self.task_namespace, label_selector=job_selector
        )

        for job_pod in job_pods_list.items:
            self.log.info(
                f"Getting logs from POD {job_pod.metadata.name}, created by job "
                f"{run_io.run_id}"
            )

            pod_log = self.core_api.read_namespaced_pod_log(
                name=job_pod.metadata.name,
                namespace=self.task_namespace,
                _preload_content=True,
            )

            pods_tty_logs.append(
                f"LOGS of POD {job_pod.metadata.name} (created by job "
                f"{run_io.container_name}) \n\n {pod_log} \n\n\n"
            )

        return pods_tty_logs

    def _encode_environment_variables(self, environment_variables: dict) -> dict:
        """
        Encode environment variable values to ensure that special characters
        are not interpretable as code while transferring them to the algorithm
        container.

        Parameters
        ----------
        environment_variables: dict
            Environment variables required to run algorithm

        Returns
        -------
        dict:
            Environment variables with encoded values
        """

        def _encode(string: str) -> str:
            """Encode env var value

            We first encode to bytes, then to b32 and then decode to a string.
            Finally, '=' is replaced by less sensitive characters to prevent
            issues with interpreting the encoded string in the env var value.

            Parameters
            ----------
            string: str
                String to be encoded

            Returns
            -------
            str:
                Encoded string

            Examples
            --------
            >>> _encode("abc")
            'MFRGG!!!'
            """
            return (
                base64.b32encode(string.encode(STRING_ENCODING))
                .decode(STRING_ENCODING)
                .replace("=", ENV_VAR_EQUALS_REPLACEMENT)
            )

        self.log.debug("Encoding environment variables")

        encoded_environment_variables = {}
        for key, val in environment_variables.items():
            encoded_environment_variables[key] = _encode(str(val))
        return encoded_environment_variables

    # ------------------------------------------------------------------
    # cancellation and generation-aware cleanup
    # ------------------------------------------------------------------
    def cleanup(self) -> None:
        """
        Cleanup Kubernetes resources created by the node on shutdown.

        Deletions that cannot be completed now are persisted as cleanup records and
        retried by the reconciler on the next node start.
        """
        try:
            self.kill_for_shutdown()
            self._process_cleanups()
        except Exception:
            self.log.exception("Error while cleaning up on node shutdown")

    def kill_for_shutdown(self) -> list[KilledRun]:
        """Kill every run generation still present because the node is shutting down."""
        self.log.warning(
            "Killing all algorithms running on this node because it is shutting down"
        )
        # Shutdown kills are local housekeeping: HQ is not told KILLED so that the
        # open runs can be re-executed when the node comes back.
        return self._kill_all_generations(
            initiator=KillInitiator.NODE_SHUTDOWN,
            reason=CLEANUP_REASON_SHUTDOWN,
            report_killed=False,
        )

    def kill_algorithm_runs(
        self,
        kill_list: list[ToBeKilled] | None = None,
        event_id: str | None = None,
        issued_at: float | None = None,
    ) -> list[KilledRun]:
        """
        Kill algorithm runs currently running on this node.

        Cancellation is bound to the run generations that are current when this
        instruction is processed. Retried or duplicated cleanup only ever touches
        the pinned generation's resources, never a newer generation.

        Parameters
        ----------
        kill_list: list[ToBeKilled], optional
            Runs to kill. If absent, every run on this node is killed.
        event_id: str, optional
            Id of the kill event, used to drop duplicate deliveries
        issued_at: float, optional
            Unix timestamp of the event, used to drop out-of-order kill-all events

        Returns
        -------
        list[KilledRun]
            Generations that transitioned to KILLED during this call
        """
        if not self.registry.note_kill_event(event_id):
            self.log.warning("Ignoring duplicate kill event %s", event_id)
            return []

        if kill_list:
            kill_list = [
                to_kill
                for to_kill in kill_list
                if to_kill.organization_id == self.client.whoami.organization_id
            ]
            killed_runs: list[KilledRun] = []
            for run_id in {to_kill.run_id for to_kill in kill_list}:
                killed_run = self._kill_run_generation(
                    run_id, KillInitiator.USER, CLEANUP_REASON_KILLED
                )
                if killed_run is not None:
                    killed_runs.append(killed_run)
            if not killed_runs:
                self.log.warning(
                    "Instructed to kill tasks but none were running on this node"
                )
        else:
            if self.registry.is_stale_kill_all(issued_at):
                self.log.warning(
                    "Ignoring stale kill-all instruction (issued_at=%s)", issued_at
                )
                return []
            self.log.warning(
                "Received instruction to kill all algorithms running on this "
                "node. Executing that now..."
            )
            killed_runs = self._kill_all_generations(
                initiator=KillInitiator.USER,
                reason=CLEANUP_REASON_KILLED,
                report_killed=True,
            )
            if not killed_runs:
                self.log.warning(
                    "Instructed to kill all tasks but none were running on this node"
                )

        if killed_runs:
            self.log.warning(
                "Killed the following run ids as instructed: %s",
                ", ".join(str(r.run_id) for r in killed_runs),
            )
        return killed_runs

    def _kill_all_generations(
        self, initiator: KillInitiator, reason: str, report_killed: bool
    ) -> list[KilledRun]:
        """Kill every generation owned by this node that is not already cleaning up."""
        killed_runs: list[KilledRun] = []
        handled_jobs = set(self.registry.cleanup_job_names())

        # generations visible in the cluster
        for job in self._list_our_jobs():
            job_name = job.metadata.name
            if job_name in handled_jobs:
                continue
            killed_run = self._kill_job_object(
                job, initiator=initiator, reason=reason, report_killed=report_killed
            )
            if killed_run is not None:
                killed_runs.append(killed_run)
            handled_jobs.add(job_name)

        # generations the registry knows about but whose Job already disappeared
        for active in self.registry.list_active():
            if active.job_name in handled_jobs:
                continue
            record = self._record_from_active(active, reason)
            killed_run = self._finalize_cleanup_record(
                record, initiator=initiator, report_killed=report_killed
            )
            if killed_run is not None:
                killed_runs.append(killed_run)
            handled_jobs.add(active.job_name)

        return killed_runs

    def _kill_run_generation(
        self, run_id: int, initiator: KillInitiator, reason: str
    ) -> KilledRun | None:
        """
        Kill the current generation of one run.

        Returns a KilledRun exactly when this call transitions a generation to
        KILLED; repeated instructions for an already handled generation return
        None so that the terminal status is reported only once.
        """
        active = self.registry.get_active(run_id)
        if active is not None:
            if (
                active.state == ACTIVE_STATE_KILLING
                or self.registry.get_cleanup(active.job_name) is not None
            ):
                self.log.debug(
                    "Kill of run_id=%s is already in progress for generation %s",
                    run_id,
                    active.attempt_tag,
                )
                return None
            job = self._read_job(active.job_name)
            if job is not None:
                return self._kill_job_object(
                    job,
                    initiator=initiator,
                    reason=reason,
                    report_killed=True,
                )
            # The Job disappeared but secrets may linger: pin a tombstone anyway.
            record = self._record_from_active(active, reason)
            return self._finalize_cleanup_record(
                record, initiator=initiator, report_killed=True
            )

        # The registry does not know this run (lost state, or a legacy generation):
        # derive the generation from the cluster objects and kill that exact one.
        jobs = [
            job
            for job in self._list_our_jobs()
            if (job.metadata.annotations or {}).get("run_id") == str(run_id)
        ]
        for job in jobs:
            if self.registry.get_cleanup(job.metadata.name) is not None:
                continue
            killed_run = self._kill_job_object(
                job, initiator=initiator, reason=reason, report_killed=True
            )
            if killed_run is not None:
                return killed_run
        return None

    def _kill_job_object(
        self,
        job_to_kill: k8s_client.V1Job,
        initiator: KillInitiator,
        reason: str,
        report_killed: bool,
    ) -> KilledRun | None:
        """
        Write a generation-pinned tombstone for a Job and delete its resources.
        """
        annotations = job_to_kill.metadata.annotations or {}
        run_io = RunIO.from_dict(
            annotations, self.client, task_dir_extension=self._task_dir_extension
        )
        if self.registry.get_cleanup(run_io.container_name) is not None:
            return None

        self.log.info(
            "Killing run_id=%s attempt=%s (job %s)",
            run_io.run_id,
            run_io.attempt_tag or "legacy",
            run_io.container_name,
        )

        logs = ""
        if initiator == KillInitiator.USER:
            logs = self._get_logs(run_io)
            logs += "\n\nAlgorithm was killed by user request."

        record = self._record_from_run_io(
            run_io=run_io,
            annotations=annotations,
            reason=reason,
            logs=logs,
            deliver_logs=initiator == KillInitiator.USER,
        )
        return self._finalize_cleanup_record(
            record, initiator=initiator, report_killed=report_killed
        )

    def _finalize_cleanup_record(
        self,
        record: CleanupRecord,
        initiator: KillInitiator,
        report_killed: bool,
    ) -> KilledRun | None:
        """Persist a tombstone, delete its generation now, and deactivate the run."""
        self.registry.mark_killing(record.run_id)
        self.registry.add_cleanup(record)
        self._delete_generation(record)
        self._decrement_active_tasks()
        self.registry.deactivate(record.run_id)
        if report_killed:
            self.registry.mark_killed_reported(record.job_name)
            return KilledRun(
                run_id=record.run_id,
                task_id=record.task_id,
                parent_id=record.parent_id,
                logs=record.logs,
                attempt_tag=record.attempt_tag,
            )
        return None

    def _enqueue_completed_cleanup(self, run_io: RunIO) -> None:
        """Tombstone a naturally completed generation and delete its resources."""
        record = self._record_from_run_io(
            run_io=run_io,
            annotations={
                "run_id": str(run_io.run_id),
                ANN_DOCKER_LOGIN_SECRET: run_io.docker_login_secret_name,
            },
            reason=CLEANUP_REASON_COMPLETED,
            logs="",
            deliver_logs=False,
        )
        self.registry.add_cleanup(record)
        self.registry.deactivate(run_io.run_id)
        self._delete_generation(record)

    def _enqueue_cleanup_from_names(
        self,
        run_id: int,
        attempt_tag: str | None,
        job_name: str,
        reason: str,
        docker_secret_name: str | None = None,
    ) -> None:
        """Tombstone a generation identified by explicit resource names."""
        record = CleanupRecord(
            run_id=run_id,
            attempt_tag=attempt_tag,
            job_name=job_name,
            secret_name=job_name,
            docker_secret_name=docker_secret_name,
            reason=reason,
            logs_delivered=True,
        )
        self.registry.add_cleanup(record)
        self._delete_generation(record)

    def _record_from_run_io(
        self,
        run_io: RunIO,
        annotations: dict,
        reason: str,
        logs: str,
        deliver_logs: bool,
    ) -> CleanupRecord:
        docker_secret_name = annotations.get(ANN_DOCKER_LOGIN_SECRET) or None
        if docker_secret_name is None and run_io.attempt_tag is None:
            # legacy generations hard-coded this secret name
            docker_secret_name = self._docker_secret_name(run_io.run_id, None)

        def _int(key: str) -> int | None:
            value = annotations.get(key)
            return int(value) if value else None

        return CleanupRecord(
            run_id=run_io.run_id,
            attempt_tag=run_io.attempt_tag,
            job_name=run_io.container_name,
            secret_name=run_io.container_name,
            docker_secret_name=docker_secret_name,
            reason=reason,
            task_id=_int("task_id"),
            parent_id=_int("task_parent_id"),
            logs=logs,
            logs_delivered=not deliver_logs,
        )

    def _record_from_active(self, active: ActiveRun, reason: str) -> CleanupRecord:
        """Build a tombstone for a generation known from the registry."""
        return CleanupRecord(
            run_id=active.run_id,
            attempt_tag=active.attempt_tag,
            job_name=active.job_name,
            secret_name=active.job_name,
            docker_secret_name=self._docker_secret_name(
                active.run_id, active.attempt_tag
            ),
            reason=reason,
            task_id=active.task_id,
            parent_id=active.parent_id,
            logs_delivered=reason != CLEANUP_REASON_KILLED,
        )

    @staticmethod
    def _docker_secret_name(run_id: int, attempt_tag: str | None) -> str:
        suffix = f"-{attempt_tag}" if attempt_tag else ""
        return f"docker-login-secret-run-id-{run_id}{suffix}"

    def _delete_generation(self, record: CleanupRecord) -> ResourceDeletionResult:
        """Delete the exact resources pinned by a cleanup tombstone."""
        result = delete_run_generation_resources(
            job_name=record.job_name,
            namespace=self.task_namespace,
            core_api=self.core_api,
            batch_api=self.batch_api,
            secret_name=record.secret_name,
            docker_secret_name=record.docker_secret_name,
        )
        if not result.all_succeeded:
            failed = [name for name, ok in result._asdict().items() if not ok]
            self.registry.record_retry(
                record.job_name, f"Failed to delete resources: {', '.join(failed)}"
            )
        return result

    def _decrement_active_tasks(self) -> None:
        self.num_active_tasks = max(0, self.num_active_tasks - 1)

    def _list_our_jobs(self) -> list[k8s_client.V1Job]:
        try:
            return self.batch_api.list_namespaced_job(
                self.task_namespace, label_selector=self.task_job_label_selector
            ).items
        except ApiException:
            self.log.exception("Failed to list task jobs in the cluster")
            return []

    def _read_job(self, job_name: str) -> k8s_client.V1Job | None:
        try:
            return self.batch_api.read_namespaced_job(
                name=job_name, namespace=self.task_namespace
            )
        except ApiException as exc:
            if exc.status != 404:
                self.log.exception("Failed to read job %s", job_name)
            return None

    # ------------------------------------------------------------------
    # background reconciliation
    # ------------------------------------------------------------------
    def _start_cleanup_reconciler(self) -> None:
        thread = threading.Thread(
            target=self._cleanup_reconciler_worker,
            daemon=True,
            name="k8s-cleanup-reconciler",
        )
        thread.start()

    def _cleanup_reconciler_worker(self) -> None:
        """Rebuild state from the cluster, then keep retrying pending deletions."""
        try:
            self._reconcile_with_cluster()
        except Exception:
            self.log.exception("Initial cluster reconciliation failed")

        while True:
            time.sleep(CLEANUP_RETRY_INTERVAL_SECONDS)
            try:
                self._process_cleanups()
            except Exception:
                self.log.exception("Cleanup reconciler iteration failed")

    def _reconcile_with_cluster(self) -> None:
        """
        Align the registry with the actual cluster state after a node restart.

        Unknown jobs rebuild active state; generations that conflict with a newer
        registered generation are reclaimed; observed attempt tags advance the
        counter so tags are never reused.
        """
        jobs = self._list_our_jobs()
        for job in jobs:
            annotations = job.metadata.annotations or {}
            job_name = job.metadata.name
            if self.registry.get_cleanup(job_name) is not None:
                continue
            tag = annotations.get(ANN_ATTEMPT_TAG) or None
            try:
                run_id = int(annotations["run_id"])
            except (KeyError, TypeError, ValueError):
                self.log.warning(
                    "Job %s has no valid run_id annotation, cannot reconcile it",
                    job_name,
                )
                continue

            if tag:
                try:
                    self.registry.ensure_counter_at_least(int(tag[1:], 36))
                except ValueError:
                    pass

            active = self.registry.get_active(run_id)
            if active is not None and active.job_name == job_name:
                continue
            if active is None:
                parent_id = annotations.get("task_parent_id")
                self.registry.activate(
                    ActiveRun(
                        run_id=run_id,
                        attempt_tag=tag or "legacy",
                        job_name=job_name,
                        task_id=int(annotations.get("task_id", 0)),
                        parent_id=int(parent_id) if parent_id else None,
                        init_org_id=int(annotations.get("init_org_id", 0)),
                    )
                )
            else:
                # a newer generation is registered: this job is stale leftovers
                self.log.warning(
                    "Reclaiming stale job %s while newer generation %s is registered "
                    "for run_id=%s",
                    job_name,
                    active.job_name,
                    run_id,
                )
                run_io = RunIO.from_dict(
                    annotations,
                    self.client,
                    task_dir_extension=self._task_dir_extension,
                )
                self._reclaim_job(run_io, annotations)

    def _process_cleanups(self) -> None:
        """Retry deletion for every pending cleanup tombstone."""
        for record in self.registry.list_cleanups():
            result = self._delete_generation(record)
            if not result.all_succeeded:
                continue

            if not record.logs_delivered and not self._deliver_kill_logs(record):
                # keep the tombstone: the logs still need to reach HQ after
                # reconnecting
                continue

            active = self.registry.get_active(record.run_id)
            if active is not None and active.job_name == record.job_name:
                self.registry.deactivate(record.run_id)
            self.registry.remove_cleanup(record.job_name)

    def _deliver_kill_logs(self, record: CleanupRecord) -> bool:
        """
        Send the kill logs to HQ as a log-only run update.

        Returns False when HQ could not be reached (delivery is retried later); a
        rejection by HQ is terminal and not retried.
        """
        if not self.share_algorithm_logs or not record.logs:
            self.registry.mark_logs_delivered(record.job_name)
            return True
        try:
            response = self.client.run.patch(record.run_id, data={"log": record.logs})
        except Exception:
            self.log.exception(
                "Failed to deliver kill logs for run_id=%s; will retry",
                record.run_id,
            )
            return False
        if isinstance(response, dict) and "msg" not in response:
            self.registry.mark_logs_delivered(record.job_name)
            return True
        self.log.warning(
            "HQ rejected the kill log update for run_id=%s: %s; not retrying",
            record.run_id,
            response,
        )
        self.registry.mark_logs_delivered(record.job_name)
        return True

    def reconcile_with_hq(self, open_run_ids: set[int]) -> list[KilledRun]:
        """
        Reconcile local resources against the runs HQ still considers open.

        Called after the open-run sync on every (re)connect. Resources whose run
        is no longer open at HQ (including runs killed while this node was
        offline) are reclaimed. Pending user-kill terminal feedback is flushed.

        Parameters
        ----------
        open_run_ids: set[int]
            Run ids that HQ reports as open

        Returns
        -------
        list[KilledRun]
            Killed generations whose terminal feedback still needs to be sent
        """
        for job in self._list_our_jobs():
            annotations = job.metadata.annotations or {}
            job_name = job.metadata.name
            if self.registry.get_cleanup(job_name) is not None:
                continue
            try:
                run_id = int(annotations["run_id"])
            except (KeyError, TypeError, ValueError):
                continue
            if run_id in open_run_ids:
                continue
            run_io = RunIO.from_dict(
                annotations, self.client, task_dir_extension=self._task_dir_extension
            )
            self.log.info(
                "Run %s is no longer open at HQ but job %s still exists locally; "
                "reclaiming its resources",
                run_id,
                job_name,
            )
            self._reclaim_job(run_io, annotations)

        pending_reports = []
        for record in self.registry.due_kill_reports():
            pending_reports.append(
                KilledRun(
                    run_id=record.run_id,
                    task_id=record.task_id,
                    parent_id=record.parent_id,
                    logs=record.logs,
                    attempt_tag=record.attempt_tag,
                )
            )
            self.registry.mark_killed_reported(record.job_name)
        return pending_reports

    def _reclaim_job(self, run_io: RunIO, annotations: dict) -> None:
        """Delete resources of a generation that HQ no longer considers active."""
        record = self._record_from_run_io(
            run_io=run_io,
            annotations=annotations,
            reason=CLEANUP_REASON_RECONCILE,
            logs="",
            deliver_logs=False,
        )
        self.registry.add_cleanup(record)
        self.registry.deactivate(run_io.run_id)
        self._delete_generation(record)


# TODO we need this code when we move to k8s clusters
# def create_volume(self, volume_name: str) -> None:
#     """
#     This method creates a persistent volume through volume claims. However, this method is not being
#     used yet, as using only host_path volume binds seems to be enough and more convenient
#     (see details on _create_volume_mounts) - this is to be discussed
#     """

#     """
#     @precondition: at least one persistent volume has been provisioned in the (single) kubernetes node

#     """

#     is_valid_vol_name = re.search(
#         "[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
#         volume_name,
#     )

#     if not is_valid_vol_name:
#         # TODO custom exceptions to decouple codebase from kubernetes
#         raise Exception(f"Invalid volume name; {volume_name}")

#     # create a persistent volume claim with the given name
#     pvc = client.V1PersistentVolumeClaim(
#         api_version="v1",
#         kind="PersistentVolumeClaim",
#         metadata=client.V1ObjectMeta(name=volume_name),
#         spec=client.V1PersistentVolumeClaimSpec(
#             storage_class_name="manual",
#             access_modes=["ReadWriteOnce"],
#             resources=client.V1ResourceRequirements(
#                 # TODO Storage quota to be defined in system properties
#                 requests={"storage": "1Gi"}
#             ),
#         ),
#     )

#     """
#     If the volume was not claimed with the given name yet, there won't be exception.
#     If the volume was already claimed with the same name, (which should not make the function to fail),
#         the API is expected to return an 409 error code.
#     """
#     try:
#         self.core_api.create_namespaced_persistent_volume_claim(self.task_namespace), body=pvc)
#     except client.rest.ApiException as e:
#         if e.status != 409:
#             # TODO custom exceptions to decouple codebase from kubernetes
#             raise Exception(
#                 f"Unexpected kubernetes API error code {e.status}"
#             ) from e
