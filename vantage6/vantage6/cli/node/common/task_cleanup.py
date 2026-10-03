import logging
from typing import NamedTuple

from kubernetes import client as k8s_client
from kubernetes.client.rest import ApiException

from vantage6.common import logger_name

log = logging.getLogger(logger_name(__name__))


class ResourceDeletionResult(NamedTuple):
    """Per-resource outcome of a generation cleanup.

    A missing resource (HTTP 404) counts as a successful deletion because the
    desired end state (resource absent) is already met.
    """

    job: bool
    pods: bool
    secret: bool
    docker_secret: bool

    @property
    def all_succeeded(self) -> bool:
        return self.job and self.pods and self.secret and self.docker_secret


def delete_run_generation_resources(
    *,
    job_name: str,
    namespace: str,
    core_api: k8s_client.CoreV1Api,
    batch_api: k8s_client.BatchV1Api,
    secret_name: str | None = None,
    docker_secret_name: str | None = None,
    pod_label_selector: str | None = None,
) -> ResourceDeletionResult:
    """
    Delete the Kubernetes resources of a single run generation.

    The resources to delete are identified by their exact names and a pod label
    selector. Callers must derive these from the generation they intend to delete
    so that retrying this function can never remove resources of a newer
    generation of the same run.

    Parameters
    ----------
    job_name: str
        Name of the Job to delete
    namespace: str
        Namespace in which the resources live
    core_api: k8s_client.CoreV1Api
        Kubernetes Core API instance
    batch_api: k8s_client.BatchV1Api
        Kubernetes Batch API instance
    secret_name: str, optional
        Name of the run Secret. Defaults to the Job name.
    docker_secret_name: str, optional
        Name of the Docker login Secret. If None, no such Secret is deleted
        (it only exists for private registries).
    pod_label_selector: str, optional
        Label selector for the Pods created by the Job. Defaults to
        ``job-name=<job_name>``.

    Returns
    -------
    ResourceDeletionResult
        Whether each kind of resource was successfully removed (or was already
        absent). ``all_succeeded`` indicates whether the cleanup is complete.
    """
    secret_name = secret_name or job_name
    pod_label_selector = pod_label_selector or f"job-name={job_name}"

    log.info(
        "Cleaning up kubernetes Job %s and its related resources in namespace %s",
        job_name,
        namespace,
    )

    job_ok = _delete_job(job_name, namespace, batch_api)

    pods_ok = True
    try:
        job_pods_list = core_api.list_namespaced_pod(
            namespace, label_selector=pod_label_selector
        )
        for job_pod in job_pods_list.items:
            if not _delete_pod(job_pod.metadata.name, namespace, core_api):
                pods_ok = False
    except ApiException as exc:
        if exc.status == 404:
            log.debug("No pods found for selector %s", pod_label_selector)
        else:
            log.error("Exception when listing pods for job %s: %s", job_name, exc)
            pods_ok = False

    secret_ok = _delete_secret(secret_name, namespace, core_api)

    docker_secret_ok = True
    if docker_secret_name:
        docker_secret_ok = _delete_secret(
            docker_secret_name, namespace, core_api, log_if_not_found=False
        )

    return ResourceDeletionResult(
        job=job_ok,
        pods=pods_ok,
        secret=secret_ok,
        docker_secret=docker_secret_ok,
    )


def delete_run_related_pods(
    run_id: int,
    container_name: str,
    namespace: str,
    core_api: k8s_client.CoreV1Api,
    batch_api: k8s_client.BatchV1Api,
) -> ResourceDeletionResult:
    """
    Delete the Job, Pods and Secrets of a run generation.

    Legacy helper kept for callers (and resources) that predate generation tags:
    names are derived from the run id and container name. New code should call
    :func:`delete_run_generation_resources` with the generation-specific names.

    Parameters
    ----------
    run_id: int
        ID of the run
    container_name: str
        Name of the container/Job
    namespace: str
        Namespace where the container is located
    core_api: k8s_client.CoreV1Api
        Kubernetes Core API instance
    batch_api: k8s_client.BatchV1Api
        Kubernetes Batch API instance

    Returns
    -------
    ResourceDeletionResult
        Whether each kind of resource was successfully removed.
    """
    return delete_run_generation_resources(
        job_name=container_name,
        namespace=namespace,
        core_api=core_api,
        batch_api=batch_api,
        secret_name=container_name,
        docker_secret_name=f"docker-login-secret-run-id-{run_id}",
    )


def _delete_secret(
    secret_name: str,
    namespace: str,
    core_api: k8s_client.CoreV1Api,
    log_if_not_found: bool = True,
) -> bool:
    """Delete a namespaced Secret. A 404 response counts as success."""
    try:
        core_api.delete_namespaced_secret(name=secret_name, namespace=namespace)
        log.info(
            "Removed kubernetes Secret %s in namespace %s",
            secret_name,
            namespace,
        )
        return True
    except ApiException as exc:
        if exc.status == 404:
            if log_if_not_found:
                log.debug(
                    "No secret %s to remove in namespace %s", secret_name, namespace
                )
            return True
        log.error("Exception when deleting namespaced secret %s: %s", secret_name, exc)
        return False


def _delete_job(
    job_name: str, namespace: str, batch_api: k8s_client.BatchV1Api
) -> bool:
    """Delete a namespaced Job. A 404 response counts as success."""
    try:
        batch_api.delete_namespaced_job(name=job_name, namespace=namespace)
        log.info("Removed kubernetes Job %s in namespace %s", job_name, namespace)
        return True
    except ApiException as exc:
        if exc.status == 404:
            log.debug(
                "Job %s not found in namespace %s, nothing to remove",
                job_name,
                namespace,
            )
            return True
        log.error("Exception when deleting namespaced job %s: %s", job_name, exc)
        return False


def _delete_pod(pod_name: str, namespace: str, core_api: k8s_client.CoreV1Api) -> bool:
    """Delete a namespaced Pod. A 404 response counts as success."""
    log.info("Cleaning up kubernetes pod %s in namespace %s", pod_name, namespace)
    try:
        core_api.delete_namespaced_pod(name=pod_name, namespace=namespace)
        return True
    except ApiException as exc:
        if exc.status == 404:
            log.debug(
                "Pod %s not found in namespace %s, nothing to remove",
                pod_name,
                namespace,
            )
            return True
        log.error("Exception when deleting namespaced pod %s: %s", pod_name, exc)
        return False
