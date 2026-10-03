from pathlib import Path

from vantage6.common.globals import APPNAME

#
#   NODE SETTINGS
#
DEFAULT_NODE_SYSTEM_FOLDERS = False

#
#   INSTALLATION SETTINGS
#
PACKAGE_FOLDER = Path(__file__).parent.parent.parent

# TODO Check and remove
# HCR: This constants is not being used, as well as the other defined in cli/globals.py
# NODE_V6_PROXY_HOSTNAME = "proxyserver"

# Timeout in seconds of the K8S event-stream watch used by the container manager.
# This is the maximum amount of time the container manager will wait
# for a terminal event (job pod created successfuly, creation failed, etc).
# The container manager will wait again (for up to K8S_EVENT_STREAM_LOOP_TIMEOUT seconds)
# for events if the timeout is caused by a long image pull (massive image or slow connection)
# so this parameter doesn't have a significant impact on the container manager
# behaviour.
K8S_EVENT_STREAM_LOOP_TIMEOUT = 120

DATA_FOLDER = PACKAGE_FOLDER / APPNAME / "_data"

# constants for retrying node login
SLEEP_BTWN_NODE_LOGIN_TRIES = 10  # retry every 10s
TIME_LIMIT_RETRY_CONNECT_NODE = 60 * 60 * 24 * 7  # i.e. 1 week

# constant for waiting for the initial websocket connection
TIME_LIMIT_INITIAL_CONNECTION_WEBSOCKET = 60

# constants for retrying task start due to a temporary error
TASK_START_RETRIES = 3

# Algorithm log streaming over the websocket connection. The node streams pod
# log lines with a per-run sequence number and only advances when HQ
# acknowledges the cursor.
# Seconds to wait for an HQ acknowledgement before resending the line.
LOG_STREAM_ACK_TIMEOUT_SECONDS = 5
# Backoff between attempts to resend a line that HQ did not acknowledge.
LOG_STREAM_SEND_RETRY_SECONDS = 2
# Backoff between attempts to re-establish an interrupted Kubernetes log watch.
LOG_STREAM_WATCH_RETRY_SECONDS = 2
# Interval at which a paused stream checks whether the socket reconnected.
LOG_STREAM_DISCONNECT_POLL_SECONDS = 1
# Consecutive acknowledgement timeouts after which the node assumes that HQ
# does not support cursors and falls back to unacknowledged streaming.
LOG_STREAM_LEGACY_ACK_TIMEOUTS = 2
# Number of attempts to find the task pod on (re)attach before giving up.
LOG_STREAM_NO_POD_RETRIES = 30

# Environment variables that should be set in the Dockerfile and that may not
# be overwritten by the user.
ENV_VARS_NOT_SETTABLE_BY_NODE = ["PKG_NAME"]

# default policies
DEFAULT_REQUIRE_ALGO_IMAGE_PULL = True

# Mount paths within the algorithm containers. Algorithms containers are run as
# jobs in the Kubernetes cluster.
TASK_JOB_INPUT_PATH = "/app/vantage6/task/input"
TASK_JOB_OUTPUT_PATH = "/app/vantage6/task/output"
TASK_JOB_SESSION_FOLDER_PATH = "/app/vantage6/task/session"

# The mount location of the tasks files, databases and kube config in the node
# container.
TASK_FILES_ROOT = "/app/tasks"
DATABASE_BASE_PATH = "/app/databases/"

# Default proxy server port. It may be changed when starting the proxy if
# the port is already in use
DEFAULT_PROXY_PORT = 7654

# Image used to probe algorithm network isolation at node startup.
ISOLATION_PROBE_IMAGE = "curlimages/curl:latest"

# RFC 2606 example domains that respond on the public internet. These are unlikely
# to appear in production whitelists.
ISOLATION_PROBE_CANDIDATES = (
    "https://example.com",
    "https://example.org",
    "https://example.net",
)

# Maximum curl time for each isolation probe request.
ISOLATION_PROBE_CURL_MAX_TIME_SECONDS = 5

# Maximum time to wait for the isolation probe pod to complete.
ISOLATION_PROBE_TIMEOUT_SECONDS = 15
