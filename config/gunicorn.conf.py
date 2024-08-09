import os
import shutil

import activator.repo_registry


# Config options
# --------------

threads = 1
worker_class = "sync"

# Normally defined in the Kubernetes config
workers = int(os.environ.get("WORKER_COUNT", 1))
graceful_timeout = int(os.environ.get("WORKER_GRACE_PERIOD", 30))
timeout = int(os.environ.get("WORKER_TIMEOUT", 0))
max_requests = int(os.environ.get("WORKER_RESTART_FREQ", 0))


# Hooks run on the master process
# -------------------------------

def when_ready(server):
    registry = activator.repo_registry.LocalRepoRegistry.get()
    registry.init_registry()


def child_exit(server, worker):
    registry = activator.repo_registry.LocalRepoRegistry.get()
    repo = registry.pop(worker.pid)
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
    # Propagate all other exceptions; it means the repo is still around!


def on_exit(server):
    registry = activator.repo_registry.LocalRepoRegistry.get()
    registry.cleanup_registry()
