import os
import shutil

import activator.repo_tracker


# Config options
# --------------

workers = 1

threads = 1
worker_class = "sync"

# Normally defined in the Kubernetes config
graceful_timeout = int(os.environ.get("WORKER_GRACE_PERIOD", 30))
timeout = int(os.environ.get("WORKER_TIMEOUT", 0))
max_requests = int(os.environ.get("WORKER_RESTART_FREQ", 0))


# Hooks run on the master process
# -------------------------------

def when_ready(server):
    tracker = activator.repo_tracker.LocalRepoTracker.get()
    tracker.init_tracker()


def child_exit(server, worker):
    tracker = activator.repo_tracker.LocalRepoTracker.get()
    repo = tracker.pop(worker.pid)
    try:
        shutil.rmtree(repo)
    except FileNotFoundError:
        pass
    # Propagate all other exceptions; it means the repo is still around!


def on_exit(server):
    tracker = activator.repo_tracker.LocalRepoTracker.get()
    tracker.cleanup_tracker()
