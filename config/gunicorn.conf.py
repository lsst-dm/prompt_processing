import shutil

import activator.repo_tracker


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
