import shutil

import activator.repo_registry


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
