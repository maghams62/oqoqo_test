# Synthetic Git Dataset Runner

This standalone repo contains just the code required to regenerate and push the synthetic multi-repo Git dataset used for Oqoqo demos. Copy `.env` locally with the required `SYNTHETIC_GIT_*` variables, then run `python scripts/synthetic_git_dataset.py --force --push` to rebuild and publish the fixtures without touching the main mono-repo.
