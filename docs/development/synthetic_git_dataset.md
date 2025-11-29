# Synthetic Git Dataset Workflow

This repo now ships a helper script that builds the multi-repo synthetic Git
dataset requested for Oqoqo’s Option 2 demo and optionally pushes the generated
artifacts to a dedicated branch.

## Environment & Config

Add the following entries to `.env` (or export them in your shell):

```
SYNTHETIC_GIT_BRANCH=synthetic-git-dataset
SYNTHETIC_GIT_REMOTE=https://<token>@github.com/<owner>/<repo>.git
# Optional overrides if remote URL already configured via ~/.gitconfig:
# SYNTHETIC_GIT_REPO_OWNER=<owner>
# SYNTHETIC_GIT_REPO_NAME=<repo>
```

`config.yaml` contains a matching `synthetic_git` section where you can change
`base_dir`, default branch names, or the commit message template.

## Generating the dataset

```
python scripts/synthetic_git_dataset.py --force
```

This removes whatever existed in `data/synthetic_git/`, rebuilds the four toy
repos with their commit history, and emits `git_events.json` + `git_prs.json`.

## Generate + push in one command

```
python scripts/synthetic_git_dataset.py --force --push
```

When `--push` is present the script:

1. Switches to the configured branch (creating it off `synthetic_git.base_branch`).
2. Stages `data/synthetic_git/` (or the entire repo with `--include-all`).
3. Commits with the timestamped template from `config.synthetic_git.commit_message`.
4. Pushes to the remote specified by `--remote`, `SYNTHETIC_GIT_REMOTE`, or the
   derived tokenized URL (`https://<token>@github.com/...`).

Use `--skip-generate` if you only need to re-push existing artifacts.

