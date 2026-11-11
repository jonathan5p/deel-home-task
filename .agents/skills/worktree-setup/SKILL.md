---
name: worktree-setup
description: >
  Create, prepare and remove a git worktree for a change in this repository.
  Trigger: starting any change, giving a subagent a place to write, or cleaning up after a PR merges.
---

Rules: [worktrees](../../rules/worktrees.md).

## Create

1. `git fetch origin`.
2. Claude Code: `EnterWorktree` with a short name (creates `.claude/worktrees/<name>`). Otherwise, or for a subagent:
   `git worktree add .claude/worktrees/<name> -b feat/<issue>-<slug> origin/main`.
   For a stacked PR, branch from the parent PR's branch (`origin/<parent branch>`) instead of `origin/main`.
3. Rename the branch if needed: `/usr/bin/git -C <worktree> branch -m feat/<issue>-<slug>`.

## Link the local context

From the worktree root:

`.agents/` and `AGENTS.md` come with the checkout. Link only what stays local (git-ignored):

```sh
ln -s <main checkout>/.agents/docs .agents/docs
mkdir -p .claude && ln -s ../.agents/skills .claude/skills
```

Check: `ls .agents/docs/ .claude/skills/` resolves.

## Prepare

- `uv sync --frozen`. On a branch below the top of a stack this can remove `pre-commit`; re-sync on the top branch afterwards.
- `uv run --frozen pre-commit install` if the hooks are not installed in this worktree.
- Use `/usr/bin/git -C <worktree> …` when the rtk hook's rewritten `git` is refused.
- Never bare `git stash`; never check out a branch another worktree uses.

## Clean up

After the PR merges:

```sh
git worktree remove .claude/worktrees/<name>
git branch -D <branch>
```

Remove the worktree's idle `deel-checks-<name>` networks as in [pre-push-checks](../pre-push-checks/SKILL.md#3-docker-network-exhaustion).
