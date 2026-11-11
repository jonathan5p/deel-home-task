# Worktrees

Every change is made in its own git worktree, never in the main checkout. The orchestrator gives each subagent that writes files its own worktree (see [collaboration](collaboration.md)). The [worktree-setup skill](../skills/worktree-setup/SKILL.md) walks through these steps.

- **Create the worktree from `origin/main`**, or from the parent branch for a stacked PR: in Claude Code use `EnterWorktree` (it creates `.claude/worktrees/<name>`); otherwise `git worktree add .claude/worktrees/<name> -b <branch> origin/main`. Rename the branch to `feat/<issue>-<slug>`. `.claude/worktrees/` is excluded in `.git/info/exclude`.
- **Link the local context:** `.agents/` and `AGENTS.md` are tracked and come with the checkout. Only `.agents/docs/` and `.claude/skills` are git-excluded; link them from the main checkout:

  ```sh
  ln -s <main checkout>/.agents/docs .agents/docs
  mkdir -p .claude && ln -s ../.agents/skills .claude/skills
  ```

  Both links are excluded. Changes to the rules, skills and knowledge base go through a PR like any other file.
- **Git inside a worktree:** if a command wrapper rewrites `git` (the rtk hook) and the session refuses it, call `/usr/bin/git -C <worktree path> ...` directly.
- **One branch per worktree:** never check out a branch that another worktree uses, and never use bare `git stash` or `git stash pop` (the stash is shared between worktrees).
- **Python:** run `uv sync --frozen` inside the worktree; each worktree has its own `.venv`. On a branch below the top of a stack, `uv sync --frozen` can remove `pre-commit` from `.venv` and the hook then fails with "No module named pre_commit"; re-sync on the top branch afterwards.
- **Docker Compose:** `.env` pins `COMPOSE_PROJECT_NAME=deel_home_task`, so `make` targets in a worktree address the shared dev stack.
- **Docker resources per checkout:** each Compose project builds its own images (`<project>-spark`, `<project>-spark-app`, `<project>-api`), so projects never share images; infra-test teardown removes that project's images. Code checks (`make test`, `lint`, `format`, `typecheck`, the hooks) run in the project `deel-checks-<folder>` and infra tests in `deel-infra-tests-<folder>-gw<n>`, one project per pytest-xdist worker (`make test-infra` runs `INFRA_WORKERS`, default 2; a serial run uses `deel-infra-tests-<folder>`), all named after the worktree folder, so worktrees can run them in parallel. The shared dev stack (`deel_home_task`: `make up`, `make spark-submit`, `make data-quality`, `make spark-sql`) is shared: the orchestrator serialises work that changes it. Every infra worker is a full stack, so a run needs Docker memory for `INFRA_WORKERS` stacks; stagger runs from several worktrees. For network exhaustion see [checks](checks.md#docker-network-exhaustion).
- **Clean up** after the PR merges: `git worktree remove .claude/worktrees/<name>` and delete the local branch.
