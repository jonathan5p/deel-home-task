---
name: stacked-prs
description: >
  Create, link, restack and merge native GitHub stacks of pull requests in this repository.
  Trigger: opening a PR that depends on another PR or continues a line of work; restacking after a
  lower PR changed, was restacked by the user, or merged; linking existing PRs into a stack; merging stacked PRs.
---

Rules: [git and PRs](../../rules/git-and-prs.md). One native stack per line of work; never chain bases by hand.

## 1. Find or create the stack

1. `git fetch origin`, then `gh stack view` (or `gh pr list --state open`) to see whether the line of work already has a stack.
2. New branches: from the worktree, `gh stack init` (bottom branch on `main`), then `gh stack add <branch>` for each layer, then `gh stack submit`.
3. Existing PRs: `gh stack link <bottom> … <top>` with branch names or PR numbers, bottom first.
4. Each PR body names the sub-issue it closes (`Closes #<n>`); the top PR also closes the parent issue. No AI attribution in titles or bodies.
5. Read back: `gh stack view` and `gh pr view <n> --json baseRefName,headRefName` for each layer.

## 2. Restack a branch

1. `git fetch origin`.
2. Compare local and remote before touching anything: `git range-diff origin/<branch>...<branch>`. The user's `gh stack` restacks rewrite remote heads; if the remote moved, reset your local branch to it (`git reset --hard origin/<branch>` only in your own worktree, after checking for uncommitted work) before continuing.
3. Record the expected remote head: `git rev-parse origin/<branch>`.
4. Move the branch: `git rebase --onto <new parent> <old parent> <branch>`. Old parent is the commit the branch was built on (the previous head of the lower branch, or the merge base with `main`).
5. Run the checks the change needs ([pre-push-checks](../pre-push-checks/SKILL.md)); never `--no-verify`.
6. Push with a lease: `git push --force-with-lease=<branch>:<expected sha> origin <branch>`. Repeat for each layer upward.
7. A base change does not trigger CI. For each PR whose base or parent changed: `gh pr close <n> && gh pr reopen <n>`, then confirm a run started with `gh pr checks <n>`.

## 3. Merge

1. Merge only what the user named, with their direct approval. `gh stack merge <pr>` merges that PR and every PR below it; if the named PR is not the bottom, confirm the full scope with the user first.
2. `gh stack merge <pr> --yes --merge`. Never `gh pr merge` or the REST merge endpoint: both refuse stacked PRs.
3. Read back: merged state of each PR, the remaining PRs' new bases (native stacks retarget them), and their CI. Close and reopen any that did not start CI.
4. Clean up merged branches' worktrees ([worktree-setup](../worktree-setup/SKILL.md#clean-up)).
