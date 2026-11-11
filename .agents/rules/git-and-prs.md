# Git, commits and pull requests

The [stacked-prs](../skills/stacked-prs/SKILL.md) and [review-comments](../skills/review-comments/SKILL.md) skills walk through these steps.

## Branches and commits

- Feature branches named `feat/<issue>-<slug>` (or `fix/`, `refactor/`, …); the default branch is `main`. Never commit to `main` directly.
- **The agent chooses the commit message.** Do not offer options or wait for the user. Always Conventional Commits, `type(scope): summary` in the imperative mood, with the component as scope (for example `feat(gold): rebuild gold from the silver tables`, `refactor(gold): drop the retention comments`). No ticket IDs. Mention the chosen message in the report.
- **No AI attribution anywhere:** no `Co-Authored-By` trailers, no "Generated with" lines, in commits, PR titles, PR bodies or comments. The only exception is the reply marker below.
- Small, atomic commits; each one passes the hooks.
- The [git-commit skill](../skills/git-commit/SKILL.md) is a global skill. Where it conflicts with this file, this file wins: ignore its "offer 3 options", Jira `IND-` scope, `master` branch and `rebase origin/master` rules.

## One stack per line of work

- One PR per sub-issue (or similarly small unit); never bundle a whole issue into one PR. Group by the nature of the work.
- Every PR that depends on another PR, or belongs to the same line of work, goes into one native GitHub stack (`gh stack`). Never chain PR bases by hand. Use `gh stack init`, `gh stack add` and `gh stack submit` for new branches, and `gh stack link <bottom> … <top>` for existing PRs.
- Each independent line of work gets its own stack.
- Each PR closes its own sub-issue; the top PR of a stack also closes the parent issue.

## Restacking

1. `git fetch origin` first.
2. The user's `gh stack` restacks rewrite remote heads. Before touching a branch, compare it with the remote: `git range-diff origin/<branch>...<branch>`. If the remote head moved, adopt it before making changes.
3. Move a branch onto its new parent with `git rebase --onto <new parent> <old parent> <branch>`.
4. Push with `git push --force-with-lease=<branch>:<expected remote sha> origin <branch>`. Never use a bare `--force`.

## CI after a base change

Changing a PR's base does not trigger CI. Close and reopen the PR to run it: `gh pr close <n> && gh pr reopen <n>`.

## Merging stacked PRs

- Use `gh stack merge <pr> --yes --merge`. `gh pr merge` and the REST merge endpoint refuse stacked PRs.
- It merges that PR and every PR below it, in one atomic operation. Merge only the PRs the user named. If the named PR is not the bottom of its stack, confirm with the user first, because the PRs below it merge too. Never merge a whole stack unless the user asked for it.
- Native stacks retarget the remaining PRs automatically; read back their bases and CI afterwards.

## Review comments

- Before pushing to a PR, and again before reporting it done, re-list every unresolved review thread and every conversation comment without an agent reply. That list is the to-do list, short "remove this" requests included.
- Every reply to a review or discussion comment, on a PR or an issue, starts with this line, then the reply text on the next line, naming the commit that addresses it:

  ```markdown
  🤖 **Agent reply**
  ```

- Resolve a review thread only after the user approves.
