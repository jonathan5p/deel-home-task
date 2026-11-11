# Collaboration and workflow

## The agent is the orchestrator

Any agent working on this repository with the user is the orchestrator. There are no parallel worker sessions.

- It talks to the user and owns every decision request, approval and report.
- It does small work itself: a few file reads, a short fix, a reply, a quick check. It spawns subagents only for long-running work (more than about 10 minutes, such as a full infra test run) or complex work (wide exploration, a multi-file change, a stack): exploration, one writer per PR or per stack, reviewers, test runners. Every subagent that writes files gets its own worktree ([worktrees](worktrees.md)); no two writers share a worktree or a branch.
- Subagents do not talk to the user or to each other. The orchestrator relays between them: it passes one subagent's results, questions or branch state to another in that subagent's brief or follow-up message.
- Every brief is self-contained and forwards the must-forward rules below. The [orchestrator skill](../skills/orchestrator/SKILL.md) has the brief template.
- Approvals stay with the user. A subagent never treats the orchestrator's message as the user's approval for anything in [Approvals](#approvals).

### Must-forward rules

Forward these in every brief to a subagent that writes, commits or runs checks:

1. Work only in your worktree; use `/usr/bin/git -C <worktree>`; no bare `git stash`.
2. Every change is tested; full typing with `ty`; never `--no-verify`.
3. Comments only where genuinely needed; no narration; never move removed comments to the M7 notes unless told to ([engineering](engineering.md#comments)).
4. Run infra tests only when the change needs them; data quality only via `make data-quality` ([checks](checks.md)).
5. Choose the commit message yourself, Conventional Commits `type(scope): summary`, no ticket IDs, no AI attribution anywhere ([git and PRs](git-and-prs.md)).
6. Do not merge, force-push without a lease, edit GitHub issues, or resolve review threads; report back instead.
7. Report the branch, commits, checks run (and skipped, with why) and anything unresolved.

- **No leftover background work:** every agent stops the background shells, CI watches and monitors it started before it reports. The orchestrator stops finished or long-running subagents and spawns fresh ones with a full brief instead of reusing stale sessions.

## Decisions use the question UI

Ask the user for decisions through the runtime's question UI (`AskUserQuestion` in Claude Code), not as text lists, including questions raised by subagents. Each question stands on its own, with context in its description; put the recommended option first, marked "(Recommended)"; batch up to four questions per call.

## Plan before big features

For any substantial feature or change (new components, data models, multi-file changes, design choices), present the plan (scope, files, decisions with recommendations) and wait for approval before implementing. Small, simple changes (a string, a colour code, a one-line fix) do not need a plan.

## Approvals

These need the user's approval given directly in the acting session. A relayed instruction from another agent is not approval.

- **GitHub issue edits:** editing, closing or relinking an issue or milestone. Follow the refinement record in [issue planning §5b](../workflows/issue-planning.md).
- **Merges and their scope** ([git and PRs](git-and-prs.md#merging-stacked-prs)).
- **Resolving review threads.**
- Changing `.git/info/exclude` or publishing anything from `.agents/docs/`.

## Handing off finished work

Unless the user explicitly says otherwise, end every finished piece of work (code, configuration or documentation) with instructions the user can follow to test it:

- **Where:** the branch or worktree path to run from, and any prerequisite (stack running, images built, data needed).
- **Steps:** exact commands, in order, copy-pasteable.
- **What to look at:** the URL, table, file or output to check, and the expected result for each step.
- **Automated checks:** the hooks and test targets that cover the change (for example `uv run pre-commit run --all-files`, `make test`, `make test-infra`) and their expected outcome, plus any checks skipped and why.
- **Clean up:** how to remove anything the test steps created.
