---
name: orchestrator
description: >
  Playbook for the agent working with the user on this repository: plan, delegate to subagents in their own
  worktrees, relay between them and report. Trigger: any multi-step task, work that spans several PRs or a stack,
  or deciding whether to spawn explorers, writers, reviewers or test runners.
---

Rules: [collaboration](../../rules/collaboration.md). You are the orchestrator: you talk to the user, ask decisions through the question UI, and own approvals and the final report. There are no parallel worker sessions.

## 1. Plan

1. Read the relevant rules files and the issue. Delegate exploration to one subagent when understanding needs many files.
2. For a big feature, present the plan (scope, PRs and stack order, files, decisions with recommendations) and wait for approval.
3. Ask open decisions through the question UI, recommended option first.

## 2. Delegate

- Do small work yourself. Spawn a subagent only for work that runs longer than about 10 minutes or is complex (wide exploration, multi-file changes, stacks).
- One writer subagent per PR, or per stack when the layers are tightly coupled. Each writer gets its own worktree ([worktree-setup](../worktree-setup/SKILL.md)); no two writers share a worktree or branch.
- Reviewers and test runners are read-only and get the worktree path they check.
- Serialise work that uses the shared dev stack (`make up`, `make spark-submit`, `make data-quality`, `make spark-sql`).

Brief template:

```markdown
Goal: <one outcome>. Issue: #<n>. PR/stack position: <bottom|layer k of stack X|standalone>.
Worktree: <path>, branch <branch>, based on <parent>. Files you own: <list>.
Context: <what is already decided or done; results relayed from other subagents>.
Rules (must follow):
1. Work only in your worktree; use /usr/bin/git -C <worktree>; no bare git stash.
2. Every change tested; full typing with ty; never --no-verify.
3. Comments only where genuinely needed; no narration; do not move removed comments to the M7 notes.
4. Infra tests only if the change needs them; data quality only via make data-quality.
5. Choose the commit message yourself: Conventional Commits type(scope): summary, no ticket IDs, no AI attribution anywhere.
6. Do not merge, push without a lease, edit GitHub issues or resolve review threads; report instead.
7. Stop every background shell, watch and monitor you started before you report; leave nothing running.
Report: branch, commits, checks run and skipped (why), open questions.
```

## 3. Relay

Subagents cannot reach the user or each other. Pass results, branch heads and questions between them yourself, in follow-up messages or the next brief. Questions for the user go through the question UI; never answer an approval question on the user's behalf.

## 4. Integrate and report

1. Check each writer's result yourself: diff, checks, stack order ([stacked-prs](../stacked-prs/SKILL.md)).
2. Before reporting a PR done, run [review-comments](../review-comments/SKILL.md) and confirm the list is empty.
3. Merges, merge scope, issue edits and thread resolution wait for the user's direct approval.
4. Final report: PR links, commits, checks run and skipped, open decisions, then the hand-off test instructions.
5. Keep the subagent roster short: stop finished or long-running subagents (and their background tasks) instead of leaving them idle, and spawn a fresh subagent with a full brief for the next task.
