# Agent context

Context for agents working on Deel's data engineering take-home assignment: rules, skills, workflows and the knowledge base, tracked in the repository. `.agents/docs/` and `.claude/skills` are local only and git-excluded: never stage, commit or publish them.

## Source documents

`.agents/docs/` is local only and git-ignored: it holds the assignment, the job description and the monitor runbook.

- [Take-home assignment](docs/DEEL-SPARK-TAKE-HOME-TEST.pdf): requirements and expected deliverables.
- [Job description](docs/job_description.pdf): role context, not additional assignment requirements.
- [Repository README](../README.md): the supplied database and CDC stack.

Read the assignment before proposing architecture or implementing the pipeline.

## Golden rules

1. You are the orchestrator: talk to the user, ask decisions through the question UI, and delegate to subagents with briefs that forward the must-forward rules. → [collaboration](rules/collaboration.md)
2. Plan big features and wait for approval; small changes skip the plan. → [collaboration](rules/collaboration.md#plan-before-big-features)
3. Every change is tested, fully typed with `ty`, coverage at or above 90%. → [engineering](rules/engineering.md)
4. Comments only where genuinely needed; never narration; never move removed comments to the M7 notes on your own. → [engineering](rules/engineering.md#comments)
5. Never `--no-verify`. → [checks](rules/checks.md)
6. Infra tests only when the change needs them; data quality only via `make data-quality`. → [checks](rules/checks.md)
7. Every change in its own worktree, with `.agents`, `AGENTS.md` and `.claude/skills` linked. → [worktrees](rules/worktrees.md)
8. One native GitHub stack per line of work; restack with fetch, range-diff, `rebase --onto` and a lease. → [git and PRs](rules/git-and-prs.md)
9. Merge stacked PRs only with `gh stack merge <pr> --yes --merge`, and only what the user named. → [git and PRs](rules/git-and-prs.md#merging-stacked-prs)
10. You choose the commit message: Conventional Commits, no ticket IDs, no AI attribution anywhere. → [git and PRs](rules/git-and-prs.md#branches-and-commits)
11. Re-list unresolved review threads before pushing and before reporting a PR done; replies start with `🤖 **Agent reply**`. → [git and PRs](rules/git-and-prs.md#review-comments)
12. GitHub issue edits, merges and thread resolution need the user's direct approval. → [collaboration](rules/collaboration.md#approvals)
13. End finished work with step-by-step test instructions. → [collaboration](rules/collaboration.md#handing-off-finished-work)

## Rules

| File | Read when |
| --- | --- |
| [rules/collaboration.md](rules/collaboration.md) | Always: the orchestrator workflow, questions, approvals, handoff |
| [rules/engineering.md](rules/engineering.md) | Writing or reviewing code, tests, schemas or comments |
| [rules/checks.md](rules/checks.md) | Before committing, pushing or reporting work done |
| [rules/git-and-prs.md](rules/git-and-prs.md) | Branching, committing, opening, restacking, merging or answering PRs |
| [rules/worktrees.md](rules/worktrees.md) | Starting any change |

## Skills

Skills live in `.agents/skills/<name>/SKILL.md`; `.claude/skills` links here so Claude Code finds them.

| Skill | Use when |
| --- | --- |
| [orchestrator](skills/orchestrator/SKILL.md) | Planning work and delegating it to subagents |
| [worktree-setup](skills/worktree-setup/SKILL.md) | Starting or cleaning up a worktree |
| [stacked-prs](skills/stacked-prs/SKILL.md) | Opening, linking, restacking or merging stacked PRs |
| [review-comments](skills/review-comments/SKILL.md) | Answering PR feedback; before any push to a PR or reporting it done |
| [pre-push-checks](skills/pre-push-checks/SKILL.md) | Choosing and running checks before a commit, push or report |
| [git-commit](skills/git-commit/SKILL.md) | Global commit skill; [git and PRs](rules/git-and-prs.md) overrides it |

The Grafana skills below come from Grafana's `gcx` CLI (Apache-2.0), installed with `gcx agent skills install <names> --dir .agents` and refreshed with `gcx agent skills update --dir .agents`. They target Grafana Cloud as well; here only the self-managed parts apply. Point gcx at the local Grafana with a context of its own (`gcx config set stacks.local.grafana.server http://localhost:3000`, org id 1, the local admin login from `.env`, `contexts.local.datasources.prometheus prometheus`), then check it with `gcx config check`.

| Skill | Use it for |
| --- | --- |
| [gcx](skills/gcx/SKILL.md) | Any gcx command: datasources, PromQL queries, dashboards, alerts |
| [setup-gcx](skills/setup-gcx/SKILL.md) | Connecting gcx to a Grafana instance and fixing auth or connection errors |
| [manage-dashboards](skills/manage-dashboards/SKILL.md) | Listing, auditing, pulling or snapshotting existing dashboards |
| [create-dashboard](skills/create-dashboard/SKILL.md) | Designing new dashboards or panels with snapshot feedback |
| [debug-with-grafana](skills/debug-with-grafana/SKILL.md) | Investigating slow or failing jobs from metrics |
| [investigate-alert](skills/investigate-alert/SKILL.md) | Understanding why an alert rule fires |

## Planning and GitHub work

Follow [the issue-planning workflow](workflows/issue-planning.md) before refining work or preparing milestones, issues and tasks. Milestones express user-story outcomes; issues carry objectives and acceptance criteria; tasks are native GitHub sub-issues of their issue. Do not add epics by default. Draft for review before publishing, and verify every remote write.

## Knowledge base

Record decisions and learnings as you work; they are the source for the M6 and M7 write-ups. Keep them local-only.

- [Decision log](knowledge/decisions.md): architecture and process decisions with alternatives and consequences. Append entries and mark changed ones superseded.
- [Learnings](knowledge/learnings.md): source-data facts, gotchas and evidence, each labeled verified, reported or external.
- [M7 docs notes](knowledge/m7-docs-notes.md): explanations the user chose to keep for the M7 docs revamp. Add to it only when the user asks.
- [Before submission](knowledge/before-submission.md): the user's personal pre-submission checklist. Remind them of open items whenever submission, the final README or M7 comes up.
- [Archive](knowledge/archive/): finished task logs, kept for history only.

## Local-only files and scope

- Keep all agent-only documentation, learnings, plans and task notes inside `.agents/` or `.atl/`. Never stage, force-add, commit, push or attach them to GitHub.
- Keep source context PDFs only in `.agents/docs/`; do not recreate copies at the repository root. The monitor runbook is [docs/monitoring.md](docs/monitoring.md) (local only).
- `.agents/docs/`, `.atl/`, `.claude/skills`, `.claude/worktrees/` and the legacy `odd/` path are excluded through `.git/info/exclude`. Do not change those exclusions without the user's permission.
- `README.md` is the tracked, human-facing project guide. It must not depend on ignored internal files.

These agent instructions are available only in this checkout, not to other clones.
