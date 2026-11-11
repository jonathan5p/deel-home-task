# Refine and publish project work

Use this convention whenever refining work or preparing milestones, issues, and task checklists for this repository. Draft first; publish only after the human approves publication. Planning approval is not implementation approval.

All agent-only planning, ODD notes, and learnings belong inside ignored `.agents/` or `.atl/`, never in tracked `odd/` or public agent-documentation folders. Never stage, force-add, commit, or upload internal files or source PDFs. Approved issue bodies may be published as requested; that does not authorize publishing their internal source notes.

## Work hierarchy

| Level | Purpose | Contents |
| --- | --- | --- |
| GitHub Project | Repository-wide work tracking | Reuse the existing private project; add the approved issues. |
| Milestone | A coherent user-story outcome | Outcome-oriented title, user story, completion rule, and non-goals. |
| Issue | An executable piece of the outcome | Objective, observable acceptance criteria, and dependencies. |
| Task | An implementation or verification step | A native GitHub sub-issue of its issue, in the same milestone, with its own objective and done-when criteria. |

Do not add epics or a duplicate parent story issue by default. The milestone expresses the story outcome and groups its issues. Tasks are sub-issues, not checklists. A small issue with a single deliverable needs no sub-issues. Fold documentation steps into the acceptance criteria of the sub-issue they document instead of creating separate documentation tasks. Link sub-issues through the REST sub-issues API, which takes the child's numeric issue ID, not its number. Read back each parent's sub-issue list after linking.

The "Further improvements" milestone is an open backlog for experiments and next steps. Its issues get sub-issues only when they are picked up.

## 1. Understand and refine

- Read the assignment, repository guidance, and relevant existing code before proposing work.
- Keep assignment requirements distinct from role context and optional improvements.
- Inspect reference projects only within the paths the human authorized. A script pointing to a sibling directory does not authorize reading that directory.
- Separate source facts, observed runtime evidence, assumptions, and unresolved decisions. Static configuration checks do not prove a working pipeline.
- Ask a focused UI question only for a consequential unresolved choice. Do not ask again for permission already granted in the current scope.
- Prefer small, outcome-based issues. Do not expand local setup into pipeline implementation, cloud deployment, or unrelated fixes.

## 2. Draft for review

Present the milestone and issue bodies in the conversation before publishing. Do not create staging documents or remote objects for a draft-only request.

Milestone template:

```markdown
## User story
As a <role>, I want <capability>, so that <benefit>.

## Complete when
All associated issues meet their acceptance criteria.

## Out of scope
<Explicit boundaries.>
```

Issue template (use the repository's own form instead when required):

```markdown
**Depends on:** <Issue link, when applicable.>

## Objective
<One observable outcome.>

## Acceptance criteria
- <Observable condition and the evidence that establishes it.>
```

Sub-issue (task) template:

```markdown
**Parent:** <Parent issue link.>

## Objective
<One concrete step.>

## Done when
- <Observable condition, including any documentation it requires.>
```

Keep acceptance criteria measurable without invented throughput, row-count, or latency targets. Record dependencies using actual issue links once available; a text link is not a native GitHub blocking relationship. Do not invent issue numbers, Jira IDs, deadlines, assignees, or labels.

## 3. Authorize and check publication

- Confirm the exact GitHub host, repository, account/session, and requested operations. Creating issues does not automatically authorize changing repository settings, visibility, labels, or workflows.
- Discover the CLI through PATH or a human-provided executable path. Git SSH access does not imply GitHub API authentication; do not request tokens in chat or print them.
- Verify the authenticated actor, target repository, permissions, Issues availability, contribution rules, issue forms, and permitted labels before writing remotely.
- If Issues are disabled, request permission before enabling them. If a required permission is unavailable, report the blocker rather than probing with a write.
- Search open and closed issues, existing milestones, and relevant projects for duplicates. Reuse an exact matching grouping where appropriate; do not change unrelated projects.
- Honor repository issue forms. Use the structured issue template above only when no required form applies and blank issues are explicitly enabled.
- Reconcile material changes with the reviewed draft. Ask again only when scope or the publication decision changes.

## 4. Publish and verify incrementally

1. Create the approved milestone, then read back its title, description, state, and URL.
2. Reuse the approved project or create it when requested. Verify its owner, visibility, and repository link.
3. Privacy-scan each exact issue body immediately before publication. Remove credentials, personal filesystem paths, private hostnames, and unrelated identifying details while preserving useful reproduction information.
4. Use `gh issue create --body-file` with a reviewed body. When temporary files are needed, use an authorized location, owner-only permissions, no stored tokens, and cleanup on success or failure.
5. Publish each issue once. Read it back from the target host and compare its title and full body, allowing only CRLF/LF and trailing-final-newline differences. Verify state, labels, and milestone before continuing.
6. Add the confirmed issues to the approved project and read back membership, issue identity, milestone assignment, project visibility, and repository link.

Do not mark tasks complete, close issues, change status fields, or assign labels merely because planning objects were created. These are separate actions requiring evidence and applicable authorization.

## 5. Handle uncertain results without duplicates

- A timeout, error, or failed readback after a write is not proof that the write failed. Stop later mutations and report what is confirmed versus uncertain; never blindly retry creation.
- Preserve returned issue URLs, milestone numbers, project IDs, and project-item IDs for recovery. Do not persist credentials.
- After authorized recovery, inspect the exact object before deciding what remains. Do not repeat a mutation already confirmed as successful.
- A project-list readback can temporarily fail to show a newly returned item. Verify the returned item ID directly, including its owning project and issue; do not add the same issue again to force visibility.
- If identity or membership still cannot be established, return one **Needs your decision** result naming the missing proof. Do not loop, poll, or claim completion.

## 5b. Refine published work with a recorded history

Never change a published milestone or issue silently.

- When editing an issue's title, body, milestone or parent, post a comment in this form: `**Refinement <date>**`, then **What changed:**, then **Why:**, then `Planning log: #74`. GitHub's edit history keeps the previous text.
- Move a task to another parent with the sub-issues API and `replace_parent: true`, and update its `**Parent:**` line. Do not close and recreate it.
- Close superseded issues with `state_reason: not_planned`, after a comment that links the replacement.
- Milestones cannot take comments. Record milestone-level changes (new, renamed, renumbered, re-scoped) as a dated entry on the pinned planning log, #74, with what changed, why, and links.
- Mirror material decisions in `.agents/knowledge/decisions.md`. Mark replaced decisions as superseded instead of rewriting them.

## 6. Close the planning work

Report links to the project, milestone, and issues; confirm grouping and dependencies; disclose failed, unresolved, or pending checks. State whether implementation has started and identify the next executable issue. Do not start it unless implementation is authorized.

## Current example and reusable lessons

- Repository: [jonathan5p/deel-home-task](https://github.com/jonathan5p/deel-home-task).
- Private project: [Deel Home Task, project 2](https://github.com/users/jonathan5p/projects/2).
- Milestone: [Run the project locally](https://github.com/jonathan5p/deel-home-task/milestone/1).
- Issues: [Validate the supplied stack locally](https://github.com/jonathan5p/deel-home-task/issues/1), then [Add an optional Conduktor monitor](https://github.com/jonathan5p/deel-home-task/issues/2).
- The first issue verifies the unchanged baseline; the second adds an opt-in capability without changing default startup or deleting baseline data on shutdown.
- Initial project-membership verification failed after an item ID was returned. Subsequent authorized reconciliation and direct item-ID readback confirmed membership without repeating the successful write.
- Milestone titles are prefixed with their number: `M<n> · <Title>` (for example `M2 · Platform foundation`). New milestones follow the same pattern.
- Milestones 2–7 (Platform foundation through Further improvements) were published with 24 issues and 42 linked sub-issues (#4–#69). Milestone 1 was then closed.
- Project 2 appears to auto-add some new repository issues. `gh project item-add` then fails with "Content already exists in this project". That error means the issue is already a member, not that the write failed. Verify membership by listing items, or for a single issue through GraphQL `issue.projectItems`. The item list can lag for the newest item.
- Issue creation in bulk: create the parent first, then each sub-issue. Link each sub-issue with `POST /repos/{owner}/{repo}/issues/{parent}/sub_issues` using `-F sub_issue_id=<numeric id>`. Read back each parent's sub-issue list. Pause about one second between content-creating writes.
- These links record past work, not standing permission or proof of current remote state. Revalidate before future mutations.

This workflow is local-only while `.agents/` and `AGENTS.md` remain excluded. Agents in another clone will not receive it until the human authorizes sharing these files.
