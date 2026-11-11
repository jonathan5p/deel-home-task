# How we worked

The project was planned and delivered like a team project on a ticketing board, in the usual Jira-style flow: milestones, issues and sub-issues, each with acceptance criteria, then small pull requests that close them, reviewed on GitHub. Everything is public on the repository, so the path to the final solution can be followed from the first milestone to the last commit.

## Why this flow

- **Small goals are the best way to make progress.** A big task such as "build the gold layer" hides its open questions; split into issues and sub-issues with acceptance criteria, each step has one outcome that is easy to finish, test and review.
- **It is the flow that works best with AI agents.** The project relied heavily on AI coding agents ([building with AI agents](ai-agents.md)). A sub-issue with an objective and observable acceptance criteria is a self-contained brief an agent can deliver, and a small pull request is a change a human can review properly.

## Refine first

Every milestone started with refinement, before any code: read the assignment and the code so far, settle the open questions, then write the work down.

1. **Milestone, the user story.** What the milestone delivers, for whom and why, when it is complete, and what is out of scope.
2. **Issues, the deliverables.** Each issue has an objective, observable acceptance criteria and its dependencies.
3. **Sub-issues, the steps.** Each step is a native GitHub sub-issue of its issue, with its own objective and "done when" criteria. A small issue with a single deliverable has none, and documentation is part of the step it documents rather than a task of its own.

Drafts were reviewed in conversation before anything was published, and every published issue was read back from GitHub to check it.

| Milestone | Outcome |
| --- | --- |
| [M1 · Run the project locally](https://github.com/jonathan5p/deel-home-task/milestone/1) | The supplied stack runs, with an optional Kafka console |
| [M2 · Platform foundation](https://github.com/jonathan5p/deel-home-task/milestone/2) | Spark cluster, analytics database, SeaweedFS and the Iceberg catalog, the pipeline skeleton |
| [M3 · Land raw CDC events in the lake](https://github.com/jonathan5p/deel-home-task/milestone/3) | Every Kafka record kept in the landing zone |
| [M4 · Bronze and silver history in Iceberg](https://github.com/jonathan5p/deel-home-task/milestone/8) | Typed change tables and versioned history |
| [M5 · Gold dimensional model in PostgreSQL](https://github.com/jonathan5p/deel-home-task/milestone/4) | The star schema, the business questions, retention and rebuild |
| [M6 · Serve the metrics](https://github.com/jonathan5p/deel-home-task/milestone/5) | The API and the Grafana dashboard |
| [M7 · Architecture, sizing and evidence](https://github.com/jonathan5p/deel-home-task/milestone/6) | The data quality chain, the sizing strategy and this documentation |
| [M8 · Ideas and experiments](https://github.com/jonathan5p/deel-home-task/milestone/7) | An open backlog of improvements, refined only when picked up |

The issues are also tracked on the [project board](https://github.com/users/jonathan5p/projects/2).

## Plans change, with a record

The plan changed as the project learned: the lake grew from two layers to four ([D17](../architecture/decisions.md#d17--layered-lake-landing-bronze-silver-gold-built-from-silver)), MinIO gave way to SeaweedFS ([D13](../architecture/decisions.md#d13--object-store-seaweedfs-instead-of-minio)), and gold in the lake became views ([D39](../architecture/decisions.md#d39--gold-in-the-lake-as-iceberg-views-over-silver)). No published issue was changed silently:

- an edited issue gets a dated "Refinement" comment that says what changed and why;
- a task that moves is re-parented, not recreated;
- a superseded issue is closed as not planned, with a link to its replacement;
- milestone-level changes, which GitHub cannot comment on, go into the pinned [planning and decision log](https://github.com/jonathan5p/deel-home-task/issues/74), #74.

The design decisions behind those changes are kept in a decision log, summarised on [design decisions](../architecture/decisions.md).

## Deliver in small pull requests

- **One pull request per sub-issue,** never a whole issue in one. Each pull request closes its sub-issue; the last one also closes the parent issue.
- **Stacked pull requests.** Work that depends on other work is opened as a native GitHub stack, one pull request on top of the other, so each one shows only its own change. A stack merges from the bottom up, with merge commits that keep the full history.
- **Conventional Commits,** `type(scope): summary`, small and atomic, each one passing the checks.
- **Checks before every push:** the pre-commit hooks (the lock file, ruff, ty and the unit tests with the 90% coverage floor), the infra tests when a change touches the platform, the data quality checks on the running stack when a change affects the data, and the strict docs build. Hooks are never skipped ([testing and CI](../development/testing.md)).
- **Review on GitHub.** Every pull request was reviewed in GitHub's review threads before it merged. Each comment got a reply naming the commit that addressed it, and a thread was resolved only once the reviewer was satisfied.

An example: [#145](https://github.com/jonathan5p/deel-home-task/issues/145), "Validate each layer against its upstream", was refined into one sub-issue per check: bronze (#146), silver (#147), the schedule (#148) and gold in the lake (#153). Each one was delivered by its own pull request (#151, #179, #154, #180), and the last one also closed #145.
