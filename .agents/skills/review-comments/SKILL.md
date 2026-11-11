---
name: review-comments
description: >
  List, answer and track review feedback on this repository's pull requests.
  Trigger: addressing PR review comments; before any push to a PR; before reporting a PR done;
  the user says "check the comments" or "address the review".
---

Rules: [git and PRs](../../rules/git-and-prs.md#review-comments).

## 1. List what is open

Unresolved review threads:

```sh
gh api graphql -F owner=jonathan5p -F repo=deel-home-task -F pr=<n> -f query='
query($owner: String!, $repo: String!, $pr: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $pr) {
      reviewThreads(first: 100) {
        nodes {
          id isResolved path line
          comments(first: 50) { nodes { databaseId author { login } body createdAt } }
        }
      }
    }
  }
}' --jq '.data.repository.pullRequest.reviewThreads.nodes[] | select(.isResolved | not)'
```

Conversation comments (not tied to a line): `gh api repos/jonathan5p/deel-home-task/issues/<n>/comments`.

The to-do list is every unresolved thread whose last comment is not an agent reply, plus every conversation comment from the user after the last agent reply. Short "remove this" requests count.

## 2. Address each item

1. Fix it in the PR's worktree, commit (agent-chosen Conventional Commits message), run the needed checks, push.
2. Reply to the thread with the commit:

   ```sh
   gh api repos/jonathan5p/deel-home-task/pulls/<n>/comments/<first comment databaseId>/replies \
     -f body=$'🤖 **Agent reply**\nFixed in <sha>: <one line>.'
   ```

   Conversation comments get a new issue comment with the same marker. If you disagree or need a decision, reply with the reason and ask the user through the question UI instead of changing code.

## 3. Re-list and close out

1. Run step 1 again. Only threads already answered by an agent reply may remain.
2. Resolve a thread only after the user approves:

   ```sh
   gh api graphql -f query='mutation($id: ID!) { resolveReviewThread(input: {threadId: $id}) { thread { isResolved } } }' -f id=<thread id>
   ```

3. Report each item with its commit, and anything left for the user.
