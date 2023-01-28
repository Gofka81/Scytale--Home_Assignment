# Scytale Home Assignment
Hi there!

In this home assignment you'll be developing a Spark script to aggregate some data from PRs.

### Instructions
1. In the .zip file you'll find 3 json files:
    1. repos.json -- A list of Github repositories.
    1. prs-e2e.json -- A list of PRs in a repository that's called "e2e-tests".
    1. prs-dart.json -- A list of PRs in a repository that's called "template.dart".
1. Your task is to transform the three separate files into this table using Spark:


| repository_id            | repository_name            | repository_owner                  | num_prs                               | num_prs_closed                               | merged_at                            | is_compliant                                                                |
|--------------------------|----------------------------|-----------------------------------|---------------------------------------|----------------------------------------------|--------------------------------------|-----------------------------------------------------------------------------|
| `raw.id` from repos.json | `raw.name` from repos.json | `raw.owner.login` from repos.json | The number of PRs for each repository | The number of closed PRs for each repository | The last date that a PR was merge in | (`num_prs` == `num_prs_closed`) AND (`repository_owner` contains "scytale") |

#### Notes
1. Not all repositories have a PR.

### What we're looking for
- The code should work!
- Separation of concerns - good directory/file/logical structure.
- Readable code.
- Optimized Spark code.

---
Please send us a zip file/github repo with your solution when you finish.
If you have any questions please don't hesitate to reach us through email or the phone!

Enjoy!

-- *Scytale R&D*
