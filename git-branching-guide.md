# Git Branching and Main: How It Works

## What is `main`?

`main` is the default branch in your repository. Think of it as the **production-ready version** of your code — the source of truth that should always be stable and working.

Everything that lives on `main` has been reviewed and approved.

## What is a Branch?

A branch is a **copy of `main` at a point in time** that lets you work on changes without affecting anyone else. You can think of it like making a photocopy of a document so you can scribble on it freely — the original stays clean.

```
main:       A --- B --- C
                         \
your-branch:              D --- E    (your new work)
```

Commits D and E only exist on `your-branch`. `main` is untouched.

## The Git Flow (Step by Step)

Here's the workflow you just followed in this repo:

### 1. Create a branch from `main`

```bash
git checkout main
git pull origin main          # make sure you have the latest
git checkout -b my-feature    # create and switch to a new branch
```

You're now on `my-feature`, which starts as an exact copy of `main`.

### 2. Make your changes

Edit files, add new ones, delete what you don't need. Your changes only exist on your branch.

### 3. Stage and commit

```bash
git add README.md                  # stage specific files
git commit -m "Rewrite the README" # save a snapshot
```

A **commit** is a saved snapshot of your changes with a message explaining what you did.

### 4. Push your branch to GitHub

```bash
git push -u origin my-feature
```

This uploads your branch to the remote repository so others can see it (and so you can open a Pull Request).

### 5. Open a Pull Request (PR)

On GitHub, you create a PR that says: "I want to merge `my-feature` into `main`." This is where:

- Others can **review** your code
- Automated **tests** can run
- You can **discuss** changes before they go live

### 6. Merge the PR

Once approved, you click **Merge** on GitHub. Your commits from `my-feature` are now part of `main`:

```
main:       A --- B --- C --- D --- E
```

### 7. Clean up

After merging, delete the branch — it served its purpose:

```bash
git checkout main
git pull origin main                    # get the merged changes
git branch -d my-feature                # delete local branch
git push origin --delete my-feature     # delete remote branch
```

## Why Not Just Work on `main`?

| Working on `main` directly | Using branches |
|---|---|
| Every save affects the "real" code | Changes are isolated until ready |
| Hard to undo mistakes | Easy to throw away a branch |
| Can't review before merging | PRs enable code review |
| Conflicts with teammates constantly | Each person works independently |

## Visual Summary

```
main         *---*---*-----------*---*
                      \         /
feature-branch         *---*---*
                       ^       ^
                    branch   merge
                    created  via PR
```

## Common Commands Cheat Sheet

| Command | What it does |
|---------|-------------|
| `git checkout -b name` | Create a new branch and switch to it |
| `git add file` | Stage a file for the next commit |
| `git commit -m "msg"` | Save staged changes as a commit |
| `git push -u origin name` | Push branch to GitHub |
| `git checkout main` | Switch back to main |
| `git pull origin main` | Get latest changes from GitHub |
| `git branch -d name` | Delete a local branch |
| `git log --oneline` | See commit history |

## What You Just Experienced

1. You started on `main`
2. A branch (`claude/lucid-feynman-DpqQM`) was created
3. The README was rewritten and committed on that branch
4. The branch was pushed to GitHub
5. You opened a PR and merged it into `main`
6. The branch was deleted

That's the entire cycle. Every feature, fix, or change follows this same loop.
