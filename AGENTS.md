# Amplifier Development Workspace

@amplifier:docs/MODULES.md
@amplifier:docs/REPOSITORY_RULES.md

---

## This Workspace Uses Submodules

Your code lives HERE as git submodules, NOT in `~/.amplifier/cache/`:

```
./
├── AGENTS.md              # This file
├── SCRATCH.md             # Working memory (create as needed)
├── amplifier/             # Submodule - EDIT HERE
├── amplifier-core/        # Submodule - EDIT HERE
└── amplifier-foundation/  # Submodule - EDIT HERE
```

## Clone Repos Eagerly

When you need to explore or modify ANY Amplifier ecosystem code:

```bash
# Add as submodule immediately - consult MODULES.md for URLs
git submodule add https://github.com/microsoft/amplifier-module-xyz.git
```

**Don't** read from `~/.amplifier/cache/` - that's the installed CLI's runtime code.
**Do** clone repos here where changes are git-tracked.

## Workspace Lifecycle

1. **Create**: `amplifier-dev ~/work/feature-name`
2. **Work**: Changes go in submodules, commit often
3. **Push**: Push submodule changes to their repos
4. **Destroy**: `amplifier-dev -d ~/work/feature-name`

The workspace is disposable. Your work persists because you push to the repos.

## Testing Local Changes

### Option 1: Shadow Environment (Recommended)

For complete isolation, use shadow environments:

```python
# Create isolated environment with your local changes
shadow.create(local_sources=[
    "./amplifier-core:microsoft/amplifier-core",
    "./amplifier-foundation:microsoft/amplifier-foundation"
])

# Test in complete isolation
shadow.exec(shadow_id, "uv tool install git+https://github.com/microsoft/amplifier")
shadow.exec(shadow_id, "amplifier run 'test my changes'")

# Clean up
shadow.destroy(shadow_id)
```

### Option 2: Local Source Overrides

Create `.amplifier/settings.yaml` in this workspace:

```yaml
sources:
  amplifier-core:
    type: local
    path: ./amplifier-core
  amplifier-foundation:
    type: local
    path: ./amplifier-foundation
```

Then `amplifier run` uses your workspace copies.

## Working Memory: SCRATCH.md

For long sessions, maintain a `SCRATCH.md`:
- Current focus (one sentence)
- Key decisions made
- Next actions

Prune aggressively - if it doesn't inform the NEXT action, remove it.

## Project Notes

[Task-specific notes go here]
