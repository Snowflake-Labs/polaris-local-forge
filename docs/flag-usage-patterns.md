# L2C Flag Usage Patterns

This guide explains when and why to use `--force` and `--yes` flags with L2C commands.

## Quick Reference

| Scenario | Command | Flags | Why |
|----------|---------|-------|-----|
| **Initial migration** | `plf l2c migrate` | `--yes` | Skip confirmations for automation |
| **After demo reset** | `plf l2c migrate` | `--force --yes` | Force sync fresh data + skip confirmations |
| **After local data changes** | `plf l2c update` | `--force --yes` | Force sync mutations + skip confirmations |
| **Regular incremental updates** | `plf l2c update` | `--yes` | Skip confirmations, smart sync is sufficient |
| **Troubleshooting sync issues** | `plf l2c sync` | `--force --yes` | Force re-upload all data + skip confirmations |
| **Refresh all tables** | `plf l2c refresh` | `--force --yes` | Refresh regardless of up-to-date status |
| **Automation/CI** | Any command | `--yes` | Non-interactive execution |
| **Preview changes** | Any command | `--dry-run` | See what would happen without executing |

## The `--yes` Flag

### What it does
- **Skips all interactive confirmations**
- Enables non-interactive/automated execution
- Does NOT change the operation logic

### When to use `--yes`
✅ **Always use in:**
- **Automation/CI pipelines**
- **Cortex Code skills** (non-interactive shell)
- **Batch operations** where you want uninterrupted execution
- **After you've reviewed with `--dry-run`**

✅ **Good for:**
```bash
# After reviewing the plan
plf l2c migrate --dry-run
plf l2c migrate --yes

# In automation
task l2c:migrate WORK_DIR=~/polaris-dev -- --yes

# Cortex Code skills
./bin/plf l2c update --yes
```

❌ **Don't use when:**
- **First time running a command** (review the plan first)
- **Destructive operations** without understanding impact
- **Learning/exploring** (confirmations help you understand)

### When NOT to use `--yes`
```bash
# Bad: First time, don't know what will happen
plf l2c cleanup --force --yes

# Good: Review first, then execute
plf l2c cleanup --force --dry-run
plf l2c cleanup --force --yes
```

## The `--force` Flag

### What it does
- **Overrides smart logic** and safety checks
- **Forces re-execution** even when things appear up-to-date
- **Bypasses optimizations** for completeness

### When to use `--force`

#### 1. **After Demo Reset** 🔄
```bash
# Demo reset loads fresh data (333 rows) into Polaris
# Without --force, sync might skip because S3 has old data
plf l2c migrate --force --yes
```

#### 2. **After Local Data Mutations** 📝
```bash
# You deleted/updated data in Polaris via PyIceberg/DuckDB
# Force ensures all changes get synced to S3
plf l2c update --force --yes
```

#### 3. **Troubleshooting Sync Issues** 🔧
```bash
# Smart sync isn't picking up changes you know exist
plf l2c sync --force --yes
```

#### 4. **Refresh All Tables** 🔄
```bash
# Force refresh all tables regardless of up-to-date status
plf l2c refresh --force --yes
```

#### 5. **Complete Cleanup** 🧹
```bash
# Delete S3 bucket (not just empty it)
plf l2c cleanup --force --yes
```

### When NOT to use `--force`

❌ **Don't use for:**
- **Regular incremental updates** (smart sync is sufficient)
- **Initial migration** (unless after demo reset)
- **Performance-sensitive operations** (force bypasses optimizations)

```bash
# Bad: Unnecessary force for regular updates
plf l2c update --force --yes  # (unless you mutated data)

# Good: Smart sync handles regular incremental updates
plf l2c update --yes
```

## Command-Specific Patterns

### `migrate` - Initial Migration
```bash
# First time (review plan)
plf l2c migrate --dry-run
plf l2c migrate --yes

# After demo reset (force sync fresh data)
plf l2c migrate --force --yes
```

### `update` - Day-2 Operations
```bash
# Regular incremental updates (new tables, small changes)
plf l2c update --yes

# After local data mutations (deletions, updates)
plf l2c update --force --yes
```

### `sync` - Data Synchronization
```bash
# Smart sync (only changed data)
plf l2c sync --yes

# Force sync (all data, bypass smart logic)
plf l2c sync --force --yes
```

### `refresh` - Metadata Updates
```bash
# Refresh only tables with changed metadata
plf l2c refresh --yes

# Force refresh all tables
plf l2c refresh --force --yes
```

### `clear` - Reset State
```bash
# Reset for iteration (keeps files for consistency)
plf l2c clear --yes
```

### `cleanup` - Full Teardown
```bash
# Remove infrastructure, keep S3 bucket
plf l2c cleanup --yes

# Remove everything including S3 bucket (irreversible)
plf l2c cleanup --force --yes
```

## Workflow Patterns

### 🚀 **Initial Setup**
```bash
# 1. Review the plan
plf l2c migrate --dry-run

# 2. Execute
plf l2c migrate --yes
```

### 🔄 **Demo Reset Workflow**
```bash
# 1. Reset demo environment
plf catalog cleanup --yes
plf catalog setup

# 2. Clear L2C table states (important!)
python3 -c "
import json
from pathlib import Path
l2c_state = Path('.snow-utils/l2c-state.json')
if l2c_state.exists():
    with open(l2c_state, 'r') as f: state = json.load(f)
    if 'tables' in state: del state['tables']
    with open(l2c_state, 'w') as f: json.dump(state, f, indent=2)
    print('L2C table states cleared')
"

# 3. Force migrate with fresh data
plf l2c migrate --force --yes
```

**⚠️ Known Issue**: After `catalog verify-sql` (which uses DuckDB), the Iceberg metadata may be stale. Always clear L2C table states and use `--force` for the first sync after demo reset.

### 📝 **Local Data Changes Workflow**
```bash
# 1. Make changes in Polaris (PyIceberg, DuckDB, etc.)
# 2. Force sync to capture all mutations
plf l2c update --force --yes
```

### 🔍 **Troubleshooting Workflow**
```bash
# 1. Check current state
plf l2c status

# 2. Force sync if data seems stale
plf l2c sync --force --yes

# 3. Force refresh if metadata seems stale
plf l2c refresh --force --yes
```

### 🧹 **Cleanup Workflow**
```bash
# 1. Preview what will be removed
plf l2c cleanup --dry-run

# 2. Remove infrastructure (keep S3 for investigation)
plf l2c cleanup --yes

# 3. Complete removal (if you're sure)
plf l2c cleanup --force --yes
```

## Task Integration

### With Go-Task
```bash
# Regular migration
task l2c:migrate WORK_DIR=~/polaris-dev -- --yes

# After demo reset
task l2c:migrate WORK_DIR=~/polaris-dev -- --force --yes

# After local mutations
task l2c:update WORK_DIR=~/polaris-dev -- --force --yes
```

### In Cortex Code Skills
```python
# Always use --yes for non-interactive execution
subprocess.run([plf, "l2c", "migrate", "--force", "--yes"], 
               cwd=project_root, check=True)
```

## Common Mistakes

### ❌ **Mistake 1: Using --force unnecessarily**
```bash
# Bad: Force on every update (slow, unnecessary)
plf l2c update --force --yes

# Good: Force only after local mutations
plf l2c update --yes  # (regular updates)
plf l2c update --force --yes  # (after mutations)
```

### ❌ **Mistake 2: Forgetting --force after demo reset**
```bash
# Bad: Smart sync might use old S3 data
plf catalog cleanup --yes
plf catalog setup
plf l2c migrate --yes  # ← Missing --force!

# Good: Force sync fresh Polaris data
plf l2c migrate --force --yes
```

### ❌ **Mistake 3: Using --yes without understanding**
```bash
# Bad: Destructive operation without review
plf l2c cleanup --force --yes

# Good: Review first
plf l2c cleanup --force --dry-run
# (review output)
plf l2c cleanup --force --yes
```

## Best Practices

1. **🔍 Always review first**: Use `--dry-run` before destructive operations
2. **🤖 Automate with --yes**: Use `--yes` for non-interactive execution
3. **🔄 Force after resets**: Use `--force` after demo resets and local mutations
4. **📊 Check status**: Use `plf l2c status` to understand current state
5. **🎯 Be specific**: Only use `--force` when you need it
6. **📝 Document workflows**: Include flag rationale in scripts/documentation

## Debugging

### If sync isn't picking up changes:
```bash
# 1. Check what sync sees
plf l2c sync --dry-run

# 2. Force sync to bypass smart logic
plf l2c sync --force --yes
```

### If refresh isn't updating tables:
```bash
# 1. Check current metadata paths
plf l2c status -o json

# 2. Force refresh all tables
plf l2c refresh --force --yes
```

### If you're unsure what will happen:
```bash
# Always safe to preview
plf l2c <command> --dry-run
```