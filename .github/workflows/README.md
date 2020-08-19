# Workflow
## auto-merge
This workflow is triggered hourly to auto-merge `HEAD` to `BASE` (now `branch-0.2` to `branch-0.3`).  
If auto-merge PR is unable to be merged due to conflicts, it will remain open until being manually fixed.
