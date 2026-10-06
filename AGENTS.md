# Personal fork maintenance

- This repository is the maintained fork of `shuishuipingan/qoder2api-hub`.
- Check Git status before editing. Preserve unrelated work and keep patches scoped.
- Read `docs/FORK_MAINTENANCE.md` for baseline, configuration and release gates.
- Runtime account files, API keys, panel settings and usage records are private.
  Use synthetic accounts and temporary directories for tests; do not inspect or
  publish production data. Disable desktop discovery and scheduling in isolation.
- Run `python tests/run_offline.py` after protocol, account or scheduler changes.
  Report skipped fixture checks separately from passed assertions.
- For Responses, verify streamed/final IDs, concatenated function/custom deltas,
  namespace restoration, tool-result history and terminal event consistency.
- For check-in, verify the UTC+8 10:00 activity window across restarts and bounded
  failures. Status reads must not trigger claims or refreshes.
- Preserve the upstream license and record design sources. Keep offline, local
  HTTP and real upstream validation results distinct.
- Do not deploy, restart or change another repository's configuration unless that
  operation is included in the current user task. Publication and deployment gates
  follow the user's authorization and the actual test results.
