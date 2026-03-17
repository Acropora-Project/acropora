# conftest.py
# Workspace root conftest — anchors pytest collection for the monorepo.

collect_ignore_glob = [
    "packages/acropora-client/tests/*",
    "packages/acropora-ingest/tests/*",
    "packages/acropora-node/tests/*",
    "packages/acropora-registry/tests/*",
    "tools/acropora-query/tests/*",
    "tools/acropora-dashboard/tests/*",
]
