# Runtime maintenance freeze

PocketContext provides `GET` and `PUT /api/context/maintenance`, authenticated
with an existing PocketBase superuser token. Ordinary users cannot change or
inspect maintenance state. The controller is per process and data directory;
it is not a distributed lease or a fence against another process.

GET returns `state` (`writable`, `draining`, or `read_only`), `generation`, and
`activeOperations` (admitted database operations, transactions and open result
sets still being drained). New read-only operations during draining are excluded
so status and superuser authentication remain available.
PUT accepts exactly a boolean `readOnly` and an integer `expectedGeneration`:

```json
{"readOnly": true, "expectedGeneration": 0}
```

Read the current generation before each transition. Stale requests return 409.
A successful freeze returns only after admitted HTTP mutations and database
operations have drained. New mutations receive 503 with code
`maintenance_read_only`. A transition has a 30-second request timeout; timeout
leaves admission closed. Inspect GET and retry with the current generation.
Setting `readOnly` to false explicitly resumes writes. Do not do this on an old
migration source after the destination has become authoritative.

The marker `maintenance.json` lives in the PocketBase data directory, outside
SQLite. It is atomically replaced, fsynced, and private. Preserve it when moving
or backing up the directory; Litestream's database replica does not include it.
An existing malformed marker prevents startup. A frozen restart requires existing
compatible databases; missing databases and unapplied migrations fail closed.
Application startup hooks must not apply configuration or provision users while
`app.Store().Get("pocketcontextMaintenanceReadOnly")` is true (JS:
`app.store().get(...)`). Normal application commands have no database-write bypass.

The database driver accounts for transactions and streaming results, checks
cached statement execution, and applies `query_only` on each connection before
use. It rejects application attempts to disable enforcement and unmanaged SQL
transaction commands. Use the normal Go/dbx transaction APIs. Read-only query
engines remain separate; disposable filtered-snapshot construction is unaffected.
This guarantees no committed changes through the managed persistent database
connections after a completed freeze, not byte-for-byte immutability of SQLite
WAL/SHM files or physical read-only filesystem access.

Allowed HTTP operations include reader assets, record reads, original downloads,
SQL/search, file tokens, realtime subscriptions and authentication refresh.
Password/OAuth login, onboarding, OTP, account mutation, uploads, thumbnail
generation, schema/settings changes, batch operations and custom mutations are
blocked. Obtain and retain a valid superuser token before freezing; refresh it
before expiry. No automatic timeout unlock or unauthenticated recovery API exists.

HTTP admission protects standard synchronous request file operations. Application
hooks must not perform external mutations on reader routes or detach untracked
work. Arbitrary shell execution, separately opened databases, external processes,
backup/archive publishing, Litestream replication and custom object-store clients
are outside the controller. Persistent log entries are dropped during maintenance;
cleanup writes are rejected by the database gate. Applications must validate their authentication hooks and
background jobs before adopting this server pin.

For migration, freeze first, then create a consistent SQLite backup and verify
all referenced originals. Separately synchronize and verify the replica, fence
the old process/publisher/CD, and only then activate the destination. A completed
freeze does not prove remote replication or backup completeness.
