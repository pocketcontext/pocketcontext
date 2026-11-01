# Hosted identity assertions

Hosted identity is opt-in per application. Add this to `pocketcontext.json` only
after reviewing the issuer and the application's account-creation hooks:

```json
"identity": {
  "issuer": "https://id.example.com",
  "audience": "https://app.example.com",
  "workspaceDomain": "example.com",
  "allowProvisioning": true
}
```

The auth collection must be the existing default `users`. Issuer and audience
must be exact HTTPS origins without trailing slashes, paths, credentials, query
parameters or fragments. There is no insecure HTTP mode. Configuration is
operator-controlled; clients cannot choose an issuer or key endpoint.

`POST /api/context/auth-with-assertion` accepts one JSON object with an `assertion`
string and returns PocketBase's normal `{record, token}` auth response. The
response has `Cache-Control: no-store`. Requests are limited to 18,000 bytes and
assertions to 16,384 bytes. Unknown request fields are rejected. The route does
not exist when identity is unconfigured. Clients must explicitly select direct
application OAuth fallback; a failed assertion must never trigger fallback.

The assertion must be an RS256 JWT with a nonempty `kid` header. Keys come only
from `<issuer>/.well-known/jwks.json` over verified HTTPS, without redirects. The
JWKS must contain at most 16 unique RSA signing keys with `alg=RS256`, `use=sig`
and a 2,048–8,192-bit modulus. Token-supplied key URLs and critical extensions
are rejected. A five-minute cache bounds key retention; an unknown key can cause
one refresh per ten seconds. Failed refreshes do not permit expired cache use.
Publish new public keys before signing with them and overlap old/new keys during
rotation. Removing a key can take up to five minutes to reach every app cache.

Required claims:

- `iss`: configured issuer; `aud`: exactly one configured application origin.
- `sub`: stable issuer account ID, at most 255 characters.
- `email`: verified account email with the exact configured domain (case ignored).
- `email_verified`: JSON `true`; `hd`: exact configured Workspace domain.
- `iat` and `exp`: required timestamps; expiry strictly after issuance and no more
  than 60 seconds later. Expired assertions are rejected without expiry leeway.
  The JWT validator permits five seconds of clock skew for future `iat`/`nbf`.
- `jti`: unique, unpredictable assertion ID of 16–255 characters.
- `name`: optional display name, used only for initial account creation.

The first login links a subject to an existing, verified, enabled account by
case-insensitive email. Ambiguous email matches, an already-bound account with a
new subject, or a bound subject with a changed email are rejected. Account email
changes need deliberate operator coordination; assertions never silently switch
accounts. If `allowProvisioning` is true, a previously unknown identity creates
an ordinary user with a random password, verified email and optional name. It
never accepts client-selected IDs, roles or memberships. If false, only existing
accounts can sign in. Existing unverified accounts need the application's normal
verification procedure before initial linking.

Model creation hooks run during JIT. Existing accounts are not saved during
login, so login does not restore revoked memberships or overwrite policy fields.
PocketBase's collection AuthRule and auth-response hooks remain authoritative.
The trusted hook method is `context-identity`; `e.get('contextIdentityEmail')`
contains the signed, validated email. Application hooks may use this method and
context value to perform the same permitted initial-login work as direct OAuth.
The endpoint does not implement a second-factor ceremony: applications requiring
PocketBase MFA fail closed and need a separate integration before adoption.

Subject bindings and replay reservations persist in private system tables
`_context_identity_bindings` and `_context_identity_replays` in the application
database. They are unavailable through SQL export and collection REST APIs.
Binding, JIT, replay reservation and application auth hooks share one transaction;
a failed auth hook rolls them all back. Expired replay entries are pruned during
successful exchange. Ordinary account deletion/recreation cannot rebind a subject.
Backups must include these tables; restoring an older backup can rewind replay
history for assertions that have not expired yet, so keep service stopped for at
least 60 seconds before exposing a restored database.

App tokens are nonrefreshable PocketBase auth tokens marked `context_identity`.
They expire no later than assertion `iat + 900` seconds, including assertion
redemption time. Calling ordinary `auth-refresh` returns the same token and
expiry; renewal requires another assertion from the identity service. Derived
protected-file tokens cannot outlive that bearer. Realtime subscriptions retain
the bearer expiry and suppress message delivery after it; clients must authenticate
again with a fresh token. Normal application account disabling/token-key rotation
and all independent file, REST, SQL and membership checks still apply. Requests
already in flight may finish. Direct application OAuth tokens retain their normal
refresh behavior and need independent app-account offboarding.

Tests in `internal/server/identity_test.go` use isolated databases and synthetic
TLS/JWT fixtures. Run `make test` and `make build`, then adopt this server pin only
after each application's policy, client and recovery checks pass.
