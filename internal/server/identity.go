package server

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/mail"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"ganigeorgiev.com/pocketbase/dbx"
	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/router"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/security"
	"github.com/golang-jwt/jwt/v5"
)

// IdentityConfig explicitly trusts one issuer for an application. Identity
// admission does not confer roles or restore application memberships.
type IdentityConfig struct {
	Issuer            string `json:"issuer"`
	Audience          string `json:"audience"`
	WorkspaceDomain   string `json:"workspaceDomain"`
	AllowProvisioning bool   `json:"allowProvisioning"`
}

func secureOrigin(s string) bool {
	u, err := url.Parse(s)
	return err == nil && u.Scheme == "https" && u.Hostname() != "" && u.User == nil && s == "https://"+u.Host
}

var identityDomainLabel = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

func (c IdentityConfig) validate(collection string) error {
	if collection != "users" || !secureOrigin(c.Issuer) || !secureOrigin(c.Audience) {
		return errors.New("identity requires default users and exact HTTPS issuer/audience origins without trailing slashes")
	}
	if len(c.WorkspaceDomain) > 253 || !strings.Contains(c.WorkspaceDomain, ".") {
		return errors.New("identity workspaceDomain must be a lowercase DNS domain")
	}
	for _, label := range strings.Split(c.WorkspaceDomain, ".") {
		if !identityDomainLabel.MatchString(label) {
			return errors.New("identity workspaceDomain must be a lowercase DNS domain")
		}
	}
	return nil
}

type assertionClaims struct {
	jwt.RegisteredClaims
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Domain        string `json:"hd"`
	Name          string `json:"name,omitempty"`
}

type identityVerifier struct {
	config    IdentityConfig
	client    *http.Client
	mu        sync.Mutex
	keys      map[string]*rsa.PublicKey
	fetched   time.Time
	attempted time.Time
}

func newIdentityVerifier(c IdentityConfig) *identityVerifier {
	return &identityVerifier{config: c, client: &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("identity redirects forbidden") }}}
}

// Refresh is serialized and bounded. An unknown kid can trigger at most one
// request per 10 seconds; cached keys are usable for at most five minutes.
func (v *identityVerifier) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.mu.Lock()
	defer v.mu.Unlock()
	now := time.Now()
	if k := v.keys[kid]; k != nil && now.Sub(v.fetched) < 5*time.Minute {
		return k, nil
	}
	if now.Sub(v.attempted) < 10*time.Second {
		return nil, errors.New("identity key unavailable")
	}
	v.attempted = now
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.config.Issuer+"/.well-known/jwks.json", nil)
	if err != nil {
		return nil, errors.New("identity key unavailable")
	}
	req.Header.Set("User-Agent", "PocketContext/identity-jwks")
	req.Header.Set("Accept", "application/json")
	resp, err := v.client.Do(req)
	if err != nil {
		return nil, errors.New("identity key unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("identity key unavailable")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(raw) > 65536 {
		return nil, errors.New("invalid identity keys")
	}
	var set struct {
		Keys []struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		} `json:"keys"`
	}
	if json.Unmarshal(raw, &set) != nil || len(set.Keys) == 0 || len(set.Keys) > 16 {
		return nil, errors.New("invalid identity keys")
	}
	keys := map[string]*rsa.PublicKey{}
	for _, k := range set.Keys {
		if k.Kty != "RSA" || k.Alg != "RS256" || k.Use != "sig" || k.Kid == "" || len(k.Kid) > 255 || keys[k.Kid] != nil {
			return nil, errors.New("invalid identity key")
		}
		nb, err := base64.RawURLEncoding.DecodeString(k.N)
		if err != nil {
			return nil, errors.New("invalid RSA modulus")
		}
		eb, err := base64.RawURLEncoding.DecodeString(k.E)
		if err != nil || len(eb) == 0 || len(eb) > 4 {
			return nil, errors.New("invalid RSA exponent")
		}
		n := new(big.Int).SetBytes(nb)
		e := new(big.Int).SetBytes(eb).Int64()
		if n.BitLen() < 2048 || n.BitLen() > 8192 || e < 3 || e > 2147483647 || e%2 == 0 {
			return nil, errors.New("invalid RSA key")
		}
		keys[k.Kid] = &rsa.PublicKey{N: n, E: int(e)}
	}
	v.keys = keys
	v.fetched = now
	if k := keys[kid]; k != nil {
		return k, nil
	}
	return nil, errors.New("unknown identity key")
}

func (v *identityVerifier) verify(ctx context.Context, raw string) (*assertionClaims, error) {
	if len(raw) == 0 || len(raw) > 16384 {
		return nil, errors.New("invalid assertion size")
	}
	c := new(assertionClaims)
	_, err := jwt.ParseWithClaims(raw, c, func(t *jwt.Token) (any, error) {
		kid, ok := t.Header["kid"].(string)
		if !ok || kid == "" || len(kid) > 255 {
			return nil, errors.New("invalid assertion kid")
		}
		// Never follow token-supplied key URLs or accept extension semantics.
		for _, h := range []string{"jku", "jwk", "x5u", "crit"} {
			if _, ok := t.Header[h]; ok {
				return nil, errors.New("unsupported assertion header")
			}
		}
		return v.key(ctx, kid)
	}, jwt.WithValidMethods([]string{"RS256"}), jwt.WithIssuer(v.config.Issuer), jwt.WithAudience(v.config.Audience), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithLeeway(5*time.Second))
	if err != nil {
		return nil, errors.New("invalid identity assertion")
	}
	now := time.Now()
	if c.IssuedAt == nil || c.ExpiresAt == nil || !c.ExpiresAt.After(now) || c.ExpiresAt.Sub(c.IssuedAt.Time) > 60*time.Second || !c.ExpiresAt.After(c.IssuedAt.Time) || len(c.Audience) != 1 || c.Subject == "" || len(c.Subject) > 255 || len(c.ID) < 16 || len(c.ID) > 255 || !c.EmailVerified || c.Domain != v.config.WorkspaceDomain || len(c.Email) > 254 {
		return nil, errors.New("invalid identity claims")
	}
	parsed, err := mail.ParseAddress(c.Email)
	parts := strings.Split(c.Email, "@")
	if err != nil || parsed.Address != c.Email || len(parts) != 2 || strings.ToLower(parts[1]) != v.config.WorkspaceDomain {
		return nil, errors.New("invalid identity email")
	}
	return c, nil
}

const identityExpiryKey = "pocketcontext.identity.expires"
const identityTokenClaim = "context_identity"

// The middleware has authenticated the bearer, but verify the signature again
// before interpreting its provenance. No client header can set this state.
func identityTokenExpiry(e *core.RequestEvent) time.Time {
	if e.Auth == nil || e.Auth.Collection().Name != "users" {
		return time.Time{}
	}
	raw := e.Request.Header.Get("Authorization")
	if len(raw) > 7 && strings.EqualFold(raw[:7], "Bearer ") {
		raw = raw[7:]
	}
	// Check the signature without checking expiry again: a bearer may expire
	// between the authentication middleware and this hook. Returning no expiry
	// in that case would incorrectly mint an uncapped derived credential.
	parsed, err := jwt.Parse(raw, func(*jwt.Token) (any, error) {
		return []byte(e.Auth.TokenKey() + e.Auth.Collection().AuthToken.Secret), nil
	}, jwt.WithValidMethods([]string{"HS256"}), jwt.WithoutClaimsValidation())
	if err != nil {
		return time.Time{}
	}
	claims, ok := parsed.Claims.(jwt.MapClaims)
	if !ok || claims[identityTokenClaim] != true {
		return time.Time{}
	}
	expires, err := claims.GetExpirationTime()
	if err != nil || expires == nil {
		return time.Time{}
	}
	return expires.Time
}

func registerIdentity(app core.App, e *core.ServeEvent, cfg IdentityConfig) error {
	return registerIdentityVerifier(app, e, cfg, newIdentityVerifier(cfg))
}

func registerIdentityVerifier(app core.App, e *core.ServeEvent, cfg IdentityConfig, verifier *identityVerifier) error {
	// These private system tables are never eligible for SQL export or REST access.
	for _, sql := range []string{
		`CREATE TABLE IF NOT EXISTS _context_identity_bindings (issuer TEXT NOT NULL, subject TEXT NOT NULL, record_id TEXT NOT NULL, PRIMARY KEY (issuer,subject), UNIQUE (issuer,record_id))`,
		`CREATE TABLE IF NOT EXISTS _context_identity_replays (issuer TEXT NOT NULL, jti TEXT NOT NULL, expires INTEGER NOT NULL, PRIMARY KEY (issuer,jti))`,
	} {
		if _, err := app.DB().NewQuery(sql).Execute(); err != nil {
			return fmt.Errorf("initialize identity state: %w", err)
		}
	}
	app.OnRecordAuthRequest("users").BindFunc(func(event *core.RecordAuthRequestEvent) error {
		if expires, ok := event.Get(identityExpiryKey).(time.Time); ok {
			remaining := time.Until(expires)
			if remaining <= 0 || remaining > 15*time.Minute {
				return event.UnauthorizedError("Identity assertion expired.", nil)
			}
			token, err := event.Record.NewStaticAuthToken(remaining)
			if err != nil {
				return event.InternalServerError("Cannot issue identity token.", nil)
			}
			key := event.Record.TokenKey() + event.Record.Collection().AuthToken.Secret
			claims, err := security.ParseJWT(token, key)
			if err != nil {
				return event.InternalServerError("Cannot issue identity token.", nil)
			}
			claims[identityTokenClaim] = true
			claims["exp"] = expires.Unix()
			token, err = security.NewJWT(claims, key, remaining)
			if err != nil {
				return event.InternalServerError("Cannot issue identity token.", nil)
			}
			event.Token = token
		}
		return event.Next()
	})
	app.OnFileTokenRequest().BindFunc(func(event *core.FileTokenRequestEvent) error {
		if expires := identityTokenExpiry(event.RequestEvent); !expires.IsZero() {
			key := event.Record.TokenKey() + event.Record.Collection().FileToken.Secret
			claims, err := security.ParseJWT(event.Token, key)
			if err != nil {
				return event.InternalServerError("Cannot issue file token.", nil)
			}
			fileExpiry, err := claims.GetExpirationTime()
			if err != nil || fileExpiry == nil {
				return event.InternalServerError("Cannot issue file token.", nil)
			}
			if expires.Before(fileExpiry.Time) {
				claims["exp"] = expires.Unix()
				event.Token, err = security.NewJWT(claims, key, time.Until(expires))
				if err != nil {
					return event.InternalServerError("Cannot issue file token.", nil)
				}
			}
		}
		return event.Next()
	})
	app.OnRealtimeSubscribeRequest().BindFunc(func(event *core.RealtimeSubscribeRequestEvent) error {
		expires := identityTokenExpiry(event.RequestEvent)
		if !expires.IsZero() {
			// Set the limit before PocketBase attaches auth. On rejection retain
			// this conservative limit rather than racing another subscription.
			event.Client.Set(identityExpiryKey, expires)
			return event.Next()
		}
		err := event.Next()
		if err == nil {
			// Only a successful independent fallback may remove the limit.
			event.Client.Set(identityExpiryKey, time.Time{})
		}
		return err
	})
	app.OnRealtimeMessageSend().BindFunc(func(event *core.RealtimeMessageEvent) error {
		// Connection setup and provider callback delivery are anonymous control
		// messages; an expired app session must not prevent fresh OAuth login.
		if event.Message != nil && (event.Message.Name == "PB_CONNECT" || event.Message.Name == "@oauth2") {
			return event.Next()
		}
		if expires, ok := event.Client.Get(identityExpiryKey).(time.Time); ok && !expires.IsZero() && !time.Now().Before(expires) {
			return nil // a cached realtime auth record must never outlive its bearer
		}
		return event.Next()
	})
	e.Router.POST("/api/context/auth-with-assertion", func(event *core.RequestEvent) error {
		event.Response.Header().Set("Cache-Control", "no-store")
		event.Request.Body = http.MaxBytesReader(event.Response, event.Request.Body, 18000)
		rawBody, readErr := io.ReadAll(event.Request.Body)
		event.Request.Body.Close()
		if readErr != nil {
			return event.BadRequestError("Invalid assertion request.", nil)
		}
		event.Request.Body = &router.RereadableReadCloser{ReadCloser: io.NopCloser(bytes.NewReader(rawBody))}
		defer event.Request.Body.Close()
		var body struct {
			Assertion string `json:"assertion"`
		}
		decoder := json.NewDecoder(bytes.NewReader(rawBody))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&body) != nil || decoder.Decode(new(any)) != io.EOF {
			return event.BadRequestError("Expected one assertion.", nil)
		}
		claims, err := verifier.verify(event.Request.Context(), body.Assertion)
		if err != nil {
			return event.UnauthorizedError("Invalid identity assertion.", nil)
		}
		expires := claims.IssuedAt.Add(15 * time.Minute)
		if localLimit := time.Now().Add(15 * time.Minute); expires.After(localLimit) {
			expires = localLimit
		}
		event.Set(identityExpiryKey, expires)
		event.Set("contextIdentityEmail", claims.Email)
		return app.RunInTransaction(func(tx core.App) error {
			if !claims.ExpiresAt.After(time.Now()) {
				return event.UnauthorizedError("Identity assertion expired.", nil)
			}
			record, err := identityRecord(tx, cfg, claims)
			if err != nil {
				return event.ForbiddenError("Identity is not admitted to this application.", nil)
			}
			if _, err = tx.DB().NewQuery(`DELETE FROM _context_identity_replays WHERE expires < {:now}`).Bind(dbx.Params{"now": time.Now().Unix()}).Execute(); err != nil {
				return err
			}
			if _, err = tx.DB().NewQuery(`INSERT INTO _context_identity_replays (issuer,jti,expires) VALUES ({:issuer},{:jti},{:expires})`).Bind(dbx.Params{"issuer": cfg.Issuer, "jti": claims.ID, "expires": claims.ExpiresAt.Unix()}).Execute(); err != nil {
				return event.UnauthorizedError("Identity assertion already used.", nil)
			}
			original := event.App
			event.App = tx
			defer func() { event.App = original }()
			return apis.RecordAuthResponse(event, record, "context-identity", nil)
		})
	})
	return nil
}

func identityRecord(app core.App, cfg IdentityConfig, c *assertionClaims) (*core.Record, error) {
	var bindings []struct {
		RecordID string `db:"record_id"`
	}
	if err := app.DB().NewQuery(`SELECT record_id FROM _context_identity_bindings WHERE issuer={:issuer} AND subject={:subject}`).Bind(dbx.Params{"issuer": cfg.Issuer, "subject": c.Subject}).All(&bindings); err != nil {
		return nil, err
	}
	matches, err := app.FindRecordsByFilter("users", "email:lower = {:email}", "", 2, 0, dbx.Params{"email": strings.ToLower(c.Email)})
	if err != nil || len(matches) > 1 {
		return nil, errors.New("ambiguous identity")
	}
	var record *core.Record
	if len(bindings) > 0 {
		record, err = app.FindRecordById("users", bindings[0].RecordID)
		if err != nil || len(matches) != 1 || matches[0].Id != record.Id {
			return nil, errors.New("identity binding changed")
		}
	} else if len(matches) == 1 {
		record = matches[0]
	} else {
		if !cfg.AllowProvisioning {
			return nil, errors.New("identity provisioning disabled")
		}
		collection, err := app.FindCollectionByNameOrId("users")
		if err != nil {
			return nil, err
		}
		record = core.NewRecord(collection)
		record.SetEmail(c.Email)
		record.SetVerified(true)
		record.SetPassword(security.RandomString(64))
		if collection.Fields.GetByName("name") != nil {
			name := strings.TrimSpace(c.Name)
			if len(name) > 200 {
				name = ""
			}
			if name == "" {
				name = strings.Split(c.Email, "@")[0]
			}
			record.Set("name", name)
		}
		if err := app.Save(record); err != nil {
			return nil, err
		}
	}
	if record.GetBool("disabled") || !record.Verified() {
		return nil, errors.New("disabled or unverified identity")
	}
	if len(bindings) == 0 {
		_, err := app.DB().NewQuery(`INSERT INTO _context_identity_bindings (issuer,subject,record_id) VALUES ({:issuer},{:subject},{:record})`).Bind(dbx.Params{"issuer": cfg.Issuer, "subject": c.Subject, "record": record.Id}).Execute()
		if err != nil {
			return nil, errors.New("identity already bound")
		}
	}
	return record, nil
}
