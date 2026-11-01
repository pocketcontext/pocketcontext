package server

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"ganigeorgiev.com/pocketbase/pocketbase/apis"
	"ganigeorgiev.com/pocketbase/pocketbase/core"
	"ganigeorgiev.com/pocketbase/pocketbase/tests"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/security"
	"ganigeorgiev.com/pocketbase/pocketbase/tools/subscriptions"
	"github.com/golang-jwt/jwt/v5"
	"github.com/pocketcontext/pocketcontext/internal/database"
)

type identityFixture struct {
	app      *tests.TestApp
	handler  http.Handler
	verifier *identityVerifier
	signer   *rsa.PrivateKey
	claims   assertionClaims
	count    atomic.Int32
}

func newIdentityFixture(t *testing.T, provision bool) *identityFixture {
	t.Helper()
	app, err := tests.NewTestAppWithConfig(core.BaseAppConfig{DataDir: t.TempDir(), DBConnect: database.Connect})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(app.Cleanup)
	users, err := app.FindCollectionByNameOrId("users")
	if err != nil {
		users = core.NewAuthCollection("users")
	}
	users.Fields.Add(&core.BoolField{Name: "disabled"}, &core.TextField{Name: "name"})
	if err := app.Save(users); err != nil {
		t.Fatal(err)
	}
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f := &identityFixture{app: app, signer: key}
	jwks := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.count.Add(1)
		if r.URL.Path != "/.well-known/jwks.json" {
			w.WriteHeader(404)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "key-v1", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(key.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes())}}})
	}))
	t.Cleanup(jwks.Close)
	cfg := IdentityConfig{Issuer: jwks.URL, Audience: "https://app.example.com", WorkspaceDomain: "example.com", AllowProvisioning: provision}
	f.verifier = newIdentityVerifier(cfg)
	f.verifier.client = jwks.Client()
	router, err := apis.NewRouter(app)
	if err != nil {
		t.Fatal(err)
	}
	event := &core.ServeEvent{App: app, Router: router}
	if err := registerIdentityVerifier(app, event, cfg, f.verifier); err != nil {
		t.Fatal(err)
	}
	f.handler, err = router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Second)
	f.claims = assertionClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: cfg.Issuer, Audience: jwt.ClaimStrings{cfg.Audience}, Subject: "person000000001", ID: security.RandomString(32), IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute))}, Email: "person@example.com", EmailVerified: true, Domain: "example.com", Name: "Synthetic Person"}
	return f
}
func (f *identityFixture) assertion(t *testing.T, c assertionClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	tok.Header["kid"] = "key-v1"
	raw, err := tok.SignedString(f.signer)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
func (f *identityFixture) exchange(t *testing.T, c assertionClaims) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"assertion": f.assertion(t, c)})
	return request(f.handler, "POST", "/api/context/auth-with-assertion", "", string(body))
}
func tokenFrom(t *testing.T, r *httptest.ResponseRecorder) string {
	t.Helper()
	if r.Code != 200 {
		t.Fatalf("exchange: %d %s", r.Code, r.Body)
	}
	var body struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(r.Body.Bytes(), &body); err != nil || body.Token == "" {
		t.Fatal("missing token")
	}
	return body.Token
}

func TestIdentityExchangeStaticTokenAndReplay(t *testing.T) {
	f := newIdentityFixture(t, true)
	response := f.exchange(t, f.claims)
	token := tokenFrom(t, response)
	if response.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("token response must not be cached")
	}
	claims, err := security.ParseUnverifiedJWT(token)
	if err != nil {
		t.Fatal(err)
	}
	if claims[core.TokenClaimRefreshable] != false {
		t.Fatal("identity token is refreshable")
	}
	expiry, _ := claims.GetExpirationTime()
	if expiry.Unix() > f.claims.IssuedAt.Unix()+900 || expiry.Unix() < f.claims.IssuedAt.Unix()+898 {
		t.Fatal("incorrect15m expiry")
	}
	for range 3 {
		refreshed := tokenFrom(t, request(f.handler, "POST", "/api/collections/users/auth-refresh", token, `{}`))
		if refreshed != token {
			t.Fatal("ordinary refresh extended identity token")
		}
	}
	if r := f.exchange(t, f.claims); r.Code != 401 {
		t.Fatalf("replay accepted: %d", r.Code)
	}
	user, err := f.app.FindAuthRecordByEmail("users", f.claims.Email)
	if err != nil {
		t.Fatal(err)
	}
	claims["exp"] = time.Now().Add(-time.Second).Unix()
	expired, err := security.NewJWT(claims, user.TokenKey()+user.Collection().AuthToken.Secret, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if r := request(f.handler, "POST", "/api/collections/users/auth-refresh", expired, `{}`); r.Code == 200 {
		t.Fatal("expired static token refreshed")
	}
	fallback, _ := user.NewAuthToken()
	next := tokenFrom(t, request(f.handler, "POST", "/api/collections/users/auth-refresh", fallback, `{}`))
	fallbackClaims, _ := security.ParseUnverifiedJWT(next)
	if fallbackClaims[core.TokenClaimRefreshable] != true {
		t.Fatal("fallback behavior changed")
	}
	if f.count.Load() != 1 {
		t.Fatal("valid key was not cached")
	}
}

func TestIdentityInvalidAssertions(t *testing.T) {
	f := newIdentityFixture(t, true)
	changes := map[string]func(*assertionClaims){
		"issuer":             func(c *assertionClaims) { c.Issuer = "https://wrong.example.com" },
		"audience":           func(c *assertionClaims) { c.Audience = jwt.ClaimStrings{"https://other.example.com"} },
		"multiple audiences": func(c *assertionClaims) { c.Audience = append(c.Audience, "https://other.example.com") },
		"expired":            func(c *assertionClaims) { c.ExpiresAt = jwt.NewNumericDate(time.Now().Add(-time.Second)) },
		"future":             func(c *assertionClaims) { c.IssuedAt = jwt.NewNumericDate(time.Now().Add(time.Minute)) },
		"long validity":      func(c *assertionClaims) { c.ExpiresAt = jwt.NewNumericDate(c.IssuedAt.Add(61 * time.Second)) },
		"no issued at":       func(c *assertionClaims) { c.IssuedAt = nil },
		"unverified":         func(c *assertionClaims) { c.EmailVerified = false },
		"workspace":          func(c *assertionClaims) { c.Domain = "elsewhere.com" },
		"email domain":       func(c *assertionClaims) { c.Email = "person@elsewhere.com" },
		"display email":      func(c *assertionClaims) { c.Email = "Person <person@example.com>" },
		"no subject":         func(c *assertionClaims) { c.Subject = "" },
		"no nonce":           func(c *assertionClaims) { c.ID = "" },
	}
	for name, change := range changes {
		t.Run(name, func(t *testing.T) {
			c := f.claims
			change(&c)
			if r := f.exchange(t, c); r.Code != 401 {
				t.Fatalf("invalid accepted: %d %s", r.Code, r.Body)
			}
		})
	}
	raw := f.assertion(t, f.claims)
	raw = raw[:len(raw)-20] + strings.Repeat("x", 20)
	body, _ := json.Marshal(map[string]string{"assertion": raw})
	if r := request(f.handler, "POST", "/api/context/auth-with-assertion", "", string(body)); r.Code != 401 {
		t.Fatal("bad signature accepted")
	}
	for _, body := range []string{`{"assertion":"x","email":"person@example.com"}`, `{} {}`, strings.Repeat("x", 18001)} {
		if r := request(f.handler, "POST", "/api/context/auth-with-assertion", "", body); r.Code != 400 {
			t.Fatalf("bad request accepted: %d", r.Code)
		}
	}
}

func TestIdentityBindingAndDisabledPolicy(t *testing.T) {
	f := newIdentityFixture(t, true)
	tokenFrom(t, f.exchange(t, f.claims))
	original, err := f.app.FindAuthRecordByEmail("users", f.claims.Email)
	if err != nil {
		t.Fatal(err)
	}
	c := f.claims
	c.ID = security.RandomString(32)
	c.Email = "Person@example.com"
	tokenFrom(t, f.exchange(t, c))
	same, _ := f.app.FindAuthRecordByEmail("users", f.claims.Email)
	if same.Id != original.Id {
		t.Fatal("case variant recreated identity")
	}
	c.ID = security.RandomString(32)
	c.Subject = "different-subject"
	if r := f.exchange(t, c); r.Code != 403 {
		t.Fatal("changed subject took existing email")
	}
	c = f.claims
	c.ID = security.RandomString(32)
	c.Email = "changed@example.com"
	if r := f.exchange(t, c); r.Code != 403 {
		t.Fatal("bound subject changed account")
	}
	original.Set("disabled", true)
	if err := f.app.Save(original); err != nil {
		t.Fatal(err)
	}
	c = f.claims
	c.ID = security.RandomString(32)
	if r := f.exchange(t, c); r.Code != 403 {
		t.Fatal("disabled identity admitted")
	}
}

func TestIdentityExistingOnlyAndAuthHookRollback(t *testing.T) {
	f := newIdentityFixture(t, false)
	if r := f.exchange(t, f.claims); r.Code != 403 {
		t.Fatal("unexpected provisioning")
	}
	users, _ := f.app.FindCollectionByNameOrId("users")
	record := core.NewRecord(users)
	record.SetEmail(f.claims.Email)
	record.SetPassword("synthetic-password123")
	record.SetVerified(true)
	if err := f.app.Save(record); err != nil {
		t.Fatal(err)
	}
	reject := true
	f.app.OnRecordAuthRequest("users").BindFunc(func(e *core.RecordAuthRequestEvent) error {
		if e.AuthMethod != "context-identity" || e.Get("contextIdentityEmail") != f.claims.Email {
			t.Error("trusted identity context missing")
		}
		if reject {
			return e.ForbiddenError("synthetic policy denied", nil)
		}
		return e.Next()
	})
	if r := f.exchange(t, f.claims); r.Code != 403 {
		t.Fatal("auth policy bypassed")
	}
	reject = false
	tokenFrom(t, f.exchange(t, f.claims)) // failed policy rolled back replay reservation
	var count struct {
		N int `db:"n"`
	}
	if err := f.app.DB().NewQuery(`SELECT count(*) n FROM _context_identity_bindings`).One(&count); err != nil || count.N != 1 {
		t.Fatal("binding transaction failed")
	}
}

func TestIdentityKeyCacheOutageAndUnknownKidThrottle(t *testing.T) {
	f := newIdentityFixture(t, true)
	if _, err := f.verifier.verify(t.Context(), f.assertion(t, f.claims)); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if _, err := f.verifier.key(t.Context(), "unknown"); err == nil {
			t.Fatal("unknown key accepted")
		}
	}
	if f.count.Load() != 1 {
		t.Fatal("unknown keys caused request amplification")
	}
	f.verifier.client = &http.Client{Transport: roundTripFail{}}
	if _, err := f.verifier.verify(t.Context(), f.assertion(t, f.claims)); err != nil {
		t.Fatal("fresh cache lost during outage")
	}
	f.verifier.fetched = time.Now().Add(-6 * time.Minute)
	f.verifier.attempted = time.Time{}
	if _, err := f.verifier.verify(t.Context(), f.assertion(t, f.claims)); err == nil {
		t.Fatal("expired cached key accepted during outage")
	}
}

type roundTripFail struct{}

func (roundTripFail) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, http.ErrServerClosed
}

func TestIdentityConfig(t *testing.T) {
	valid := IdentityConfig{Issuer: "https://id.example.com", Audience: "https://app.example.com", WorkspaceDomain: "example.com"}
	if err := valid.validate("users"); err != nil {
		t.Fatal(err)
	}
	for _, issuer := range []string{"http://id.example.com", "https://id.example.com/", "https://id.example.com/a", "https://u:p@id.example.com", "https://id.example.com?x=1"} {
		c := valid
		c.Issuer = issuer
		if c.validate("users") == nil {
			t.Fatalf("invalid origin accepted %s", issuer)
		}
	}
	if valid.validate("agents") == nil {
		t.Fatal("non-default collection accepted")
	}
}

func TestIdentityDerivedFileAndRealtimeExpiry(t *testing.T) {
	f := newIdentityFixture(t, true)
	token := tokenFrom(t, f.exchange(t, f.claims))
	user, _ := f.app.FindAuthRecordByEmail("users", f.claims.Email)
	claims, _ := security.ParseJWT(token, user.TokenKey()+user.Collection().AuthToken.Secret)
	claims["exp"] = time.Now().Add(20 * time.Second).Unix()
	short, _ := security.NewJWT(claims, user.TokenKey()+user.Collection().AuthToken.Secret, time.Minute)
	file := tokenFrom(t, request(f.handler, "POST", "/api/files/token", short, `{}`))
	fileClaims, err := security.ParseJWT(file, user.TokenKey()+user.Collection().FileToken.Secret)
	if err != nil {
		t.Fatal(err)
	}
	fileExpiry, _ := fileClaims.GetExpirationTime()
	if fileExpiry.Unix() > int64(claims["exp"].(int64)) {
		t.Fatal("file token outlives identity bearer")
	}
	client := subscriptions.NewDefaultClient()
	f.app.SubscriptionsBroker().Register(client)
	t.Cleanup(func() { f.app.SubscriptionsBroker().Unregister(client.Id()) })
	body, _ := json.Marshal(map[string]any{"clientId": client.Id(), "subscriptions": []string{"users"}})
	if r := request(f.handler, "POST", "/api/realtime", short, string(body)); r.Code != 204 {
		t.Fatalf("subscribe: %d %s", r.Code, r.Body)
	}
	expiry, ok := client.Get(identityExpiryKey).(time.Time)
	if !ok || expiry.Unix() != int64(claims["exp"].(int64)) {
		t.Fatal("realtime expiry missing")
	}
	send := func() bool {
		delivered := false
		event := &core.RealtimeMessageEvent{Client: client, Message: &subscriptions.Message{Name: "users", Data: []byte(`{}`)}}
		if err := f.app.OnRealtimeMessageSend().Trigger(event, func(*core.RealtimeMessageEvent) error { delivered = true; return nil }); err != nil {
			t.Fatal(err)
		}
		return delivered
	}
	if !send() {
		t.Fatal("valid realtime blocked")
	}
	client.Set(identityExpiryKey, time.Now().Add(-time.Second))
	if send() {
		t.Fatal("expired realtime delivered")
	}
	fallback, _ := user.NewAuthToken()
	if r := request(f.handler, "POST", "/api/realtime", fallback, string(body)); r.Code != 204 {
		t.Fatal("fallback could not reauthenticate")
	}
	if !send() {
		t.Fatal("explicit fallback realtime blocked")
	}
}

func TestIdentityCreationHookRollback(t *testing.T) {
	f := newIdentityFixture(t, true)
	deny := true
	f.app.OnRecordAuthRequest("users").BindFunc(func(e *core.RecordAuthRequestEvent) error {
		if deny {
			return e.ForbiddenError("fixture", nil)
		}
		return e.Next()
	})
	if r := f.exchange(t, f.claims); r.Code != 403 {
		t.Fatalf("policy bypass: %d", r.Code)
	}
	if _, err := f.app.FindAuthRecordByEmail("users", f.claims.Email); err == nil {
		t.Fatal("denied JIT left account behind")
	}
	deny = false
	tokenFrom(t, f.exchange(t, f.claims))
}

func TestIdentityKeyRotation(t *testing.T) {
	f := newIdentityFixture(t, true)
	tokenFrom(t, f.exchange(t, f.claims))
	next, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	f.verifier.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		raw, _ := json.Marshal(map[string]any{"keys": []any{map[string]any{"kty": "RSA", "kid": "key-v2", "alg": "RS256", "use": "sig", "n": base64.RawURLEncoding.EncodeToString(next.N.Bytes()), "e": base64.RawURLEncoding.EncodeToString(big.NewInt(int64(next.E)).Bytes())}}})
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
	})}
	f.verifier.attempted = time.Now().Add(-11 * time.Second)
	c := f.claims
	c.ID = security.RandomString(32)
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, c)
	tok.Header["kid"] = "key-v2"
	raw, _ := tok.SignedString(next)
	body, _ := json.Marshal(map[string]string{"assertion": raw})
	tokenFrom(t, request(f.handler, "POST", "/api/context/auth-with-assertion", "", string(body)))
	if _, err := f.verifier.verify(t.Context(), f.assertion(t, f.claims)); err == nil {
		t.Fatal("removed signing key retained after successful refresh")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return fn(r) }

func TestIdentityExpiryBetweenMiddlewareAndDerivedCredential(t *testing.T) {
	f := newIdentityFixture(t, true)
	token := tokenFrom(t, f.exchange(t, f.claims))
	user, _ := f.app.FindAuthRecordByEmail("users", f.claims.Email)
	key := user.TokenKey() + user.Collection().AuthToken.Secret
	claims, _ := security.ParseJWT(token, key)
	expiredAt := time.Now().Add(-time.Second).Truncate(time.Second)
	claims["exp"] = expiredAt.Unix()
	expired, _ := security.NewJWT(claims, key, time.Minute)
	event := &core.RequestEvent{Auth: user}
	event.Request = httptest.NewRequest("POST", "/api/files/token", strings.NewReader(`{}`))
	event.Request.Header.Set("Authorization", "Bearer "+expired)
	if got := identityTokenExpiry(event); !got.Equal(expiredAt) {
		t.Fatal("expiry race would remove derived credential limit")
	}
}

func TestIdentityReplayAndBindingSurviveRestart(t *testing.T) {
	f := newIdentityFixture(t, true)
	tokenFrom(t, f.exchange(t, f.claims))
	dir := f.app.DataDir()
	if err := f.app.ClearBootstrap(); err != nil {
		t.Fatal(err)
	}
	reopened := core.NewBaseApp(core.BaseAppConfig{DataDir: dir, DBConnect: database.Connect})
	if err := reopened.Bootstrap(); err != nil {
		t.Fatal(err)
	}
	reopened.Settings().Logs.MaxDays = 0
	t.Cleanup(func() { reopened.ClearBootstrap() })
	router, err := apis.NewRouter(reopened)
	if err != nil {
		t.Fatal(err)
	}
	verifier := newIdentityVerifier(f.verifier.config)
	verifier.client = f.verifier.client
	if err := registerIdentityVerifier(reopened, &core.ServeEvent{App: reopened, Router: router}, verifier.config, verifier); err != nil {
		t.Fatal(err)
	}
	f.handler, err = router.BuildMux()
	if err != nil {
		t.Fatal(err)
	}
	if r := f.exchange(t, f.claims); r.Code != 401 {
		t.Fatalf("replayed after restart: %d", r.Code)
	}
	c := f.claims
	c.Subject = "replacement-person"
	c.ID = security.RandomString(32)
	if r := f.exchange(t, c); r.Code != 403 {
		t.Fatalf("rebound after restart: %d", r.Code)
	}
}

func TestIdentityExpiredRealtimeAllowsOAuthControlMessages(t *testing.T) {
	f := newIdentityFixture(t, true)
	client := subscriptions.NewDefaultClient()
	client.Set(identityExpiryKey, time.Now().Add(-time.Second))
	for _, name := range []string{"PB_CONNECT", "@oauth2"} {
		delivered := false
		e := &core.RealtimeMessageEvent{Client: client, Message: &subscriptions.Message{Name: name}}
		if err := f.app.OnRealtimeMessageSend().Trigger(e, func(*core.RealtimeMessageEvent) error { delivered = true; return nil }); err != nil || !delivered {
			t.Fatalf("fresh OAuth control message blocked: %s", name)
		}
	}
}
