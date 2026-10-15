# Synthetic TLS fixture

`release_tls.crt` and `release_tls.key` are a public test-only certificate and key
for `localhost`, generated for the isolated release smoke server. They contain no
production credentials. Never use them outside tests. The certificate expires in
October 2036.

The Linux smoke process trusts this certificate only through its own
`SSL_CERT_FILE` environment; it does not modify the machine's trust store. The
test uses an ephemeral loopback listener and never contacts a public service.
