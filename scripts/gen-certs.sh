#!/usr/bin/env bash
# Generates self-signed CA + server TLS certs for chord-grpc dev.
# Requires OpenSSL 3.x. Run from the project root: bash scripts/gen-certs.sh
set -euo pipefail
# Prevent Git Bash on Windows from rewriting /CN=... as a path
export MSYS_NO_PATHCONV=1

CERTS_DIR="${CERTS_DIR:-certs}"
mkdir -p "$CERTS_DIR"

echo "Generating CA key and certificate..."
openssl genrsa -out "$CERTS_DIR/ca.key" 2048 2>/dev/null
openssl req -new -x509 -days 3650 -key "$CERTS_DIR/ca.key" \
  -subj "/CN=chord-grpc-ca" \
  -out "$CERTS_DIR/ca.crt" 2>/dev/null

echo "Generating server key and CSR..."
openssl genrsa -out "$CERTS_DIR/server.key" 2048 2>/dev/null
openssl req -new -key "$CERTS_DIR/server.key" \
  -subj "/CN=chord-node" \
  -out "$CERTS_DIR/server.csr" 2>/dev/null

# SANs cover Docker Compose service names plus localhost
cat > "$CERTS_DIR/server.ext" <<'EOF'
[req]
req_extensions = v3_req
[v3_req]
subjectAltName = DNS:chord-node,DNS:node_primary,DNS:localhost,IP:127.0.0.1
EOF

echo "Signing server certificate..."
openssl x509 -req -days 3650 \
  -in "$CERTS_DIR/server.csr" \
  -CA "$CERTS_DIR/ca.crt" -CAkey "$CERTS_DIR/ca.key" -CAcreateserial \
  -extfile "$CERTS_DIR/server.ext" -extensions v3_req \
  -out "$CERTS_DIR/server.crt" 2>/dev/null

rm "$CERTS_DIR/server.csr" "$CERTS_DIR/server.ext"
# ca.key not needed after signing; remove to avoid committing it
rm "$CERTS_DIR/ca.key"

echo "Done. Files written to $CERTS_DIR/:"
echo "  ca.crt      CA certificate (clients verify servers against this)"
echo "  server.crt  Server certificate (SANs: chord-node, node_primary, localhost)"
echo "  server.key  Server private key"
