#!/bin/bash

: ${COUNTRY:=US}
: ${STATE:=CO}
: ${LOCALE:=FortCollins}
: ${COMPANY:=Vertica}
: ${ORG:=SQL}
: ${CN:=$(hostname -f)}

: ${CERT_LOC:=./resources/tests/ssl}￼
[ -d "$CERT_LOC" ] || mkdir -p "$CERT_LOC"
cd "$CERT_LOC" || { echo "Error: Failed to cd into $CERT_LOC"; exit 1; }

printf "\n---generating root CA---\n"
# Create the request Config

cat - > ca_req.conf <<EOF
distinguished_name = ca
prompt = no
[ca]
C = ${COUNTRY}
ST = ${STATE}
L = ${LOCALE}
O = ${COMPANY}
OU = ${ORG}
CN = CA_${USER}@${CN}
EOF

echo "------ Content of ca_req.conf: ------"
cat ca_req.conf
echo "Generating rootCA.crt"

openssl req \
   -x509 \
   -nodes \
   -days 3650 \
   -newkey rsa:4096 \
   -keyout rootCA.key \
   -config ca_req.conf \
   -out rootCA.crt


printf "\n---generating server certs---\n"

cat - > req.conf <<EOF
distinguished_name = sqlvertica
x509_extensions = v3_req
prompt = no
[sqlvertica]
C = ${COUNTRY}
ST = ${STATE}
L = ${LOCALE}
O = ${COMPANY}
OU = ${ORG}
CN = ${CN}
[v3_req]
keyUsage = keyEncipherment, dataEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @dns_names
[dns_names]
DNS.1 = ${CN}
EOF

echo "----- Generating server.cnf"

cat - > server.cnf <<EOF
prompt = no
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost
EOF

echo "Generating server.key"

openssl genrsa -out server.key 4096

echo "Generating server.csr"
echo "--- Content of req.conf: ---"
cat req.conf

openssl req \
   -new
   -key server.key \
   -out server.csr \
   -config req.conf \
   -extensions 'v3_req'

echo "Generating server.crt"
openssl x509 \
  -req \
  -in server.csr \
  -days 365 \
  -sha256 \
  -CA rootCA.crt \
  -CAkey rootCA.key \
  -out server.crt \
  -CAcreateserial \
  -extfile req.conf \
  -extensions v3_req

printf "\n---generating client certs---\n"


cat - > client_req.conf <<EOF
distinguished_name = client
prompt = no
[client]
C = ${COUNTRY}
ST = ${STATE}
L = ${LOCALE}
O = ${COMPANY}
OU = ${ORG}
CN = CA_${USER}@${CN}
EOF


cat > client.cnf <<EOF
basicConstraints = CA:FALSE
nsCertType = client, email
nsComment = "OpenSSL Generated Client Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, emailProtection
EOF

echo "Generating client.key"
openssl genrsa -out client.key 4096
echo "Generating cliend.csr"
openssl req -new -key client.key -out client.csr -config client_req.conf
echo "Generating client.crt"
openssl x509 -req -in client.csr -CA rootCA.crt -CAkey rootCA.key -out client.crt -CAcreateserial -days 3650 -sha256 -extfile client.cnf
