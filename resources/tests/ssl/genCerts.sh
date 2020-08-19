#!/bin/bash

printf "\n---generating root stuff---\n"

openssl req \
    -x509 \
    -nodes \
    -days 3650 \
    -newkey rsa:4096 \
    -keyout rootCA.key \
    -out rootCA.crt

printf "\n---generating server stuff---\n"

cat > server.cnf <<EOF
basicConstraints = CA:FALSE
nsCertType = server
nsComment = "OpenSSL Generated Server Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer:always
keyUsage = critical, digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = DNS:localhost
EOF

openssl genrsa -out server.key 4096
openssl req -new -key server.key -out server.csr
openssl x509 -req -in server.csr -CA rootCA.crt -CAkey rootCA.key -out server.crt -CAcreateserial -days 3650 -sha256 -extfile server.cnf

printf "\n---generating client stuff---\n"

cat > client.cnf <<EOF
basicConstraints = CA:FALSE
nsCertType = client, email
nsComment = "OpenSSL Generated Client Certificate"
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid,issuer
keyUsage = critical, nonRepudiation, digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth, emailProtection
EOF

openssl genrsa -out client.key 4096
openssl req -new -key client.key -out client.csr
openssl x509 -req -in client.csr -CA rootCA.crt -CAkey rootCA.key -out client.crt -CAcreateserial -days 3650 -sha256 -extfile client.cnf
    
