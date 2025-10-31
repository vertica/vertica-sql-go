package vertigo

import (
    "crypto/rand"
    "crypto/rsa"
    "crypto/tls"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    "math/big"
    "os"
    "testing"
    "time"
)

// generateTempCert creates a self-signed certificate and returns its bytes
func generateTempCert(t *testing.T) ([]byte, []byte) {
    t.Helper()

    priv, err := rsa.GenerateKey(rand.Reader, 2048)
    if err != nil {
        t.Fatalf("failed to generate key: %v", err)
    }

    host, err := os.Hostname()
    if err != nil {
        host = "localhost"
    }

    template := x509.Certificate{
        SerialNumber: big.NewInt(1),
        Subject: pkix.Name{
            Organization: []string{"TestOrg"},
        },
        NotBefore:             time.Now(),
        NotAfter:              time.Now().Add(24 * time.Hour),
        KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
        ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
        BasicConstraintsValid: true,
        DNSNames:              []string{host},
    }

    certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &priv.PublicKey, priv)
    if err != nil {
        t.Fatalf("failed to create certificate: %v", err)
    }

    certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
    keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(priv)})

    return certPEM, keyPEM
}

func TestTLSAutoNegotiate(t *testing.T) {
    certPEM, _ := generateTempCert(t)

    // Create a pool with our self-signed cert
    caCertPool := x509.NewCertPool()
    if !caCertPool.AppendCertsFromPEM(certPEM) {
        t.Fatalf("failed to append CA cert")
    }

    host, err := os.Hostname()
    if err != nil {
        host = "localhost"
    }

    // Rely on Go's default TLS handshake behavior: protocol version and cipher suite
    // selection are negotiated automatically between client and server at runtime.
    // We only provide RootCAs and ServerName (SNI); we intentionally do not set
    // MinVersion, MaxVersion, or CipherSuites because we want the runtime defaults
    // to choose the best mutually supported options for this test.

    tlsConfig := &tls.Config{
        RootCAs:    caCertPool,
        ServerName: host,
    }

    // Register TLS config in the driver
    RegisterTLSConfig("temp_test", tlsConfig)

    // Check that the TLS config exists in the internal map
    cfg, ok := tlsConfigs.get("temp_test")
    if !ok {
        t.Fatalf("TLS config not registered correctly")
    }

    if cfg.ServerName != host {
        t.Errorf("Expected ServerName %s, got %s", host, cfg.ServerName)
    }
}
