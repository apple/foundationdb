package certloader

import (
	"crypto/tls"
	"fmt"
	"os"
	"time"

	"github.com/go-logr/logr"
)

// CertLoader is used to reload certificates if needed.
type CertLoader struct {
	// CertFile specifies the path to the x509 certificate.
	CertFile string
	// CertFile specifies the path to the x509 private key.
	KeyFile string
	// cachedCert will cache the loaded certificate key pair.
	cachedCert *tls.Certificate
	// cachedCertModTime is used to track the last modification time of the key file.
	cachedCertModTime time.Time
	// logger is the logger for logging.
	logger logr.Logger
}

// NewCertLoader creates a new CertLoader.
func NewCertLoader(logger logr.Logger, certFile string, keyFile string) *CertLoader {
	return &CertLoader{
		CertFile: certFile,
		KeyFile:  keyFile,
		logger:   logger.WithName("CertLoader"),
	}
}

// GetCertificate returns the certificate for the TLS requests and will return the cached certificate if the file has not been changed.
func (certLoader *CertLoader) GetCertificate(_ *tls.ClientHelloInfo) (*tls.Certificate, error) {
	stat, err := os.Stat(certLoader.KeyFile)
	if err != nil {
		err = fmt.Errorf("failed checking key file: %s modification time: %w", certLoader.KeyFile, err)
		certLoader.logger.Error(err, "could not load information from key file", "certFile", certLoader.CertFile, "keyFile", certLoader.KeyFile)
		return nil, err
	}

	if certLoader.cachedCert == nil || stat.ModTime().After(certLoader.cachedCertModTime) {
		certLoader.logger.Info("loading new certificates", "certFile", certLoader.CertFile, "keyFile", certLoader.KeyFile, "cachedModificationTime", certLoader.cachedCertModTime.String(), "currentModificationTime", stat.ModTime().String())
		pair, err := tls.LoadX509KeyPair(certLoader.CertFile, certLoader.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed loading tls key pair: %w", err)
		}

		certLoader.cachedCert = &pair
		certLoader.cachedCertModTime = stat.ModTime()
	}

	return certLoader.cachedCert, nil
}
