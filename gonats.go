// Package gonats is the shared event-bus library for FileConvert microservices,
// backed by NATS JetStream. It is the drop-in successor to go-kafka and keeps
// the same API shape (Producer/Consumer, Message, Viper constructors) so
// services can migrate mechanically.
//
// Concept mapping from Kafka:
//
//	topic            -> JetStream stream (name == topic) with a single subject (== topic)
//	message key      -> "Nats-Msg-Key" header (no partitioning; streams are totally ordered)
//	consumer group   -> durable pull consumer named after the group ID
//	auto offset reset-> DeliverPolicy (earliest = all, latest = new)
//
// Streams are created idempotently by both producers and consumers, so service
// startup order does not matter. If a stream already exists its configuration
// is left untouched (infra/gitops may own it).
package gonats

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/spf13/viper"
)

const (
	// KeyHeader carries the Kafka-style message key.
	KeyHeader = "Nats-Msg-Key"

	DefaultURL            = "nats://localhost:4222"
	DefaultConnectTimeout = 5 * time.Second
	DefaultReconnectWait  = 2 * time.Second

	DefaultStreamMaxAge = 7 * 24 * time.Hour
	// 128 MiB per stream. Keep this well below the JetStream max_file_store
	// headroom: every stream ensure reserves MaxBytes up front, and a request
	// larger than the remaining reservation fails server-side with err 10047
	// ("insufficient storage resources") even when the stream already exists.
	DefaultStreamMaxBytes = 1 << 27
	DefaultStreamReplicas = 1
)

// Message represents a consumed event. Field names mirror go-kafka's Message
// so downstream code keeps compiling; Partition is always 0 and Offset is the
// JetStream stream sequence.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time
}

// TLSConfig holds TLS configuration for NATS connections.
type TLSConfig struct {
	Enabled            bool
	CAFile             string // Path to CA certificate file
	CertFile           string // Path to client certificate file (for mTLS)
	KeyFile            string // Path to client key file (for mTLS)
	InsecureSkipVerify bool   // Skip server certificate verification (not recommended for production)
}

// AuthConfig holds optional authentication for NATS connections.
type AuthConfig struct {
	Username  string
	Password  string
	Token     string
	CredsFile string // NATS .creds file (JWT auth)
}

// StreamConfig controls how streams are created when they do not exist yet.
// Existing streams are never modified.
type StreamConfig struct {
	MaxAge   time.Duration // retention window (Kafka retention.ms equivalent)
	MaxBytes int64         // size cap per stream
	Replicas int           // JetStream replicas (1 in dev, 3 in prod)
}

func applyStreamDefaults(c *StreamConfig) {
	if c.MaxAge == 0 {
		c.MaxAge = DefaultStreamMaxAge
	}
	if c.MaxBytes == 0 {
		c.MaxBytes = DefaultStreamMaxBytes
	}
	if c.Replicas == 0 {
		c.Replicas = DefaultStreamReplicas
	}
}

func streamConfigFromViper() StreamConfig {
	return StreamConfig{
		MaxAge:   viper.GetDuration("nats.stream.max_age"),
		MaxBytes: viper.GetInt64("nats.stream.max_bytes"),
		Replicas: viper.GetInt("nats.stream.replicas"),
	}
}

func tlsConfigFromViper() *TLSConfig {
	if !viper.GetBool("nats.tls.enabled") {
		return nil
	}
	return &TLSConfig{
		Enabled:            true,
		CAFile:             viper.GetString("nats.tls.ca_file"),
		CertFile:           viper.GetString("nats.tls.cert_file"),
		KeyFile:            viper.GetString("nats.tls.key_file"),
		InsecureSkipVerify: viper.GetBool("nats.tls.insecure_skip_verify"),
	}
}

func authConfigFromViper() *AuthConfig {
	auth := &AuthConfig{
		Username:  viper.GetString("nats.auth.username"),
		Password:  viper.GetString("nats.auth.password"),
		Token:     viper.GetString("nats.auth.token"),
		CredsFile: viper.GetString("nats.auth.creds_file"),
	}
	if auth.Username == "" && auth.Token == "" && auth.CredsFile == "" {
		return nil
	}
	return auth
}

// connect establishes a NATS connection with sane reconnect behaviour.
func connect(urls []string, clientName string, tlsCfg *TLSConfig, auth *AuthConfig) (*nats.Conn, error) {
	if len(urls) == 0 {
		urls = []string{DefaultURL}
	}

	opts := []nats.Option{
		nats.Name(clientName),
		nats.Timeout(DefaultConnectTimeout),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(DefaultReconnectWait),
	}

	if tlsCfg != nil && tlsCfg.Enabled {
		stdTLS, err := createTLSConfig(tlsCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create TLS config: %w", err)
		}
		opts = append(opts, nats.Secure(stdTLS))
	}

	if auth != nil {
		switch {
		case auth.CredsFile != "":
			opts = append(opts, nats.UserCredentials(auth.CredsFile))
		case auth.Token != "":
			opts = append(opts, nats.Token(auth.Token))
		case auth.Username != "":
			opts = append(opts, nats.UserInfo(auth.Username, auth.Password))
		}
	}

	nc, err := nats.Connect(strings.Join(urls, ","), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	return nc, nil
}

// createTLSConfig creates a *tls.Config from the provided TLSConfig.
func createTLSConfig(config *TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	if config.CAFile != "" {
		caCert, err := os.ReadFile(config.CAFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}
		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	if config.CertFile != "" && config.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// sanitizeName makes a topic/group usable as a JetStream stream/durable name
// (names must not contain '.', '*', '>', path separators or spaces).
func sanitizeName(name string) string {
	r := strings.NewReplacer(".", "-", "*", "-", ">", "-", "/", "-", "\\", "-", " ", "-")
	return r.Replace(name)
}
