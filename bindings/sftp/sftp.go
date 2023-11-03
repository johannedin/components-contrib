package sftp_binding

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	kitmd "github.com/dapr/kit/metadata"
	"github.com/google/uuid"
)

type SftpBinding struct {
	metadata Metadata
	logger   logger.Logger
	client   SftpClient
}

type Metadata struct {
	Host     string `mapstructure:"host"`
	Port     string `mapstructure:"port"`
	Username string `mapstructure:"username"`
	Password string `mapstructure:"password"`
	RootPath string `mapstructure:"rootPath"`
	HostKey  string `mapstructure:"hostKey"`
}

func NewSftpBinding(logger logger.Logger) *SftpBinding {
	return &SftpBinding{
		logger: logger,
	}
}

// Init implements bindings.OutputBinding.
func (sb *SftpBinding) Init(ctx context.Context, metadata bindings.Metadata) error {
	meta, err := sb.parseMetadata(metadata)

	if err != nil {
		return fmt.Errorf("Error parsing metadata %w", err)
	}

	sb.metadata = meta
	sb.logger = logger.NewLogger("SftpBinding")

	c := NewSftpClientAdapter(sb.metadata, sb.logger)
	sb.client = c

	return nil
}

// Invoke implements bindings.OutputBinding.
func (sb *SftpBinding) Invoke(ctx context.Context, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	filename := req.Metadata["fileName"]
	if filename == "" && req.Operation == bindings.CreateOperation {
		u, err := uuid.NewRandom()
		if err != nil {
			return nil, fmt.Errorf("failed to generate UUID: %w", err)
		}
		filename = u.String()
	}

	switch req.Operation {
	case bindings.ListOperation:
		return sb.List(filename, req)
	case bindings.GetOperation:
		return sb.Get(req)
	case bindings.CreateOperation:
		return sb.Create(filename, req)
	}

	return nil, fmt.Errorf("Operation not implemented")
}

// Operations implements bindings.OutputBinding.
func (sb *SftpBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{
		bindings.ListOperation,
		bindings.GetOperation,
		bindings.CreateOperation,
	}
}

func (sb *SftpBinding) parseMetadata(meta bindings.Metadata) (Metadata, error) {
	sftpMeta := Metadata{}
	err := kitmd.DecodeMetadata(meta.Properties, &sftpMeta)

	if err != nil {
		return sftpMeta, err
	}

	if sftpMeta.RootPath == "" {
		return sftpMeta, fmt.Errorf("property rootPath must not be empty")
	}

	return sftpMeta, err

}

func (sb *SftpBinding) Get(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	fileName := req.Metadata["fileName"]

	sb.client.Connect()
	data, err := sb.client.Get(sb.metadata.RootPath, fileName)
	sb.client.Close()

	if err != nil {
		return nil, err
	}

	return &bindings.InvokeResponse{
		Data: data,
	}, nil

}

func (sb *SftpBinding) List(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	sb.logger.Info("Connecting")
	sb.client.Connect()

	path, err := securejoin.SecureJoin(sb.metadata.RootPath, filename)

	if err != nil {
		return nil, fmt.Errorf("error getting absolute path for file %s: %w", filename, err)
	}

	files, err := sb.client.List(path)
	sb.client.Close()

	if err != nil {
		sb.logger.Error(err)
		return nil, fmt.Errorf("Unable to list files")
	}

	b, err := json.Marshal(files)

	return &bindings.InvokeResponse{
		Data: b,
		Metadata: map[string]string{
			"count":     strconv.Itoa(len(files)),
			"operation": "list",
			"type":      "[]string",
			"rootPath":  sb.metadata.RootPath,
		},
	}, nil
}

func (sb *SftpBinding) Create(filename string, req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	sb.client.Connect()
	err := sb.client.Create(sb.metadata.RootPath, filename, req.Data)

	if err != nil {
		return nil, fmt.Errorf("could not create file: %w", err)
	}

	sb.client.Close()

	// TODO: add metadata to response
	res := &bindings.InvokeResponse{}
	return res, nil

}

type SftpClient interface {
	Connect() error
	Close() error
	List(path string) ([]string, error)
	Get(path string, filename string) ([]byte, error)
	Create(path string, filename string, data []byte) error
}
