package sftp_binding

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/components-contrib/metadata"
	"github.com/dapr/kit/logger"
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
}

/*func NewSftpBinding(metadata Metadata, client SftpClient, logger logger.Logger) *SftpBinding {
	return &SftpBinding{
		metadata: metadata,
		client:   client,
		logger:   logger,
	}
}

*/

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
	switch req.Operation {
	case bindings.ListOperation:
		return sb.List(req)
	case bindings.GetOperation:
		return sb.Get(req)
	}

	return nil, fmt.Errorf("Operation not implemented")
}

// Operations implements bindings.OutputBinding.
func (sb *SftpBinding) Operations() []bindings.OperationKind {
	return []bindings.OperationKind{bindings.ListOperation}
}

func (sb *SftpBinding) parseMetadata(meta bindings.Metadata) (Metadata, error) {
	sftpMeta := Metadata{}
	err := metadata.DecodeMetadata(meta.Properties, &sftpMeta)

	if err != nil {
		return sftpMeta, err
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

func (sb *SftpBinding) List(req *bindings.InvokeRequest) (*bindings.InvokeResponse, error) {
	sb.logger.Info("Connecting")
	sb.client.Connect()

	files, err := sb.client.List(sb.metadata.RootPath)
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

type SftpClient interface {
	Connect() error
	Close() error
	List(path string) ([]string, error)
	Get(path string, filename string) ([]byte, error)
}

//var _ = bindings.OutputBinding(&SftpBinding{})
