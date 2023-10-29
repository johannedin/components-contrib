package sftp_binding

import (
	"bytes"
	"fmt"

	securejoin "github.com/cyphar/filepath-securejoin"
	"github.com/dapr/kit/logger"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"

	"net"
)

type SftpClientAdapter struct {
	metadata Metadata
	logger   logger.Logger
	client   *sftp.Client
}

func NewSftpClientAdapter(metadata Metadata, logger logger.Logger) SftpClient {
	return &SftpClientAdapter{metadata: metadata, logger: logger}
}

func (s *SftpClientAdapter) Connect() error {

	m := s.metadata
	c := &ssh.ClientConfig{
		User:            m.Username,
		Auth:            []ssh.AuthMethod{ssh.Password(m.Password)},
		HostKeyCallback: s.HostKeyCallback,
	}

	conn, err := ssh.Dial("tcp", m.Host+":"+m.Port, c)

	if err != nil {
		s.logger.Errorf("Unable to connect")
		return err
	}

	client, err := sftp.NewClient(conn)
	s.client = client

	return nil
}

func (s *SftpClientAdapter) HostKeyCallback(hostname string, remote net.Addr, key ssh.PublicKey) error {
	s.logger.Warn("No hostkey validation implemented")
	s.logger.Info("Host: " + hostname)
	s.logger.Info("Connecting to: " + remote.Network())

	return nil
}

func (s *SftpClientAdapter) Close() error {
	err := s.client.Close()

	if err != nil {
		return err
	}

	return nil
}

func (s *SftpClientAdapter) List(rootPath string) ([]string, error) {
	files, err := s.client.ReadDir(s.metadata.RootPath)
	if err != nil {
		return nil, err
	}

	var fileNames []string
	for _, file := range files {
		if !file.IsDir() {
			fileNames = append(fileNames, file.Name())
		}
	}
	return fileNames, nil
}

func (s *SftpClientAdapter) Get(rootPath string, fileName string) ([]byte, error) {
	absPath, err := securejoin.SecureJoin(rootPath, fileName)

	if err != nil {
		return nil, fmt.Errorf("Could not form a secure path from rootPath and fileName: %w", err)
	}

	file, err := s.client.Open(absPath)

	if err != nil {
		return nil, fmt.Errorf("Could not open file: %w", err)
	}

	defer file.Close()

	var buf bytes.Buffer
	if _, err = file.WriteTo(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil

}
