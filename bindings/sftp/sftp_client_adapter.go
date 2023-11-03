package sftp_binding

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strconv"

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
	// s.logger.Info("PublicKey" + string(key.Marshal()))
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

func (s *SftpClientAdapter) Create(rootPath string, fileName string, data []byte) error {
	d, err := strconv.Unquote(string(data))
	if err == nil {
		data = []byte(d)
	}

	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		data = decoded
	}

	absPath, relPath, err := getSecureAbsRelPath(rootPath, fileName)
	if err != nil {
		return fmt.Errorf("error getting absolute path for file %s: %w", fileName, err)
	}

	dir := filepath.Dir(absPath)
	err = s.client.MkdirAll(dir)
	if err != nil {
		return fmt.Errorf("error creating directory %s: %w", dir, err)
	}

	file, err := s.client.Create(absPath)
	if err != nil {
		return fmt.Errorf("error creating file %s: %w", absPath, err)
	}

	defer file.Close()

	numBytes, err := file.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to file %s: %w", absPath, err)
	}

	s.logger.Debugf("wrote file: %s. numBytes: %d", absPath, numBytes)
	s.logger.Debugf("relpath: %s", relPath)

	return nil
}

func getSecureAbsRelPath(rootPath string, filename string) (absPath string, relPath string, err error) {
	absPath, err = securejoin.SecureJoin(rootPath, filename)
	if err != nil {
		return
	}
	relPath, err = filepath.Rel(rootPath, absPath)
	if err != nil {
		return
	}

	return
}
