package sftp_binding

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/dapr/components-contrib/bindings"
	"github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockSftpClient struct {
	mock.Mock
}

func (m *MockSftpClient) Connect() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSftpClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockSftpClient) List(rootPath string) ([]string, error) {
	args := m.Called(rootPath)
	return args.Get(0).([]string), args.Error(1)
}

func (m *MockSftpClient) Get(rootPath string, fileName string) ([]byte, error) {
	args := m.Called(rootPath, fileName)
	return args.Get(0).([]byte), args.Error(1)
}

func (m *MockSftpClient) Create(rootPath string, fileName string, data []byte) error {
	m.Called(rootPath, fileName, data)

	if rootPath == "download" && fileName == "testfile.txt" {
		return nil
	}

	return fmt.Errorf("Could not create file")
}

func CreatePasswordBindingMetadata() bindings.Metadata {
	m := bindings.Metadata{}
	m.Properties = map[string]string{
		"host":     "172.17.0.7",
		"port":     "22",
		"username": "demo",
		"password": "demo",
		"rootPath": "download",
	}

	return m
}

func CreateMockedBinding() (*SftpBinding, *MockSftpClient, error) {
	b := &SftpBinding{}
	mc := new(MockSftpClient)
	m := CreatePasswordBindingMetadata()

	err := b.Init(nil, m)

	if err != nil {
		return b, nil, err
	}

	b.client = mc

	return b, mc, err
}

func TestSftpBinding_Init(t *testing.T) {
	logger := logger.NewLogger("sftptest")

	t.Run("listing files", func(t *testing.T) {
		b, mc, err := CreateMockedBinding()
		f := []string{"RFC4251.pdf", "RFC4252.pdf", "RFC4253.pdf", "RFC4254.pdf"}

		assert.NoError(t, err, "could not create sftpbinding with mocked client")

		mc.On("Connect").Return(nil)
		mc.On("Close").Return(nil)
		mc.On("List", "download").Return(f, nil)

		fres, err := b.List(nil)

		var files []string
		if err := json.Unmarshal(fres.Data, &files); err != nil {
			assert.NoError(t, err, "")
		}

		assert.NoError(t, err, "could not list files")

		assert.True(t, len(files) == 4, "did not return all mocked files")

		logger.Info("Testing")

	})

	t.Run("get file", func(t *testing.T) {
		b, mc, err := CreateMockedBinding()
		// f := []string{"RFC4251.pdf", "RFC4252.pdf", "RFC4253.pdf", "RFC4254.pdf"}
		data := []byte("some data from mock")

		assert.NoError(t, err, "could not create sftpbinding with mocked client")

		mc.On("Connect").Return(nil)
		mc.On("Close").Return(nil)
		// mc.On("List", "download").Return(f, nil)
		mc.On("Get", "download", "testfile.txt").Return(data, nil)
		todo := context.TODO()
		req := &bindings.InvokeRequest{}
		req.Operation = bindings.GetOperation
		req.Metadata = map[string]string{
			"fileName": "testfile.txt",
		}

		res, err := b.Invoke(todo, req)

		assert.NoError(t, err, "could not invoke get")

		assert.True(t, string(res.Data) == "some data from mock", "did not return all mocked files")

		logger.Info("Testing")

	})

	t.Run("create file", func(t *testing.T) {
		b, mc, err := CreateMockedBinding()
		data := []byte("some data to save")

		mc.On("Connect").Return(nil)
		mc.On("Close").Return(nil)
		mc.On("Create", "download", "testfile.txt", data).Return()

		todo := context.TODO()
		req := &bindings.InvokeRequest{}
		req.Operation = bindings.CreateOperation
		req.Metadata = map[string]string{
			"fileName": "testfile.txt",
		}

		req.Data = data

		res, err := b.Invoke(todo, req)

		assert.NoError(t, err, "Could not create file")

		assert.True(t, res != nil)

	})

	t.Run("create binary file", func(t *testing.T) {
		panic("implement test")
	})

	t.Run("parse metadata", func(t *testing.T) {
		b := &SftpBinding{}
		m := CreatePasswordBindingMetadata()
		meta, err := b.parseMetadata(m)

		assert.NoError(t, err)
		assert.True(t, meta.Host == "172.17.0.7", "host parsed incorrectly")
		assert.True(t, meta.Port == "22", "Port parseed incorrectly")
		assert.True(t, meta.Username == "demo", "username parsed incorrectly")
		assert.True(t, meta.Password == "demo", "password parsed incorrectly")
		assert.True(t, meta.RootPath == "download", "rootpath parsed incorrectly")

	})

	t.Run("Integration test, list", func(t *testing.T) {
		b := &SftpBinding{}
		m := CreatePasswordBindingMetadata()
		todo := context.TODO()
		req := &bindings.InvokeRequest{}
		req.Operation = bindings.ListOperation

		err := b.Init(todo, m)

		assert.NoError(t, err, "Could not initialize component")

		res, err := b.Invoke(todo, req)

		assert.NoError(t, err, "could not invoke list")

		meta := res.Metadata["type"]
		logger.Info(meta)
	})

	t.Run("Integration test, get", func(t *testing.T) {
		b := &SftpBinding{}
		m := CreatePasswordBindingMetadata()
		todo := context.TODO()
		req := &bindings.InvokeRequest{}
		req.Operation = bindings.GetOperation
		req.Metadata = map[string]string{
			"fileName": "testfile.txt",
		}
		err := b.Init(todo, m)

		assert.NoError(t, err, "Could not initialize component")

		res, err := b.Invoke(todo, req)

		assert.NoError(t, err, "could not invoke get")

		logger.Info(string(res.Data))
	})

	t.Run("Integration test, create textfile", func(t *testing.T) {
		b := &SftpBinding{}
		m := CreatePasswordBindingMetadata()
		todo := context.TODO()
		req := &bindings.InvokeRequest{}
		req.Operation = bindings.CreateOperation
		req.Metadata = map[string]string{
			"fileName": "testfile2.txt",
		}

		req.Data = []byte("some new data")

		err := b.Init(todo, m)

		assert.NoError(t, err, "Could not initialize component")

		res, err := b.Invoke(todo, req)

		assert.NoError(t, err, "could not invoke get")

		logger.Info(string(res.Data))
	})

	t.Run("Integration test, create binary file", func(t *testing.T) {
		panic("write test")
	})
}
