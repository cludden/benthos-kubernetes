package writer

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/Jeffail/benthos/v3/internal/bloblang"
	"github.com/Jeffail/benthos/v3/internal/bloblang/field"
	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// FilesConfig contains configuration fields for the files output type.
type FilesConfig struct {
	Path string `json:"path" yaml:"path"`
}

// NewFilesConfig creates a new Config with default values.
func NewFilesConfig() FilesConfig {
	return FilesConfig{
		Path: `${!count("files")}-${!timestamp_unix_nano()}.txt`,
	}
}

//------------------------------------------------------------------------------

// Files is a benthos writer.Type implementation that writes message parts each
// to their own file.
type Files struct {
	conf FilesConfig

	path field.Expression

	log   log.Modular
	stats metrics.Type
}

// NewFiles creates a new file based writer.Type.
func NewFiles(
	conf FilesConfig,
	log log.Modular,
	stats metrics.Type,
) (*Files, error) {
	path, err := bloblang.NewField(conf.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to parse path expression: %v", err)
	}
	return &Files{
		conf:  conf,
		path:  path,
		log:   log,
		stats: stats,
	}, nil
}

// ConnectWithContext is a noop.
func (f *Files) ConnectWithContext(ctx context.Context) error {
	return f.Connect()
}

// Connect is a noop.
func (f *Files) Connect() error {
	f.log.Infoln("Writing message parts as files.")
	return nil
}

// WriteWithContext attempts to write message contents to a directory as files.
func (f *Files) WriteWithContext(ctx context.Context, msg types.Message) error {
	return f.Write(msg)
}

// Write attempts to write message contents to a directory as files.
func (f *Files) Write(msg types.Message) error {
	return IterateBatchedSend(msg, func(i int, p types.Part) error {
		path := f.path.String(i, msg)

		err := os.MkdirAll(filepath.Dir(path), os.FileMode(0777))
		if err != nil {
			return err
		}

		return ioutil.WriteFile(path, p.Get(), os.FileMode(0666))
	})
}

// CloseAsync begins cleaning up resources used by this reader asynchronously.
func (f *Files) CloseAsync() {
}

// WaitForClose will block until either the reader is closed or a specified
// timeout occurs.
func (f *Files) WaitForClose(time.Duration) error {
	return nil
}

//------------------------------------------------------------------------------
