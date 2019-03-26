package sptool

import (
	"archive/zip"
	"io"
	"os"
	"path/filepath"
)

type Creator interface {
	Close() error
	Create(string) (io.Writer, io.Closer, error)
}

func NewZipCreator(name string) (*ZipCreator, error) {
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	zw := zip.NewWriter(file)

	return &ZipCreator{f: file, w: zw}, nil
}

type ZipCreator struct {
	f *os.File
	w *zip.Writer
}

func (c *ZipCreator) Close() error {
	c.w.Close()
	return c.f.Close()
}

type nopCloser struct{}

func (c *nopCloser) Close() error {
	return nil
}

func (c *ZipCreator) Create(name string) (io.Writer, io.Closer, error) {
	w, err := c.w.Create(name)
	if err != nil {
		return nil, nil, err
	}

	return w, &nopCloser{}, nil
}

func NewFileCreator(dir string) *FileCreator {
	return &FileCreator{dir: dir}
}

type FileCreator struct {
	nopCloser
	dir string
}

func (c *FileCreator) Create(name string) (io.Writer, io.Closer, error) {
	p := filepath.Join(c.dir, name)
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, nil, err
	}

	return f, f, nil
}
