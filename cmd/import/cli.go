package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"cloud.google.com/go/spanner"
	"github.com/yanolab/sptool"
	"gopkg.in/validator.v2"
)

type client struct{}

func NewClient() *client {
	return &client{}
}

type options struct {
	DBPath    string `validate:"nonzero"`
	OverWrite bool
}

func findTable(tables []*sptool.Table, name string) *sptool.Table {
	for _, v := range tables {
		if v.Name == name {
			return v
		}
	}
	return nil
}

func importTo(ctx context.Context, table *sptool.Table, f io.Reader, cli *sptool.Client) error {
	defer cli.Flush(ctx)
	scanner := bufio.NewScanner(f)
	v := table.New()
	for scanner.Scan() {
		if err := json.Unmarshal(scanner.Bytes(), v); err != nil {
			return err
		}
		vals := table.Vals(v)
		m := spanner.InsertOrUpdate(table.Name, table.Columns(), vals)
		if err := cli.Save(ctx, []*spanner.Mutation{m}); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (c *client) Run(args []string) error {
	var opt options

	flags := flag.NewFlagSet("import", flag.ContinueOnError)
	flags.StringVar(&opt.DBPath, "db", "", "spanner database path")
	//flags.BoolVar(&opt.OverWrite, "u", false, "overwrite")
	flags.Usage = func() {
		fmt.Println("usage: import -db projects/gcloud_project_id>/instances/<instance_id>/databases/<database_id> [FILE...]")
		os.Exit(0)
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if err := validator.Validate(opt); err != nil {
		return err
	}

	ctx := context.Background()
	cli, err := sptool.NewClient(ctx, opt.DBPath)
	if err != nil {
		panic(err)
	}
	defer cli.Close()

	tables, err := cli.GetTables(ctx)
	if err != nil {
		panic(err)
	}

	for _, fname := range flags.Args() {
		f, err := os.Open(fname)
		if err != nil {
			fmt.Fprintf(os.Stderr, "can not open: %s, err:%v", fname, err)
			continue
		}
		func() {
			defer f.Close()

			name := strings.TrimSuffix(filepath.Base(f.Name()), filepath.Ext(f.Name()))
			table := findTable(tables, name)
			if table == nil {
				fmt.Fprintf(os.Stderr, "no matched table: %s\n", name)
				return
			}

			fmt.Printf("importing %s ... ", table.Name)
			defer fmt.Println("done")

			if err := importTo(ctx, table, f, cli); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}()
	}

	return nil
}
