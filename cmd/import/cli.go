package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
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

	fis, err := ioutil.ReadDir("out")
	for _, fi := range fis {
		fi := fi
		func() {
			f, err := os.Open(filepath.Join("out", fi.Name()))
			if err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			defer f.Close()

			var table *sptool.Table
			name := strings.TrimSuffix(filepath.Base(f.Name()), filepath.Ext(f.Name()))
			for _, v := range tables {
				if v.Name == name {
					table = v
					break
				}
			}
			if table == nil {
				fmt.Fprintf(os.Stderr, "no matched table: %s\n", name)
				return
			}

			scanner := bufio.NewScanner(f)
			v := table.New()
			ms := make([]*spanner.Mutation, 0)
			for scanner.Scan() {
				err := json.Unmarshal(scanner.Bytes(), v)
				if err != nil {
					fmt.Fprintln(os.Stderr, err)
					continue
				}
				vals := table.Vals(v)
				m := spanner.InsertOrUpdate(table.Name, table.Columns(), vals)
				ms = append(ms, m)
			}
			if err := scanner.Err(); err != nil {
				fmt.Fprintln(os.Stderr, err)
				return
			}
			if err := cli.Save(ctx, ms); err != nil {
				fmt.Fprintln(os.Stderr, err)
			}
		}()
	}

	return nil
}
