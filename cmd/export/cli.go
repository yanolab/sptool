package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/yanolab/sptool"
	"gopkg.in/validator.v2"
)

type client struct{}

func NewClient() *client {
	return &client{}
}

type options struct {
	DBPath       string `validate:"nonzero"`
	OutPath      string
	TargetTables string
	Compress     bool
}

func (c *client) Run(args []string) error {
	var opt options

	flags := flag.NewFlagSet("export", flag.ContinueOnError)
	flags.StringVar(&opt.DBPath, "db", "", "spanner database path")
	flags.StringVar(&opt.OutPath, "o", "out", "output dir or file")
	flags.BoolVar(&opt.Compress, "z", false, "zip compress")
	flags.StringVar(&opt.TargetTables, "tables", "", "target tables separated comma")
	flags.Usage = func() {
		fmt.Println("usage: export -db projects/<gcloud_project_id>/instances/<instance_id>/databases/<database_id> [-tables A,B,C] [-o out]")
		os.Exit(0)
	}

	if err := flags.Parse(args); err != nil {
		return err
	}

	if err := validator.Validate(opt); err != nil {
		return err
	}

	if !opt.Compress {
		if _, err := os.Stat(opt.OutPath); os.IsNotExist(err) {
			if err := os.MkdirAll(opt.OutPath, 0755); err != nil {
				return err
			}
		}
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

	var creator sptool.Creator
	if opt.Compress {
		zc, err := sptool.NewZipCreator(opt.OutPath)
		if err != nil {
			return err
		}
		creator = zc
	} else {
		creator = sptool.NewFileCreator(opt.OutPath)
	}
	defer creator.Close()

	targets := tables[:0]
	for _, table := range tables {
		if strings.Contains(opt.TargetTables, table.Name) || opt.TargetTables == "" {
			targets = append(targets, table)
		}
	}

	dump := func(table *sptool.Table) {
		fmt.Printf("exporting %s ... ", table.Name)
		defer fmt.Println("done")
		w, c, err := creator.Create(fmt.Sprintf("%s.json", table.Name))
		if err != nil {
			fmt.Printf("failed to create entry: %s", err)
			return
		}
		defer c.Close()

		if err := cli.DumpTo(ctx, table, w); err != nil {
			fmt.Printf("failed to dump table %s: %s", table.Name, err)
		}
	}

	for _, table := range targets {
		dump(table)
	}

	return nil
}
