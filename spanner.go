package sptool

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/syucream/spar/src/parser"
	"google.golang.org/api/iterator"
	databasepb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

type Client struct {
	db     string
	admin  *database.DatabaseAdminClient
	client *spanner.Client
}

func (c Client) Close() error {
	c.client.Close()
	return c.admin.Close()
}

func NewClient(ctx context.Context, db string) (*Client, error) {
	dataClient, err := spanner.NewClient(ctx, db)
	if err != nil {
		return nil, err
	}
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		dataClient.Close()
		return nil, err
	}

	return &Client{db: db, admin: adminClient, client: dataClient}, nil
}

func (c *Client) GetDatabaseDDL(ctx context.Context) ([]string, error) {
	req := &databasepb.GetDatabaseDdlRequest{Database: c.db}
	res, err := c.admin.GetDatabaseDdl(ctx, req)
	if err != nil {
		return nil, err
	}

	return res.GetStatements(), nil
}

func (c *Client) GetTables(ctx context.Context) ([]*Table, error) {
	ddl, err := c.GetDatabaseDDL(ctx)
	if err != nil {
		return nil, err
	}

	tables := make([]*Table, 0)
	for _, v := range ddl {
		b := bytes.NewBufferString(v + ";")
		ddstmts, err := parser.Parse(b)
		if err != nil {
			return nil, err
		}
		if len(ddstmts.CreateTables) >= 1 {
			table := newTableStruct(ddstmts.CreateTables[0])
			tables = append(tables, table)
		}
	}

	return tables, nil
}

func (c *Client) Save(ctx context.Context, ms []*spanner.Mutation) error {
	_, err := c.client.Apply(ctx, ms)
	return err
}

func (c *Client) DumpTo(ctx context.Context, table *Table, w io.Writer) error {
	txn, err := c.client.BatchReadOnlyTransaction(ctx, spanner.StrongRead())
	if err != nil {
		return err
	}
	defer txn.Close()

	sql := fmt.Sprintf("SELECT * FROM %s;", table.Name)
	stmt := spanner.Statement{SQL: sql}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		} else if err != nil {
			return err
		}

		v := table.New()
		if err := row.ToStruct(v); err != nil {
			fmt.Fprintf(os.Stderr, "failed to read row: %s", err)
			continue
		}

		b, err := json.Marshal(v)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to marshal: %s", err)
			continue
		}

		if _, err := fmt.Fprintln(w, string(b)); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write: %s", err)
		}
	}

	return nil
}
