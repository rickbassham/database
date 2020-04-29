package database_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/rickbassham/database"
)

type mockDB struct {
	mock.Mock
}

func (m *mockDB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	a := m.Called(ctx, query, args)

	p := a.Get(0)
	if p == nil {
		return nil, a.Error(1)
	}

	return p.(sql.Result), a.Error(1)
}

func (m *mockDB) SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	a := m.Called(ctx, dest, query, args)
	return a.Error(0)
}

func (m *mockDB) GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error {
	a := m.Called(ctx, dest, query, args)
	return a.Error(0)
}

func (m *mockDB) Ping() error {
	return m.Called().Error(0)
}

func (m *mockDB) Preparex(query string) (*sqlx.Stmt, error) {
	a := m.Called(query)

	p := a.Get(0)
	if p == nil {
		return nil, a.Error(1)
	}

	return p.(*sqlx.Stmt), a.Error(1)
}

func (m *mockDB) BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error) {
	a := m.Called(ctx, opts)
	return a.Get(0).(*sqlx.Tx), a.Error(1)
}

func TestExec(t *testing.T) {
	d := &mockDB{}

	ctx := context.Background()

	d.On("Ping").Return(nil)
	d.On("Preparex", "select 1").Return(nil, nil)
	d.On("ExecContext", ctx, "select 1 /* database/db_test.go:75 */", []interface{}(nil)).Return(nil, nil)

	db, err := database.New(d)
	require.NoError(t, err)

	err = db.RegisterStatement("test", "select 1")
	require.NoError(t, err)

	_, err = db.Exec(context.Background(), "test")
	require.NoError(t, err)
}
