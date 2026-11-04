package postgres

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Nguyenngoctruong274/go-webhook-gateway/internal/testdb"
)

func TestLoadMigrations(t *testing.T) {
	tests := []struct {
		name    string
		files   fstest.MapFS
		want    []int
		wantErr string
	}{
		{
			name: "sorted by version",
			files: fstest.MapFS{
				"migrations/0010_later.sql": {Data: []byte("SELECT 10")},
				"migrations/0002_next.sql":  {Data: []byte("SELECT 2")},
				"migrations/0001_init.sql":  {Data: []byte("SELECT 1")},
			},
			want: []int{1, 2, 10},
		},
		{name: "empty", files: fstest.MapFS{}, wantErr: "no migrations"},
		{
			name:    "missing description",
			files:   fstest.MapFS{"migrations/0001.sql": {Data: []byte("x")}},
			wantErr: "name must look like",
		},
		{
			name:    "non-numeric version",
			files:   fstest.MapFS{"migrations/abc_init.sql": {Data: []byte("x")}},
			wantErr: "invalid version",
		},
		{
			name: "duplicate version",
			files: fstest.MapFS{
				"migrations/0001_a.sql": {Data: []byte("x")},
				"migrations/1_b.sql":    {Data: []byte("y")},
			},
			wantErr: "share version 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadMigrations(tt.files)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d migrations, want %d", len(got), len(tt.want))
			}
			for i, m := range got {
				if m.version != tt.want[i] {
					t.Fatalf("migration %d has version %d, want %d", i, m.version, tt.want[i])
				}
			}
		})
	}
}

func TestEmbeddedMigrationsLoad(t *testing.T) {
	ms, err := loadMigrations(migrationFS)
	if err != nil {
		t.Fatal(err)
	}
	if ms[0].version != 1 || !strings.Contains(ms[0].sql, "CREATE TABLE endpoints") {
		t.Fatalf("unexpected first migration: %s", ms[0].name)
	}
	if len(ms) < 2 || ms[1].version != 2 || !strings.Contains(ms[1].sql, "CREATE TABLE deliveries") {
		t.Fatalf("unexpected second migration: %+v", ms)
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := testdb.New(t, Migrate)
	ctx := context.Background()
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("second run: %v", err)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT count(*) FROM schema_migrations`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if want := len(mustLoad(t)); n != want {
		t.Fatalf("schema_migrations has %d rows, want %d", n, want)
	}
}

func TestMigrateRollsBackAFailedMigration(t *testing.T) {
	db := testdb.NewEmpty(t)
	ctx := context.Background()
	fsys := fstest.MapFS{
		"migrations/0001_ok.sql":     {Data: []byte(`CREATE TABLE a (id INT)`)},
		"migrations/0002_broken.sql": {Data: []byte(`CREATE TABLE b (id INT); SELECT nope FROM nowhere`)},
	}
	err := migrate(ctx, db, fsys)
	if err == nil || !strings.Contains(err.Error(), "0002_broken.sql") {
		t.Fatalf("error = %v, want one naming the broken migration", err)
	}
	var a, b bool
	if err := db.QueryRowContext(ctx,
		`SELECT to_regclass('a') IS NOT NULL, to_regclass('b') IS NOT NULL`).Scan(&a, &b); err != nil {
		t.Fatal(err)
	}
	if !a || b {
		t.Fatalf("table a exists %v (want true), table b exists %v (want false)", a, b)
	}
}

func mustLoad(t *testing.T) []migration {
	t.Helper()
	ms, err := loadMigrations(migrationFS)
	if err != nil {
		t.Fatal(err)
	}
	return ms
}
