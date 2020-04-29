package migrate

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"plugin"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/rickbassham/database"
)

const (
	createDbVersionTable = `create table if not exists dbVersion (version int not null primary key, createdAt bigint not null)`
	getDbVersion         = `select * from dbVersion order by version desc limit 1`
	addVersion           = `insert into dbVersion (version, createdAt) values (?, ?)`
)

var (
	// ErrUnknownMigrationType is returned when we don't recognize the migration file type.
	ErrUnknownMigrationType = errors.New("unknown migration type")

	// ErrInvalidMigrationFileName is returned when we the migration file doens't match the regex `(\d+)_(.*?)\.(sql|so)`.
	ErrInvalidMigrationFileName = errors.New("invalid migration file name")

	// ErrInvalidPluginMigration is returned when an invalid .so file was loaded as a plugin migration.
	ErrInvalidPluginMigration = errors.New("invalid plugin migration")
)

// Service is responsible for holding state to upgrade the database.
type Service struct {
	db *database.Database

	migrations []Migration
	err        error
}

// NewService creates a new Service.
func NewService(db *database.Database) (*Service, error) {
	err := db.RegisterStatement("CREATE_DB_VERSION_TABLE", createDbVersionTable)
	if err != nil {
		return nil, fmt.Errorf("register statement CREATE_DB_VERSION_TABLE: %w", err)
	}

	return &Service{
		db: db,
	}, nil
}

// Init ensures all necessary tables exist to keep migration history.
func (svc *Service) Init() (err error) {
	_, err = svc.db.Exec(context.Background(), "CREATE_DB_VERSION_TABLE")
	if err != nil {
		return fmt.Errorf("exec CREATE_DB_VERSION_TABLE: %w", err)
	}

	err = svc.db.RegisterStatement("GET_DB_VERSION", getDbVersion)
	if err != nil {
		return fmt.Errorf("register statement GET_DB_VERSION: %w", err)
	}

	err = svc.db.RegisterStatement("ADD_VERSION", addVersion)
	if err != nil {
		return fmt.Errorf("register statement ADD_VERSION: %w", err)
	}

	return
}

// LoadMigrations will load all of the sql and so files in the given path.
func (svc *Service) LoadMigrations(path string) error {
	_, err := svc.getMigrations(path)
	if err != nil {
		return fmt.Errorf("%w", err)
	}

	return nil
}

// Err returns any error that happened as part of Upgrade.
func (svc *Service) Err() error {
	return svc.err
}

// Upgrade will upgrade the database by one version. Check Err() after running.
// Returns true if a migration was run, false otherwise.
func (svc *Service) Upgrade() bool {
	svc.err = nil

	version, err := svc.getDbVersion()
	if err != nil {
		svc.err = fmt.Errorf("get db version: %w", err)
		return false
	}

	for _, migration := range svc.migrations {
		if migration.Version() > version {
			var tx database.Tx
			t := time.Now().Unix()

			tx, err = svc.db.BeginTx(context.Background(), nil)
			if err != nil {
				svc.err = fmt.Errorf("begin tx: %w", err)
				return false
			}

			defer func() {
				if err != nil && tx != nil {
					rollbackErr := tx.Rollback()
					if rollbackErr != nil {
						svc.err = fmt.Errorf("rollback: %w", err)
					}
				}
			}()

			err = migration.Run(svc.db, tx)
			if err != nil {
				svc.err = fmt.Errorf("exec: %w", err)
				return false
			}

			_, err = svc.db.Insert(context.Background(), "ADD_VERSION", migration.Version(), t)
			if err != nil {
				svc.err = fmt.Errorf("add version: %w", err)
				return false
			}

			err = tx.Commit()
			if err != nil {
				svc.err = fmt.Errorf("commit: %w", err)
				return false
			}

			tx = nil

			return true
		}
	}

	svc.err = nil
	return false
}

// CurrentVersion returns the current version of our database.
func (svc *Service) CurrentVersion() (int, error) {
	return svc.getDbVersion()
}

func (svc *Service) getDbVersion() (version int, err error) {
	type dbVersion struct {
		Version   int   `db:"version"`
		CreatedAt int64 `db:"createdAt"`
	}

	var result []dbVersion

	err = svc.db.Select(context.Background(), &result, "GET_DB_VERSION")
	if err != nil {
		err = fmt.Errorf("select: %w", err)
		return
	}

	if len(result) == 0 {
		version = -1
	} else {
		version = result[0].Version
	}

	return
}

// AddMigration allows the user to add any custom migrations to be run. This is useful
// for more complex code-based migrations.
func (svc *Service) AddMigration(m Migration) {
	svc.migrations = append(svc.migrations, m)
}

func (svc *Service) getMigrations(folder string) ([]Migration, error) {
	var files []os.FileInfo
	var err error

	if len(folder) > 0 {
		files, err = ioutil.ReadDir(folder)
		if err != nil {
			return nil, fmt.Errorf("readdir: %w", err)
		}
	}

	for _, file := range files {
		var m Migration

		extn := strings.ToLower(path.Ext(file.Name()))

		switch extn {
		case ".sql":
			m, err = NewSQLMigrationFile(path.Join(folder, file.Name()))
			if err != nil {
				return nil, err
			}

		case ".so":
			m, err = NewPluginMigration(path.Join(folder, file.Name()))
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("extension %s: %w", extn, ErrUnknownMigrationType)
		}

		svc.migrations = append(svc.migrations, m)
	}

	sort.SliceStable(svc.migrations, func(i, j int) bool {
		return svc.migrations[i].Version() < svc.migrations[j].Version()
	})

	return svc.migrations, nil
}

// Migration represents a single migration.
type Migration interface {
	Runner
	Version() int
}

// Runner defines the method used to actually run a migration.
type Runner interface {
	Run(*database.Database, database.Tx) error
}

// SQLMigration is a simple SQL statement based migration.
type SQLMigration struct {
	path      string
	statement string
	version   int
}

// PluginMigration is a migration loaded from a golang .so plugin.
type PluginMigration struct {
	path    string
	version int
	runner  Runner
}

var (
	fileNameRegex = regexp.MustCompile(`(\d+)_(.*?)\.(sql|so)`)
)

// NewSQLMigration creates a new SQL migration.
func NewSQLMigration(version int, statement string) SQLMigration {
	return SQLMigration{
		version:   version,
		statement: statement,
	}
}

// NewSQLMigrationFile creates a new SQL migration from the given file. The file name
// must match the regex `(\d+)_(.*?)\.(sql|so)`.
func NewSQLMigrationFile(path string) (m SQLMigration, err error) {
	matches := fileNameRegex.FindStringSubmatch(path)
	if len(matches) == 0 {
		err = ErrInvalidMigrationFileName
		return
	}

	version, _ := strconv.Atoi(matches[1])

	f, err := os.Open(path)
	if err != nil {
		return
	}

	b, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}

	m.path = path
	m.statement = string(b)
	m.version = version
	return
}

// Run will run the sql statement against the database.
func (m SQLMigration) Run(db *database.Database, tx database.Tx) error {
	key := fmt.Sprintf("DB_MIGRATION_%d", m.Version())
	err := db.RegisterStatement(key, m.statement)
	if err != nil {
		return nil
	}

	_, err = db.ExecTx(context.Background(), tx, key)

	return err
}

// Version is the version number of the migration.
func (m SQLMigration) Version() int {
	return m.version
}

// NewPluginMigration creates a Migration from a golang .so plugin file.
func NewPluginMigration(path string) (m PluginMigration, err error) {
	matches := fileNameRegex.FindStringSubmatch(path)
	if len(matches) == 0 {
		err = ErrInvalidMigrationFileName
		return
	}

	version, _ := strconv.Atoi(matches[1])

	plug, err := plugin.Open(path)
	if err != nil {
		return
	}

	symRunner, err := plug.Lookup("Runner")
	if err != nil {
		return
	}

	r, ok := symRunner.(Runner)
	if !ok {
		err = ErrInvalidPluginMigration
		return
	}

	m.version = version
	m.path = path
	m.runner = r

	return
}

// Run will run the sql statement against the database.
func (m PluginMigration) Run(db *database.Database, tx database.Tx) error {
	return m.runner.Run(db, tx)
}

// Version is the version number of the migration.
func (m PluginMigration) Version() int {
	return m.version
}
