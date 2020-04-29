package migrate_test

import (
	"log"

	"github.com/rickbassham/database"
	"github.com/rickbassham/database/migrate"
)

func Example() {
	db, err := database.New(nil)
	if err != nil {
		panic(err.Error())
	}

	migration, err := migrate.NewMigrateDB(db)
	if err != nil {
		panic(err.Error())
	}

	err = migration.Init()
	if err != nil {
		panic(err.Error())
	}

	migration.AddMigration(migrate.NewSQLMigration(1, "CREATE TABLE test (id integer, name varchar(50));"))

	version, err := migration.CurrentVersion()
	if err != nil {
		panic(err.Error())
	}

	log.Printf("starting version: %d\n", version)

	for migration.Upgrade() {
		version, err := migration.CurrentVersion()
		if err != nil {
			panic(err.Error())
		}

		log.Printf("now at version: %d\n", version)
	}

	if err = migration.Err(); err != nil {
		panic(err.Error())
	}
}
