package main

import (
	"errors"
	"log"
	"os"

	"github.com/pocketbase/dbx"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/plugins/jsvm"
	"github.com/pocketbase/pocketbase/plugins/migratecmd"
	"github.com/pocketcontext/pocketcontext/internal/database"
	"github.com/pocketcontext/pocketcontext/internal/server"
	"github.com/spf13/pflag"
)

func main() {
	var controller *database.Controller
	app := pocketbase.NewWithConfig(pocketbase.Config{DBConnect: func(path string) (*dbx.DB, error) {
		if controller == nil {
			return nil, errors.New("maintenance controller is not initialized")
		}
		return controller.Connect(path)
	}})
	var configPath, hooksDir, migrationsDir string
	app.RootCmd.PersistentFlags().StringVar(&configPath, "contextConfig", "pocketcontext.json", "SQL read configuration file")
	app.RootCmd.PersistentFlags().StringVar(&hooksDir, "hooksDir", "pb_hooks", "application JavaScript hooks directory")
	app.RootCmd.PersistentFlags().StringVar(&migrationsDir, "migrationsDir", "pb_migrations", "application JavaScript migrations directory")
	if err := app.RootCmd.ParseFlags(os.Args[1:]); err != nil && !errors.Is(err, pflag.ErrHelp) {
		log.Fatal(err)
	}
	var err error
	controller, err = database.NewController(app.DataDir())
	if err != nil {
		log.Fatal("maintenance state could not be loaded")
	}
	server.RegisterMaintenance(app, controller)
	jsvm.MustRegister(app, jsvm.Config{HooksDir: hooksDir, MigrationsDir: migrationsDir, HooksWatch: false})
	migratecmd.MustRegister(app, app.RootCmd, migratecmd.Config{Dir: migrationsDir, TemplateLang: migratecmd.TemplateLangJS, Automigrate: false})
	server.Register(app, configPath)
	if err := app.Start(); err != nil {
		log.Fatal(err)
	}
}
