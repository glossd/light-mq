package config

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"strings"
)

type AppProps struct {
	Port   int
	Log    Log
	Stdout Stdout
}

type Log struct {
	Dir string
}

type Stdout struct {
	Level string
}

var Props *AppProps

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatalf("Error reading config file, %s", err)
	}

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.SetEnvPrefix("lmq")
	viper.AutomaticEnv()

	err := viper.Unmarshal(&Props)
	if err != nil {
		log.Fatalf("unable to decode into struct, %v", err)
	}

	initLogs()
	log.Debugf("Configuration properties: %+v", Props)
}

func initLogs() {
	level, err := log.ParseLevel(Props.Stdout.Level)
	if err != nil {
		log.Errorf("Couldn't parse log level, falling back to the INFO level")
		level = log.InfoLevel
	}
	log.SetLevel(level)
	log.SetOutput(os.Stdout)
	log.SetReportCaller(true)
}
