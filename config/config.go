package config

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
	"runtime"
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
var ProjectRoot = getRootProjectDir()

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath(ProjectRoot)
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
	InitDirs()
}

func getRootProjectDir() string {
	_, b, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(b))
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

func InitDirs() {
	err := os.MkdirAll(TopicsDir(), 0700)
	if err != nil && !os.IsNotExist(err) {
		log.Fatalf("Couldn't create topics directory: %s", err.Error())
	}
}
