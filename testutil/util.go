package testutil

import (
	"fmt"
	"github.com/gl-ot/light-mq/config"
	"os"
	"path/filepath"
	"time"
)

type StdWriter struct {
}

func (writer StdWriter) Write(bytes []byte) (int, error) {
	return fmt.Print(time.Now().Format("15:04:05.999") + " " + string(bytes))
}

// Creates log directory for each package because go runs tests package-parallel style
func LogSetup(packName string) error {
	err := os.RemoveAll(filepath.Join(config.ProjectRoot, "build", packName))
	if err != nil {
		return fmt.Errorf("couldn't delete build directory: %s", err)
	}
	logDir := filepath.Join(config.ProjectRoot, "build", packName, "log-dir")
	err = os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("couldn't create build directory %s: %s", logDir, err)
	}
	config.Props.Log.Dir = logDir
	config.InitDirs()
	return nil
}
