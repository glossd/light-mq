package testutil

import (
	"github.com/gl-ot/light-mq/config"
	"os"
	"path/filepath"
	"testing"
)

// Creates log directory for each package because go runs tests package-parallel style
func LogSetup(t *testing.T, packName string) {
	err := os.RemoveAll(filepath.Join(config.ProjectRoot, "build", packName))
	if err != nil {
		t.Fatal("Couldn't delete build directory: ", err)
	}
	logDir := filepath.Join(config.ProjectRoot, "build", packName, "log-dir")
	err = os.MkdirAll(logDir, os.ModePerm)
	if err != nil {
		t.Fatalf("Couldn't create build directory %s: %s", logDir, err)
	}
	config.Props.Log.Dir = logDir
	config.InitDirs()
}
