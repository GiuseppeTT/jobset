package agent

import (
	"fmt"
	"os"

	"k8s.io/klog/v2"
)

const (
	startedFilePath = "/app/startup/agent-started-once-before"
)

func (a *Agent) startedFileExists() (bool, error) {
	klog.Info("Checking if started file exists")
	_, err := os.Stat(startedFilePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to run stat on started file: %w", err)
}

func (a *Agent) createStartedFile() error {
	klog.Info("Creating started file")
	file, err := os.Create(startedFilePath)
	if err != nil {
		return fmt.Errorf("failed to create started file: %w", err)
	}
	defer file.Close()
	return nil
}
