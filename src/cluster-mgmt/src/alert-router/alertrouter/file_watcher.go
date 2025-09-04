package alertrouter

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

// ConfigCallback represents a callback for a specific config file
type ConfigCallback struct {
	configPath string
	callback   func([]byte) error
}

// FileWatcher watches multiple YAML config files and calls the provided callbacks when changes occur
type FileWatcher struct {
	logger    *logrus.Logger
	watcher   *fsnotify.Watcher
	callbacks map[string]ConfigCallback // key is directory path
}

// NewFileWatcher creates a new file watcher instance
func NewFileWatcher(logger *logrus.Logger) (*FileWatcher, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %v", err)
	}

	return &FileWatcher{
		logger:    logger,
		watcher:   watcher,
		callbacks: make(map[string]ConfigCallback),
	}, nil
}

// AddWatch adds a new file to watch with its corresponding callback
func (w *FileWatcher) AddWatch(configPath string, callback func([]byte) error) error {
	dirPath := filepath.Dir(configPath)
	w.callbacks[dirPath] = ConfigCallback{
		configPath: configPath,
		callback:   callback,
	}

	// Load initial config
	if err := w.loadConfig(configPath); err != nil {
		return fmt.Errorf("failed to load initial config for %s: %v", configPath, err)
	}

	// Watch the ..data file in the config directory
	watchPath := filepath.Join(dirPath, "..data")
	if err := w.watcher.Add(filepath.Dir(watchPath)); err != nil {
		return fmt.Errorf("failed to watch config directory for %s: %v", configPath, err)
	}

	w.logger.Infof("Started watching config file: %s (watching ..data)", configPath)
	return nil
}

// Start begins watching all config files for changes
func (w *FileWatcher) Start() error {
	go w.watchLoop()
	return nil
}

// Stop stops watching all config files
func (w *FileWatcher) Stop() {
	w.watcher.Close()
}

func (w *FileWatcher) watchLoop() {
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				return
			}

			// Check for ..data file creation events
			if filepath.Base(event.Name) == "..data" && event.Op&fsnotify.Create == fsnotify.Create {
				configDir := filepath.Dir(event.Name)
				// Direct lookup by directory
				if entry, ok := w.callbacks[configDir]; ok {
					w.logger.Infof("Config file changed (via ..data creation): %s", entry.configPath)
					data, err := os.ReadFile(entry.configPath)
					if err != nil {
						w.logger.WithError(err).Errorf("Failed to read config: %s", entry.configPath)
						continue
					}
					if err := entry.callback(data); err != nil {
						w.logger.WithError(err).Errorf("Failed to process config: %s", entry.configPath)
					}
				}
			}

		case err, ok := <-w.watcher.Errors:
			if !ok {
				return
			}
			w.logger.WithError(err).Error("Error watching config files")
		}
	}
}

func (w *FileWatcher) loadConfig(configPath string) error {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("failed to read config file %s: %v", configPath, err)
	}

	dirPath := filepath.Dir(configPath)
	entry, ok := w.callbacks[dirPath]
	if !ok || entry.configPath != configPath {
		return fmt.Errorf("no callback registered for config file %s", configPath)
	}

	w.logger.WithField("path", configPath).WithField("config", string(data)).Info("Loaded config")
	return entry.callback(data)
}
