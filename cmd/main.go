package main

import (
	"fmt"
	"github.com/bearaujus/bworker/pool"
	"gopkg.in/yaml.v2"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

type (
	Config struct {
		GDAccountName  string `yaml:"gd_account_name"`
		GDRootFolderID string `yaml:"gd_root_folder_id"`

		SyncTargetPath  string `yaml:"sync_target_path"`
		SyncDelayMinute int    `yaml:"sync_delay_minute"`
		SyncWorker      int    `yaml:"sync_worker"`
		SyncRetry       int    `yaml:"sync_retry"`

		TestMode              bool `yaml:"test_mode"`
		TestModeOpDelayMillis int  `yaml:"test_mode_op_delay_ms"`
	}
)

func main() {
	cfgRaw, err := os.ReadFile("config.yaml")
	if err != nil {
		panic(err)
	}

	cfg := Config{}
	err = yaml.Unmarshal(cfgRaw, &cfg)
	if err != nil {
		panic(err)
	}

	cmd := exec.Command("gdrive", "account", "switch", cfg.GDAccountName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stdout
	err = cmd.Run()
	if err != nil {
		panic(err)
	}

	om, err := NewObjectManager(&cfg)
	if err != nil {
		panic(err)
	}

	for {
		delay := time.Duration(cfg.SyncDelayMinute) * time.Minute
		fmt.Println("Syncing...")

		err = syncFiles(&cfg, om)
		t := time.Now().Add(delay).Format(time.DateTime)
		msg := fmt.Sprintf("Synced! next schedule: %v", t)
		if err != nil {
			msg = fmt.Sprintf("Sync error! err: (%v). next schedule: %v", err, t)
		}

		fmt.Println(msg)
		printSep()
		time.Sleep(delay)
	}
}

type WalkResp struct {
	loc         string
	modTimeUnix int64
	isDir       bool
	size        int64
}

func syncFiles(cfg *Config, om *ObjectManager) error {
	var erw error
	bw := pool.NewBWorkerPool(cfg.SyncWorker, pool.WithError(&erw), pool.WithRetry(cfg.SyncRetry))
	defer bw.Shutdown()
	ntrLock := sync.Mutex{}

	var tr []WalkResp
	if err := filepath.Walk(cfg.SyncTargetPath, func(loc string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		tr = append(tr, WalkResp{
			loc:         loc,
			modTimeUnix: info.ModTime().Unix(),
			isDir:       info.IsDir(),
			size:        info.Size(),
		})
		return nil
	}); err != nil {
		return err
	}

	for {
		ntrLock.Lock()
		ltr := len(tr)
		ntrLock.Unlock()
		if ltr == 0 {
			break
		}
		var ntr []WalkResp
		for _, wr := range tr {
			wrCp := wr
			bw.Do(func() error {
				_, _, locked, err := om.Sync(&wrCp)
				if err != nil {
					return err
				}
				if locked {
					ntrLock.Lock()
					ntr = append(ntr, wrCp)
					ntrLock.Unlock()
					return nil
				}
				return nil
			})
		}
		bw.Wait()
		if erw != nil {
			return erw
		}

		ntrLock.Lock()
		tr = ntr
		ntrLock.Unlock()
		printSep()
	}

	deletedQueue := om.CopyObjects()
	if err := filepath.Walk(cfg.SyncTargetPath, func(loc string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		delete(deletedQueue, loc)
		return nil
	}); err != nil {
		return err
	}

	if len(deletedQueue) != 0 {
		for loc, object := range deletedQueue {
			locCp, objectCp := loc, object
			bw.Do(func() error {
				om.DeleteObjectGDrive(locCp, objectCp)
				return nil
			})
		}
		bw.Wait()
		printSep()
	}

	err := om.SaveToFile()
	if err != nil {
		return err
	}
	return nil
}

func printSep() {
	fmt.Println("------------------------------------------------------------------")
}
