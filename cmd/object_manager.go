package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Object struct {
	GDId    string `json:"gd_id"`    // id
	GDPId   string `json:"gdp_id"`   // parent id. if empty, it indicates parent directory
	LastMod int64  `json:"last_mod"` // if not empty, it indicates the object is a file
	Size    int64  `json:"size"`
}

type ObjectManager struct {
	cfg               *Config
	ObjectMapFilePath string
	objectMap         map[string]*Object
	objectMapRWMu     *sync.RWMutex
}

func (om *ObjectManager) storeObject(key string, object *Object) (stored bool) {
	om.objectMapRWMu.Lock()
	defer om.objectMapRWMu.Unlock()
	_, loaded := om.objectMap[key]
	if loaded {
		return
	}
	om.objectMap[key] = object
	stored = true
	return
}

func (om *ObjectManager) isLocked(o *Object) bool {
	om.objectMapRWMu.RLock()
	defer om.objectMapRWMu.RUnlock()
	return o.GDId == ""
}

func (om *ObjectManager) updateStoredObject(o *Object, f func(o *Object)) *Object {
	om.objectMapRWMu.Lock()
	defer om.objectMapRWMu.Unlock()
	f(o)
	return o
}

func (om *ObjectManager) loadObject(key string) (*Object, bool) {
	om.objectMapRWMu.RLock()
	defer om.objectMapRWMu.RUnlock()
	if key == binPath {
		if om.cfg.RootFolderID == "" {
			om.cfg.RootFolderID = "."
		}
		return &Object{GDId: om.cfg.RootFolderID}, true
	}
	object, loaded := om.objectMap[key]
	return object, loaded
}

func (om *ObjectManager) deleteObject(key string) {
	om.objectMapRWMu.Lock()
	defer om.objectMapRWMu.Unlock()
	delete(om.objectMap, key)
}

func (om *ObjectManager) CopyObjects() map[string]*Object {
	om.objectMapRWMu.RLock()
	defer om.objectMapRWMu.RUnlock()
	data, _ := json.MarshalIndent(om.objectMap, "", "\t")
	objectMapCopy := make(map[string]*Object)
	_ = json.Unmarshal(data, &objectMapCopy)
	return objectMapCopy
}

func (om *ObjectManager) SaveToFile() error {
	data, err := json.MarshalIndent(om.CopyObjects(), "", "\t")
	if err != nil {
		return err
	}

	err = os.WriteFile(om.ObjectMapFilePath, data, os.ModePerm)
	if err != nil {
		return err
	}

	return nil
}

func (om *ObjectManager) NewObject(loc string) (*Object, bool, bool, error) {
	var loaded bool
	eObj, loaded := om.loadObject(loc)
	if loaded {
		return eObj, loaded, om.isLocked(eObj), nil
	}

	var err error
	d, b := filepath.Dir(loc), filepath.Base(loc)
	pObj, ok := om.loadObject(d)
	if !ok {
		var locked bool
		pObj, loaded, locked, err = om.NewObject(d)
		if err != nil {
			return nil, false, false, err
		}

		if loaded || locked {
			return pObj, loaded, locked, nil
		}
	}

	if om.isLocked(pObj) {
		return pObj, false, true, nil
	}

	wr, err := os.Stat(loc)
	if err != nil {
		return nil, false, false, err
	}

	op := "mkdir"
	var lastMod int64
	if !wr.IsDir() {
		op = "upload"
		lastMod = wr.ModTime().Unix()
	}

	lockedNObj := &Object{
		GDId:    "",
		GDPId:   pObj.GDId,
		LastMod: lastMod,
		Size:    wr.Size(),
	}

	stored := om.storeObject(loc, lockedNObj)
	if !stored {
		return pObj, false, true, nil
	}
	execArgs := fmt.Sprintf("cd %v && gdrive files %v %v --parent %v --print-only-id", d, op, b, pObj.GDId)
	if pObj.GDId == "." {
		execArgs = fmt.Sprintf("cd %v && gdrive files %v %v --print-only-id", d, op, b)
	}

	var nGDId string
	nGDId, err = om.execCommand("sh", "-c", execArgs)
	if err != nil {
		om.deleteObject(loc)
		return nil, false, false, err
	}

	nObject := om.updateStoredObject(lockedNObj, func(o *Object) {
		o.GDId = nGDId
	})

	if op == "upload" {
		op = "created"
	}
	fmt.Printf("%v: %v (%v)\n", op, strings.TrimPrefix(loc, binPath), getFileSizeFormatted(wr.Size()))

	return nObject, false, false, nil
}

func (om *ObjectManager) Sync(wr *WalkResp) (created, updated, locked bool, err error) {
	object, loaded, locked, err := om.NewObject(wr.loc)
	if err != nil {
		return false, false, false, err
	}

	if locked {
		return false, false, true, nil
	}

	if !loaded {
		return true, false, false, nil
	}

	updated, err = om.UpdateObjectIfModTimeChanged(wr, object)
	return false, updated, false, err
}

func (om *ObjectManager) UpdateObjectIfModTimeChanged(wr *WalkResp, object *Object) (bool, error) {
	if wr.isDir {
		return false, nil
	}

	currMod := wr.modTimeUnix
	if currMod <= object.LastMod || wr.size == object.Size {
		return false, nil
	}

	d, b := filepath.Dir(wr.loc), filepath.Base(wr.loc)
	_, err := om.execCommand("sh", "-c", fmt.Sprintf("cd %v && gdrive files update %v %v", d, object.GDId, b))
	if err != nil {
		return false, nil
	}

	originSize := object.Size
	om.updateStoredObject(object, func(o *Object) {
		o.LastMod = currMod
		o.Size = wr.size
	})

	fmt.Printf("updated: %v (%v -> %v)\n", strings.TrimPrefix(wr.loc, binPath), getFileSizeFormatted(originSize), getFileSizeFormatted(wr.size))
	return true, nil
}

func NewObjectManager(cfg *Config) (*ObjectManager, error) {
	objectMapFilePath := filepath.Join(binPath, "object_map.json")
	objectMapRaw, err := readObjectMap(objectMapFilePath)
	if err != nil {
		return nil, err
	}

	objectMap := map[string]*Object{}
	err = json.Unmarshal(objectMapRaw, &objectMap)
	if err != nil {
		return nil, err
	}

	return &ObjectManager{
		cfg:               cfg,
		ObjectMapFilePath: objectMapFilePath,
		objectMap:         objectMap,
		objectMapRWMu:     &sync.RWMutex{},
	}, nil
}

func (om *ObjectManager) DeleteObjectGDrive(loc string, object *Object) {
	defer om.deleteObject(loc)
	_, _ = om.execCommand("gdrive", "files", "delete", object.GDId, "--recursive")
	fmt.Printf("deleted: %v (%v)\n", strings.TrimPrefix(loc, binPath), getFileSizeFormatted(object.Size))
}

func readObjectMap(sourceLoc string) ([]byte, error) {
	objectMapFile, err := os.OpenFile(sourceLoc, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	defer objectMapFile.Close()

	data, err := io.ReadAll(objectMapFile)
	if err != nil {
		return nil, err
	}

	if len(data) == 0 {
		data = []byte("{}")
	}

	return data, nil
}

func (om *ObjectManager) execCommand(name string, arg ...string) (string, error) {
	if om.cfg.TestMode {
		time.Sleep(time.Millisecond * time.Duration(om.cfg.TestModeOpDelayMillis))
		return "0", nil
	}
	cmd := exec.Command(name, arg...)
	//fmt.Println(strings.Join(append([]string{name}, arg...), " "))
	stdout := bytes.NewBuffer(nil)
	cmd.Stdout = stdout
	cmd.Stderr = stdout

	err := cmd.Run()
	out := strings.TrimSpace(stdout.String())
	if err != nil {
		return "", errors.New(out)
	}
	return out, nil
}

func getFileSizeFormatted(byteSize int64) string {
	fileSizeMB := fmt.Sprintf("%.2f", float64(byteSize)/(1024*1024))
	if fileSizeMB != "0.00" {
		return fmt.Sprintf("%v MB", fileSizeMB)
	}

	fileSizeKB := fmt.Sprintf("%.2f", float64(byteSize)/1024)
	if fileSizeKB != "0.00" {
		return fmt.Sprintf("%v KB", fileSizeKB)
	}

	return fmt.Sprintf("%v B", byteSize)
}
