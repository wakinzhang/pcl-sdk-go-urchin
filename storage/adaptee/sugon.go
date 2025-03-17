package adaptee

import (
	"bufio"
	"encoding/json"
	"errors"
	"github.com/huaweicloud/huaweicloud-sdk-go-obs/obs"
	"github.com/panjf2000/ants/v2"
	sg "github.com/urchinfs/sugon-sdk/sugon"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/common"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/module"
	. "github.com/wakinzhang/pcl-sdk-go-urchin/storage/service"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type Sugon struct {
	sgClient sg.Sgclient
}

func (o *Sugon) Init(
	clusterId,
	user,
	password,
	orgId,
	secEnv,
	apiEnv string) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:Init start.")
	o.sgClient, err = sg.New(clusterId, user, password, orgId, secEnv, apiEnv)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "sg.New failed. err: %v", err)
		return err
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:Init finish.")
	return nil
}

func (o *Sugon) Upload(
	urchinServiceAddr, sourcePath string,
	taskId int32,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Sugon:Upload start. sourcePath: %s, taskId: %d, needPure: %t",
		sourcePath, taskId, needPure)

	urchinService := new(UrchinService)
	urchinService.Init(urchinServiceAddr, 10, 10)

	getTaskReq := new(GetTaskReq)
	getTaskReq.TaskId = &taskId
	getTaskReq.PageIndex = DefaultPageIndex
	getTaskReq.PageSize = DefaultPageSize

	err, getTaskResp := urchinService.GetTask(getTaskReq)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR, "GetTask failed. err: %v", err)
		return err
	}
	if len(getTaskResp.Data.List) == 0 {
		obs.DoLog(obs.LEVEL_ERROR, "task not exist. taskId: %d", taskId)
		return errors.New("task not exist")
	}

	task := getTaskResp.Data.List[0].Task
	var targetPath string
	if TaskTypeMigrate == task.Type {
		migrateObjectTaskParams := new(MigrateObjectTaskParams)
		err = json.Unmarshal([]byte(task.Params), migrateObjectTaskParams)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"MigrateObjectTaskParams Unmarshal failed. params: %s, error: %v",
				task.Params, err)
			return err
		}
		targetPath = migrateObjectTaskParams.Request.ObjUuid
	} else {
		obs.DoLog(obs.LEVEL_ERROR, "task type invalid. taskId: %d", taskId)
		return errors.New("task type invalid")
	}

	stat, err := os.Stat(sourcePath)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"os.Stat failed. sourcePath: %s, err: %v",
			sourcePath, err)
		return err
	}

	if stat.IsDir() {
		err = o.uploadFolder(sourcePath, targetPath, needPure)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"uploadFolder failed. sourcePath: %s, targetPath: %s, err: %v",
				sourcePath, targetPath, err)
			return err
		}
	} else {
		fileNode := targetPath + filepath.Base(sourcePath)
		fd, err := os.Open(sourcePath)
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.Open failed. sourcePath: %s, error: %v", sourcePath, err)
			return err
		}
		defer func() {
			errMsg := fd.Close()
			if errMsg != nil {
				obs.DoLog(obs.LEVEL_WARN,
					"Failed to close file with reason: %v", errMsg)
			}
		}()
		err = o.sgClient.Upload(fileNode, fd, stat.Size())
		if err != nil {
			obs.DoLog(obs.LEVEL_ERROR,
				"sgClient.Upload failed."+
					" sourcePath: %s, fileNode:%s, err: %v", sourcePath, fileNode, err)
			return err
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:Upload finish.")
	return err
}

func (o *Sugon) uploadFolder(
	sourcePath, targetPath string,
	needPure bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG,
		"Sugon:uploadFolder start."+
			" sourcePath: %s, targetPath: %s, needPure: %t",
		sourcePath, targetPath, needPure)

	var fileMutex sync.Mutex
	fileMap := make(map[string]int)

	obs.DoLog(obs.LEVEL_DEBUG, "uploadFolderRecord info."+
		" filepath.Dir(sourcePath): %s, filepath.Base(dirPath): %s",
		filepath.Dir(sourcePath), filepath.Base(sourcePath))

	uploadFolderRecord :=
		filepath.Dir(sourcePath) + "/" +
			filepath.Base(sourcePath) + ".upload_folder_record"

	if needPure {
		err = os.Remove(uploadFolderRecord)
		if nil != err {
			if !os.IsNotExist(err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. uploadFolderRecord: %s, err: %v",
					uploadFolderRecord, err)
				return err
			}
		}
	} else {
		fileData, err := os.ReadFile(uploadFolderRecord)
		if nil == err {
			lines := strings.Split(string(fileData), "\n")
			for _, line := range lines {
				fileMap[strings.TrimSuffix(line, "\r")] = 0
			}
		} else if !os.IsNotExist(err) {
			obs.DoLog(obs.LEVEL_ERROR,
				"os.ReadFile failed. uploadFolderRecord: %s, err: %v",
				uploadFolderRecord, err)
			return err
		}
	}

	// 创建文件夹
	_, err = o.sgClient.CreateDir(targetPath + filepath.Base(sourcePath))
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"sgClient.CreateDir failed. err: %v", err)
		return err
	}

	pool, err := ants.NewPool(DefaultSugonUploadFileTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for upload file failed. err: %v", err)
		return err
	}
	defer pool.Release()

	var isAllSuccess = true
	var wg sync.WaitGroup
	err = filepath.Walk(
		sourcePath, func(filePath string, fileInfo os.FileInfo, err error) error {
			if err != nil {
				obs.DoLog(obs.LEVEL_ERROR,
					"filepath.Walk failed."+
						" sourcePath: %s, err: %v", sourcePath, err)
				return err
			}
			if fileInfo.IsDir() {
			} else {
				wg.Add(1)
				// 处理文件
				err = pool.Submit(func() {
					defer func() {
						wg.Done()
						if err := recover(); err != nil {
							obs.DoLog(obs.LEVEL_ERROR,
								"uploadFile failed. err: %v", err)
							isAllSuccess = false
						}
					}()
					fileKey, err := filepath.Rel(sourcePath, filePath)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"filepath.Rel failed."+
								" sourcePath: %s, filePath: %s, fileKey: %s, err: %v",
							sourcePath, filePath, fileKey, err)
						return
					}
					if _, exists := fileMap[fileKey]; exists {
						obs.DoLog(obs.LEVEL_INFO,
							"file already success. fileKey: %s", fileKey)
						return
					}
					stat, err := os.Stat(filePath)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"os.Stat failed. filePath: %s, err: %v",
							filePath, err)
						return
					}
					fileNode := targetPath + fileKey
					fd, err := os.Open(filePath)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"os.Open failed."+
								" filePath: %s, error: %v", filePath, err)
						return
					}
					defer func() {
						errMsg := fd.Close()
						if errMsg != nil {
							obs.DoLog(obs.LEVEL_WARN,
								"Failed to close file with reason: %v", errMsg)
						}
					}()
					err = o.sgClient.Upload(fileNode, fd, stat.Size())
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"sgClient.Upload failed."+
								" filePath: %s, fileNode:%s, err: %v",
							filePath, fileNode, err)
						return
					}
					fileMutex.Lock()
					defer fileMutex.Unlock()
					f, err := os.OpenFile(
						uploadFolderRecord,
						os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"os.OpenFile failed."+
								" uploadFolderRecord: %s, err: %v",
							uploadFolderRecord, err)
						return
					}
					defer func() {
						errMsg := f.Close()
						if errMsg != nil {
							obs.DoLog(obs.LEVEL_WARN,
								"Failed to close file. error: %v", errMsg)
						}
					}()
					_, err = f.Write([]byte(fileKey + "\n"))
					if err != nil {
						isAllSuccess = false
						obs.DoLog(obs.LEVEL_ERROR,
							"Write file failed."+
								" uploadFolderRecord: %s, file item: %s, err: %v",
							uploadFolderRecord, fileKey, err)
						return
					}
					return
				})
				if err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"ants.Submit for upload file failed. err: %v", err)
					return err
				}
			}
			return nil
		})
	wg.Wait()

	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"filepath.Walk failed. sourcePath: %s, err: %v", sourcePath, err)
		return err
	}
	if !isAllSuccess {
		obs.DoLog(obs.LEVEL_ERROR,
			"Sugon:uploadFolder not all success. sourcePath: %s", sourcePath)
		return errors.New("uploadFolder not all success")
	} else {
		_err := os.Remove(uploadFolderRecord)
		if nil != _err {
			if !os.IsNotExist(_err) {
				obs.DoLog(obs.LEVEL_ERROR,
					"os.Remove failed. uploadFolderRecord: %s, err: %v",
					uploadFolderRecord, _err)
			}
		}
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:uploadFolder finish.")
	return nil
}

func (o *Sugon) Download(targetPath, fileNode string) (err error) {
	obs.DoLog(obs.LEVEL_DEBUG,
		"Sugon:Download start."+
			" targetPath: %s, fileNode: %s", targetPath, fileNode)

	fileMap := make(map[string]int)

	downloadFolderRecord := targetPath + fileNode + ".download_folder_record"
	fileData, err := os.ReadFile(downloadFolderRecord)
	if nil == err {
		lines := strings.Split(string(fileData), "\n")
		for _, line := range lines {
			fileMap[strings.TrimSuffix(line, "\r")] = 0
		}
	} else if !os.IsNotExist(err) {
		obs.DoLog(obs.LEVEL_ERROR,
			"ReadFile failed. downloadFolderRecord: %s, err: %v",
			downloadFolderRecord, err)
		return err
	}

	path := fileNode
	var start, limit int64 = 0, DefaultPageSize
	var fileList []sg.FileMeta
	var isAllSuccess = true
	err = o.DownloadMulti(
		targetPath,
		path,
		downloadFolderRecord,
		start,
		limit,
		&fileList,
		fileMap,
		&isAllSuccess)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"Sugon.DownloadMulti failed."+
				" targetPath: %s, path: %s, downloadFolderRecord: %s,"+
				" start: %d, limit: %d, err: %v",
			targetPath, path, downloadFolderRecord, start, limit, err)
		return err
	}

	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:Download finish.")
	return err
}

func (o *Sugon) DownloadMulti(
	targetPath, path, downloadFolderRecord string,
	start, limit int64,
	fileList *[]sg.FileMeta,
	fileMap map[string]int,
	isAllSuccess *bool) (err error) {

	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:DownloadMulti start."+
		" targetPath: %s, path: %s, downloadFolderRecord: %s, start: %d, limit: %d",
		targetPath, path, downloadFolderRecord, start, limit)

	files, err := o.sgClient.GetFileList(path, "", start, limit)
	if nil != err {
		obs.DoLog(obs.LEVEL_ERROR,
			"sgClient.GetFileList failed."+
				" path: %s, start: %d, limit: %d, err: %v", path, start, limit, err)
		return err
	}
	var fileMutex sync.Mutex
	var wg sync.WaitGroup
	pool, err := ants.NewPool(DefaultS3DownloadFileTaskNum)
	if err != nil {
		obs.DoLog(obs.LEVEL_ERROR,
			"ants.NewPool for download object failed. err: %v", err)
		return err
	}
	defer pool.Release()
	for _, file := range files.FileList {
		wg.Add(1)
		err = pool.Submit(func() {
			*fileList = append(*fileList, file)
			defer func() {
				wg.Done()
				if err := recover(); err != nil {
					obs.DoLog(obs.LEVEL_ERROR,
						"downloadFile failed. err: %v", err)
					*isAllSuccess = false
				}
			}()
			if file.IsDirectory {
				err = os.MkdirAll(targetPath+file.Path, 0644)
				if nil != err {
					obs.DoLog(obs.LEVEL_ERROR,
						"os.MkdirAll failed."+
							" path: %s, err: %v", targetPath+file.Path, err)
					*isAllSuccess = false
					return
				}
			} else {
				reader, err := o.sgClient.Download(file.Path)
				if nil != err {
					obs.DoLog(obs.LEVEL_ERROR,
						"sgClient.Download failed."+
							" filePath: %s, error: %v", file.Path, err)
					*isAllSuccess = false
					return
				}
				filePath := targetPath + file.Path
				fd, err := os.OpenFile(filePath, os.O_WRONLY, 0640)
				if nil != err {
					obs.DoLog(obs.LEVEL_ERROR,
						"os.OpenFile failed."+
							" filePath: %s, error: %v", filePath, err)
					*isAllSuccess = false
					return
				}
				defer func() {
					errMsg := fd.Close()
					if errMsg != nil {
						obs.DoLog(obs.LEVEL_WARN,
							"Failed to close file. error: %v", errMsg)
					}
				}()
				fileWriter := bufio.NewWriterSize(fd, 65536)
				part := make([]byte, 8192)
				var readErr error
				var readCount, readTotal int
				for {
					readCount, readErr = reader.Read(part)
					if readCount > 0 {
						writeCount, err := fileWriter.Write(part[0:readCount])
						if err != nil {
							obs.DoLog(obs.LEVEL_ERROR,
								"Failed to write to file. error: %v", err)
							*isAllSuccess = false
							return
						}
						if writeCount != readCount {
							obs.DoLog(obs.LEVEL_ERROR,
								"Failed to write to file."+
									" filePath: %s, expect: %d, actual: %d",
								filePath, readCount, writeCount)
							*isAllSuccess = false
							return
						}
						readTotal = readTotal + readCount
					}
					if readErr != nil {
						if readErr != io.EOF {
							obs.DoLog(obs.LEVEL_ERROR,
								"Failed to read response body. error: %v", readErr)
							*isAllSuccess = false
							return
						}
						break
					}
				}
				err = fileWriter.Flush()
				if nil != err {
					obs.DoLog(obs.LEVEL_ERROR,
						"Failed to flush file. error: %v", err)
					*isAllSuccess = false
					return
				}
			}
			fileMutex.Lock()
			defer fileMutex.Unlock()
			f, err := os.OpenFile(
				downloadFolderRecord,
				os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
			if err != nil {
				*isAllSuccess = false
				obs.DoLog(obs.LEVEL_ERROR,
					"os.OpenFile failed."+
						" downloadFolderRecord: %s, err: %v",
					downloadFolderRecord, err)
				return
			}
			defer func() {
				errMsg := f.Close()
				if errMsg != nil {
					obs.DoLog(obs.LEVEL_WARN,
						"Failed to close file. error: %v", errMsg)
				}
			}()
			_, err = f.Write([]byte(file.Path + "\n"))
			if err != nil {
				*isAllSuccess = false
				obs.DoLog(obs.LEVEL_ERROR,
					"Write file failed."+
						" downloadFolderRecord: %s, file item: %s, err: %v",
					downloadFolderRecord, file.Path, err)
				return
			}
		})
	}
	if (start + limit) < int64(files.Total) {
		start = start + limit
		err = o.DownloadMulti(
			targetPath,
			path,
			downloadFolderRecord,
			start,
			limit,
			fileList,
			fileMap,
			isAllSuccess)
		if nil != err {
			obs.DoLog(obs.LEVEL_ERROR,
				"Sugon.DownloadMulti failed."+
					" targetPath: %s, path: %s, downloadFolderRecord: %s,"+
					" start: %d, limit: %d, err: %v",
				targetPath, path, downloadFolderRecord, start, limit, err)
			return err
		}
	} else {
		for _, file := range *fileList {
			if file.IsDirectory {
				path = file.Path
				start = 0
				var newFileList []sg.FileMeta
				err = o.DownloadMulti(
					targetPath,
					path,
					downloadFolderRecord,
					start,
					limit,
					&newFileList,
					fileMap,
					isAllSuccess)
				if nil != err {
					obs.DoLog(obs.LEVEL_ERROR,
						"Sugon.DownloadMulti failed."+
							" targetPath: %s, path: %s, downloadFolderRecord: %s,"+
							" start: %d, limit: %d, err: %v",
						targetPath, path, downloadFolderRecord, start, limit, err)
					return err
				}
			}
		}
	}
	obs.DoLog(obs.LEVEL_DEBUG, "Sugon:DownloadMulti finish.")
	return err
}
