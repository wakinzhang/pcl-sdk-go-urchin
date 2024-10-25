package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func generate() error {
	dir, err := os.Getwd()
	if err != nil {
		fmt.Printf("err: %s\n", err)
		return err
	}
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if ext := filepath.Ext(path); ext != ".proto" {
			return nil
		}
		protoFile := strings.ReplaceAll(path, dir+string(filepath.Separator), "./")
		if strings.HasPrefix(protoFile, "./module/") ||
			strings.HasPrefix(protoFile, "./api/v1/") {

			protocInput := []string{
				"--proto_path=.",
				"--go_out=.",
				"--go_opt=paths=source_relative",
				"--go-grpc_out=.",
				"--go-grpc_opt=paths=source_relative",
				"--grpc-gateway_out=.",
				"--grpc-gateway_opt=paths=source_relative",
				"--validate_out=.",
				"--validate_opt=paths=source_relative",
				"--validate_opt=lang=go",
				"--openapiv2_out=../openapi",
				protoFile,
			}
			protocFd := exec.Command("protoc", protocInput...)
			protocFd.Stdout = os.Stdout
			protocFd.Stderr = os.Stderr
			protocFd.Dir = dir
			if err := protocFd.Run(); err != nil {
				fmt.Printf("err: %s\n", err)
				return err
			}
			fmt.Printf("protoFile: %s\n", protoFile)

			pbFile := strings.ReplaceAll(protoFile, ".proto", ".pb.go")
			protocInjectInput := []string{
				"-input=" + pbFile,
			}
			protocInjectFd := exec.Command("protoc-go-inject-tag", protocInjectInput...)
			protocInjectFd.Stdout = os.Stdout
			protocInjectFd.Stderr = os.Stderr
			protocInjectFd.Dir = dir
			if err := protocInjectFd.Run(); err != nil {
				fmt.Printf("err: %s\n", err)
				return err
			}
			fmt.Printf("pbFile: %s\n", pbFile)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return nil
}

//go:generate go run generate.go
func main() {
	err := generate()
	if err != nil {
		panic(err)
	}
}
