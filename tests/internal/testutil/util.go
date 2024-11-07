package testutil

import (
	"fmt"
	"os"
	"strings"
)

func Ptr[T any](value T) *T {
	return &value
}

func ProjectRootDir() string {
	dir, err := os.Getwd()
	if err != nil {
		panic(fmt.Errorf("failed to find project root dir: %w", err))
	}
	krpIdx := strings.LastIndex(dir, "krp")
	if krpIdx == -1 {
		panic(fmt.Errorf("failed to find project root dir from: %s", dir))
	}
	return dir[:krpIdx+3]
}

func FormatCfg(cfg string) string {
	newCfg := strings.TrimLeft(cfg, "\n")
	newCfg = strings.ReplaceAll(newCfg, "\t", "  ")
	return newCfg
}
