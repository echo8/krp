package main

import "github.com/echo8/krp/tests/internal/loadtest"

func main() {
	if err := loadtest.RunLocal(); err != nil {
		panic(err)
	}
}
