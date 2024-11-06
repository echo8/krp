package testutil

func Ptr[T any](value T) *T {
	return &value
}

func ProjectRootDir() string {
	// dir, err := os.Getwd()
	// if err != nil {
	// 	panic(fmt.Errorf("failed to find project root dir: %w", err))
	// }
	// pathSplit := strings.Split(dir, string(filepath.Separator))
	// if len(pathSplit) > 2 && pathSplit[len(pathSplit)-1] == "internal" &&
	// 	pathSplit[len(pathSplit)-2] == "e2e" {
	// 	return string(filepath.Separator) + filepath.Join(pathSplit[0:len(pathSplit)-2]...)
	// } else if len(pathSplit) > 1 && pathSplit[len(pathSplit)-1] == "e2e" {
	// 	return string(filepath.Separator) + filepath.Join(pathSplit[0:len(pathSplit)-1]...)
	// }
	// return dir
	return "/home/greg/workspace/krp"
}
