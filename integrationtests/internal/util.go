package integrationtest

func Ptr[T any](value T) *T {
	return &value
}
