package dbqcore

import "fmt"

const (
	DbqCoreLibVersion = "0.0.1"
)

func PrintDbqVersion() {
	fmt.Println("DBQ Core version: " + DbqCoreLibVersion)
}
