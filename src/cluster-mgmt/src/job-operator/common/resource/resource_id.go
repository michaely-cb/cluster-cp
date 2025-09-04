package resource

import (
	"fmt"
	"strings"
)

// MustRIDToTypeName splits RID into TYPE, NAME components or panics
func MustRIDToTypeName(id string) (string, string) {
	t, n, err := RIDToTypeName(id)
	if err != nil {
		panic(err.Error())
	}
	return t, n
}

// RIDToTypeName splits RID into TYPE, NAME components
func RIDToTypeName(id string) (string, string, error) {
	i, j := strings.Index(id, "/"), strings.LastIndex(id, "/")
	if i != j || i == -1 {
		return "", "", fmt.Errorf("invalid id: " + id)
	}
	return id[0:i], id[i+1:], nil
}
