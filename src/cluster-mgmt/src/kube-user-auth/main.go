package main

import (
	"fmt"
	"log"
	"os"
	"os/user"

	"cerebras.net/kube-user-auth/pkg"
)

const (
	tokenExpireTimeSec = pkg.DefaultTokenExpireTimeSec
	secretFile         = "/root/.cerebras/user-auth-secret"
	versionString      = "version"
	version            = "v1"
)

func main() {
	if len(os.Args) == 2 {
		if os.Args[1] == versionString {
			fmt.Printf("version %s\n", version)
			return
		}
	}

	secret, err := os.ReadFile(secretFile)
	if err != nil {
		log.Printf("Error reading file %s: %s", secretFile, err)
		os.Exit(1)
	}

	userAuth := pkg.NewSharedSecretUserAuth(secret, tokenExpireTimeSec)

	currentUser, err := user.Current()
	var username string
	if err != nil {
		log.Printf("Error: can't get current user, ignore...")
	} else {
		username = currentUser.Username
	}

	uid := int64(os.Getuid())
	gid := int64(os.Getgid())
	groups, err := os.Getgroups()
	if err != nil {
		log.Printf("Can't get groups, ignore...")
	}

	var groupsInt64 []int64
	for _, group := range groups {
		groupsInt64 = append(groupsInt64, int64(group))
	}
	token, err := userAuth.GenerateToken(username, uid, gid, groupsInt64)
	if err != nil {
		log.Printf("Can't generate token")
		os.Exit(1)
	}
	fmt.Printf("Token=%s", token)
}
