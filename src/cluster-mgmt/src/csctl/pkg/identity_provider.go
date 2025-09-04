package pkg

import (
	"fmt"
	"os/user"
	"strconv"
)

type UserIdentityProvider interface {
	GetUidGidGroups() (int64, int64, []int64, error)
}

type DefaultUserIdentityProvider struct{}

func (d DefaultUserIdentityProvider) GetUidGidGroups() (int64, int64, []int64, error) {
	user, err := user.Current()
	if err != nil {
		return 0, 0, nil, fmt.Errorf("can't get the current user")
	}

	uid, err := strconv.ParseInt(user.Uid, 10, 64)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("can't get the current user id")
	}
	gid, err := strconv.ParseInt(user.Gid, 10, 64)
	if err != nil {
		return 0, 0, nil, fmt.Errorf("can't get the current group id")
	}

	groupsString, err := user.GroupIds()
	groups := make([]int64, len(groupsString))
	for index, groupString := range groupsString {
		group, err := strconv.ParseInt(groupString, 10, 64)
		if err != nil {
			return 0, 0, nil, fmt.Errorf("group %s is not an integer", groupString)
		}
		groups[index] = group
	}

	return uid, gid, groups, nil
}
