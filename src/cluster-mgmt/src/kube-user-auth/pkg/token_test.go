package pkg

import (
	"crypto/rand"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func generateSecret() []byte {
	byteSize := 512

	secret := make([]byte, byteSize)
	rand.Read(secret)
	return secret
}

func TestUserAuth(t *testing.T) {
	secret := generateSecret()
	userAuth := NewSharedSecretUserAuth(secret, 5)
	username := "test-user"
	uid := int64(100)
	gid := int64(101)
	groups := []int64{101, 102}
	token, err := userAuth.GenerateToken(username, uid, gid, groups)
	assert.True(t, err == nil)

	userInfo, err := userAuth.ValidateToken(token)
	require.True(t, err == nil)
	assert.True(t, userInfo.Username == username && userInfo.UID == uid && userInfo.GID == gid && reflect.DeepEqual(userInfo.Groups, groups))

	// Test token expiration
	jwt.TimeFunc = func() time.Time {
		return time.Now().Add(10 * time.Second)
	}
	defer func() {
		jwt.TimeFunc = time.Now
	}()
	userInfo, err = userAuth.ValidateToken(token)
	assert.True(t, err != nil && userInfo == nil)
	assert.True(t, strings.Contains(err.Error(), "Unauthenticated"))
}

func TestEmptyGroups(t *testing.T) {
	secret := generateSecret()
	userAuth := NewSharedSecretUserAuth(secret, 5)
	username := "test-user"
	uid := int64(100)
	gid := int64(101)
	var groups []int64
	token, err := userAuth.GenerateToken(username, uid, gid, groups)
	assert.True(t, err == nil)

	userInfo, err := userAuth.ValidateToken(token)
	assert.True(t, err == nil)
	assert.True(t, userInfo.Username == username && userInfo.UID == uid && userInfo.GID == gid && reflect.DeepEqual(userInfo.Groups, groups))
}

func TestEmptyUsername(t *testing.T) {
	secret := generateSecret()
	userAuth := NewSharedSecretUserAuth(secret, 5)
	username := ""
	uid := int64(100)
	gid := int64(101)
	groups := []int64{101}
	token, err := userAuth.GenerateToken(username, uid, gid, groups)
	assert.True(t, err == nil)

	userInfo, err := userAuth.ValidateToken(token)
	assert.True(t, err == nil)
	assert.True(t, userInfo.Username == username && userInfo.UID == uid && userInfo.GID == gid && reflect.DeepEqual(userInfo.Groups, groups))
}

func TestFakeToken(t *testing.T) {
	secret := generateSecret()
	userAuth := NewSharedSecretUserAuth(secret, 5)
	username := "test-user"
	uid := int64(100)
	gid := int64(101)
	groups := []int64{101, 102}
	token1, err := userAuth.GenerateToken(username, uid, gid, groups)
	assert.True(t, err == nil)

	groups2 := []int64{101, 102, 103}
	token2, err := userAuth.GenerateToken(username, uid, gid, groups2)
	assert.True(t, err == nil)

	// Replace the second section in `token1` with the one in `token2` to fake a new token
	// with tampered groups value. The validation should fail.
	parts1 := strings.Split(token1, ".")
	parts2 := strings.Split(token2, ".")
	assert.True(t, len(parts1) == 3 && len(parts2) == 3)
	parts1[1] = parts2[1]

	userInfo, err := userAuth.ValidateToken(strings.Join(parts1, "."))
	assert.True(t, err != nil && userInfo == nil)
	assert.True(t, strings.Contains(err.Error(), "Unauthenticated"))
}

func TestTokenUsedBeforeIssued(t *testing.T) {
	defer func() {
		jwt.TimeFunc = time.Now
	}()

	secret := generateSecret()
	userAuth := NewSharedSecretUserAuth(secret, DefaultTokenExpireTimeSec)
	jwt.TimeFunc = func() time.Time {
		return time.Unix(600, 0)
	}

	token1, err := userAuth.GenerateToken("foo", int64(100), int64(101), []int64{101, 102})
	assert.NoError(t, err)

	jwt.TimeFunc = func() time.Time {
		return time.Unix(580, 0)
	}
	userInfo, err := userAuth.ValidateToken(token1)
	require.Nil(t, userInfo)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attempted use auth token before its issued time: issued=590, time=580")
}
