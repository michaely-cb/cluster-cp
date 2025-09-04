package pkg

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/golang-jwt/jwt"
	log "github.com/sirupsen/logrus"
)

const (
	DefaultTokenExpireTimeSec = int64(10 * 60)
	defaultGraceWindowSec     = 10
	version                   = "1.0"
)

func init() {
	log.SetFormatter(&log.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
}

type UserInfo struct {
	Username string
	UID      int64
	GID      int64
	Groups   []int64
}

type UserAuth interface {
	GenerateToken(username string, uid int64, gid int64, groups []int64) (string, error)
	ValidateToken(tokenString string) (*UserInfo, error)
}

type SharedSecretUserAuth struct {
	secret             []byte
	tokenExpireTimeSec int64
}

func NewSharedSecretUserAuth(
	secret []byte,
	tokenExpireTimeSec int64,
) *SharedSecretUserAuth {
	return &SharedSecretUserAuth{
		secret:             secret,
		tokenExpireTimeSec: tokenExpireTimeSec,
	}
}

func (userAuth *SharedSecretUserAuth) GenerateToken(
	username string,
	uid int64,
	gid int64,
	groups []int64,
) (string, error) {
	var groupsString []string
	for _, num := range groups {
		groupsString = append(groupsString, strconv.FormatInt(num, 10))
	}

	token := jwt.New(jwt.SigningMethodHS512)
	claims := token.Claims.(jwt.MapClaims)

	now := jwt.TimeFunc()
	claims["user"] = username
	claims["exp"] = now.Add(time.Duration(userAuth.tokenExpireTimeSec) * time.Second).Unix()
	// Allow 10 second jitter to accommodate the time difference between the client and the server
	claims["iat"] = now.Add(-defaultGraceWindowSec * time.Second).Unix()
	claims["uid"] = strconv.FormatInt(uid, 10)
	claims["gid"] = strconv.FormatInt(gid, 10)
	if len(groups) > 0 {
		claims["groups"] = groupsString
	}
	claims["version"] = version
	tokenString, err := token.SignedString(userAuth.secret)
	if err != nil {
		log.Errorf("error generating auth token for user %d: %v", uid, err)
		return "", err
	}

	log.Debugf("Generate auth token for username=%s uid=%d gid=%d groups=%v",
		username, uid, gid, groups)
	return tokenString, nil
}

func (userAuth *SharedSecretUserAuth) parseClaims(claims jwt.MapClaims) (*UserInfo, error) {
	var uid, gid int64
	var err error
	if uid, err = strconv.ParseInt(claims["uid"].(string), 10, 64); err != nil {
		return nil, err
	}

	if gid, err = strconv.ParseInt(claims["gid"].(string), 10, 64); err != nil {
		return nil, err
	}

	var groups []int64
	if claims["groups"] != nil {
		groupsString := claims["groups"].([]interface{})
		for _, groupInt := range groupsString {
			group, err := strconv.ParseInt(groupInt.(string), 10, 64)
			if err != nil {
				return nil, err
			}
			groups = append(groups, group)
		}
	}

	userInfo := UserInfo{
		Username: claims["user"].(string),
		UID:      uid,
		GID:      gid,
		Groups:   groups,
	}
	return &userInfo, nil
}

func (userAuth *SharedSecretUserAuth) ValidateToken(tokenString string) (*UserInfo, error) {
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		_, ok := token.Method.(*jwt.SigningMethodHMAC)
		if !ok {
			log.Errorf("Not a HMAC token: %s, %s", tokenString, token.Method)
			return nil, errors.New("Unauthenticated")
		}

		return userAuth.secret, nil

	})

	if err != nil {
		// Provide user with details if the usernode clock is out of sync with server
		if jwtErr, ok := err.(*jwt.ValidationError); ok {
			log.Errorf("Error validating token: %s, %d, %s", tokenString, jwtErr.Errors, jwtErr.Error())
			if jwtErr.Errors == jwt.ValidationErrorIssuedAt {
				claims, claimsOk := token.Claims.(jwt.MapClaims)
				if claimsOk && claims["iat"] != nil {
					var iat interface{}
					iat, ok := claims["iat"].(float64)
					if ok {
						iat = int(iat.(float64))
					} else {
						iat = claims["iat"]
					}
					return nil, fmt.Errorf(
						"attempted use auth token before its issued time: issued=%v, time=%d. "+
							"Ensure user and management node clocks are in sync",
						iat, jwt.TimeFunc().Unix())
				}
			}
		} else {
			log.Errorf("Error parsing token: %s, %v", tokenString, err)
		}
		return nil, errors.New("Unauthenticated")
	}

	if !token.Valid {
		log.Errorf("Invalid token: %s", tokenString)
		return nil, errors.New("Unauthenticated")
	}

	claims := token.Claims.(jwt.MapClaims)
	userInfo, err := userAuth.parseClaims(claims)
	if err != nil {
		log.Errorf("Invalid claims: %s", err)
		return nil, errors.New("Unauthenticated")
	}

	if claims["version"] != version {
		log.Errorf("Invalid claim version: %v (expected %v)", claims["version"], version)
		return nil, errors.New("Unauthenticated")
	}

	return userInfo, nil
}
