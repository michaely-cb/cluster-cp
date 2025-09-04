//go:build !e2e

package csctl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestValidateLabels(t *testing.T) {
	for _, testcase := range []struct {
		v       string
		keyOk   bool
		valueOk bool
	}{
		{v: "end-special_"},
		{v: "_start_special"},
		{v: "illegal@characters"},
		{v: "_"},
		{v: "."},
		{v: strings.Repeat("long", 64/4)},

		{v: "contains--special", keyOk: true, valueOk: true},
		{v: "l" + strings.Repeat("o", 60) + "ng", keyOk: true, valueOk: true},
		{v: "", valueOk: true},
	} {
		err := ValidateLabelKey(testcase.v)
		if testcase.keyOk && err != nil {
			t.Errorf("testcase key=%s got unexpected err (%v)", testcase.v, err)
		} else if !testcase.keyOk && err == nil {
			t.Errorf("testcase key=%s didn't get expected err", testcase.v)
		}

		err = ValidateLabelValue(&testcase.v)
		if testcase.valueOk && err != nil {
			t.Errorf("testcase value=%s got unexpected err (%v)", testcase.v, err)
		} else if !testcase.valueOk && err == nil {
			t.Errorf("testcase value=%s didn't get expected err", testcase.v)
		}
	}
}

func TestNodeMarketingRole(t *testing.T) {
	for _, testcase := range []struct {
		role   string
		labels []string
	}{
		{role: "any", labels: []string{"management", "memory"}},
		{role: "coordinator", labels: []string{"coordinator"}},
		{role: "management", labels: []string{"management", "coordinator"}},
		{role: "management", labels: []string{"management"}},
	} {
		labels := make(map[string]string)
		for _, role := range testcase.labels {
			labels[fmt.Sprintf("k8s.cerebras.com/node-role-%s", role)] = ""
		}
		node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{
			Labels: labels,
		}}
		assert.Equal(t, testcase.role, getNodeMarketingRole(node))
	}
}
