//go:build default

package health

import (
	"errors"
	"testing"

	"github.com/prometheus/alertmanager/api/v2/client/alert"
	"github.com/prometheus/alertmanager/api/v2/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

func Test_syncer_Get(t *testing.T) {
	mockAlertClient := &mockAlertClient{}
	s := alertSyncer{
		log:         zap.New(),
		alertClient: mockAlertClient,
	}

	response1 := response1()
	var alerts models.GettableAlerts
	var fetchTime, before, after v1.Time

	_, _, err := s.Get("node1")
	assert.EqualError(t, err, "alert cache is not yet initialized yet")
	mockAlertClient.AssertExpectations(t)

	mockAlertClient.On("GetAlerts", mock.Anything).Return(response1, nil).Once()
	before = v1.Now()
	s.SyncOnce()
	after = v1.Now()
	alerts, fetchTime, err = s.Get("node1")
	assert.Nil(t, err)
	assert.EqualValues(t, response1.Payload, alerts)
	assert.True(t, fetchTime.Before(&after))
	assert.True(t, before.Before(&fetchTime))
	mockAlertClient.AssertExpectations(t)

	alerts, fetchTime, err = s.Get("node2")
	assert.Nil(t, err)
	assert.Empty(t, alerts)
	mockAlertClient.AssertExpectations(t)

	mockAlertClient.On("GetAlerts", mock.Anything).Return(nil, errors.New("an error")).Once()
	before = v1.Now()
	s.SyncOnce()
	after = v1.Now()
	alerts, fetchTime, err = s.Get("node1")
	assert.Nil(t, alerts)
	assert.EqualError(t, err, "an error")
	assert.True(t, fetchTime.Before(&after))
	assert.True(t, before.Before(&fetchTime))
	mockAlertClient.AssertExpectations(t)

}

type mockAlertClient struct {
	mock.Mock
}

func response1() *alert.GetAlertsOK {
	return &alert.GetAlertsOK{
		Payload: []*models.GettableAlert{
			{
				Alert: models.Alert{
					Labels: map[string]string{
						"type":     "NodeError",
						"instance": "node1",
					},
				},
			},
		},
	}
}

func (m *mockAlertClient) GetAlerts(params *alert.GetAlertsParams, _ ...alert.ClientOption) (*alert.GetAlertsOK, error) {
	args := m.Called(params)
	resp := args.Get(0)
	if resp == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*alert.GetAlertsOK), args.Error(1)
}
