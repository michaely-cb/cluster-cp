package common

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func TestCreateWithRetryAndTimeout(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToCreate := &discoveryv1.EndpointSlice{}
		mockClient.On("Create", mock.Anything, objectToCreate, mock.Anything).Return(nil).Once()

		err := CreateWithRetryAndTimeout(context.Background(), mockClient, objectToCreate)

		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("success after error", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToCreate := &discoveryv1.EndpointSlice{}

		mockClient.On("Create", mock.Anything, objectToCreate, mock.Anything).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Once()

		mockClient.On("Create", mock.Anything, objectToCreate, mock.Anything).Return(nil).Once()

		err := CreateWithRetryAndTimeout(context.Background(), mockClient, objectToCreate)

		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("fails after max attempts", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToCreate := &discoveryv1.EndpointSlice{}

		// Should retry a max of 5 times
		mockClient.On("Create", mock.Anything, objectToCreate, mock.Anything).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Times(5)

		err := CreateWithRetryAndTimeout(context.Background(), mockClient, objectToCreate)

		assert.True(t, apierrors.IsInternalError(err))
		mockClient.AssertExpectations(t)
	})

	t.Run("timesout the operation", func(t *testing.T) {
		shortenTimeout(t)
		mockClient := &MockK8SClient{}
		objectToCreate := &discoveryv1.EndpointSlice{}

		mockClient.On("Create", mock.Anything, objectToCreate, mock.Anything).
			Run(sleepLongerThanOpTimeout).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Times(1)

		err := CreateWithRetryAndTimeout(context.Background(), mockClient, objectToCreate)

		assert.True(t, apierrors.IsInternalError(err))
		mockClient.AssertExpectations(t)
	})
}

func TestPatchWithRetryAndTimeout(t *testing.T) {
	t.Run("success on first attempt", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToPatch := &discoveryv1.EndpointSlice{}
		patch := client.RawPatch(types.MergePatchType, []byte{})
		mockClient.On("Patch", mock.Anything, objectToPatch, patch, mock.Anything).Return(nil).Once()

		err := PatchWithRetryAndTimeout(context.Background(), mockClient, objectToPatch, patch)

		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("success after error", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToPatch := &discoveryv1.EndpointSlice{}
		patch := client.RawPatch(types.MergePatchType, []byte{})

		mockClient.On("Patch", mock.Anything, objectToPatch, patch, mock.Anything).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Once()

		mockClient.On("Patch", mock.Anything, objectToPatch, patch, mock.Anything).Return(nil).Once()

		err := PatchWithRetryAndTimeout(context.Background(), mockClient, objectToPatch, patch)

		assert.NoError(t, err)
		mockClient.AssertExpectations(t)
	})

	t.Run("fails after max attempts", func(t *testing.T) {
		mockClient := &MockK8SClient{}
		objectToPatch := &discoveryv1.EndpointSlice{}
		patch := client.RawPatch(types.MergePatchType, []byte{})

		// Should retry a max of 5 times
		mockClient.On("Patch", mock.Anything, objectToPatch, patch, mock.Anything).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Times(5)

		err := PatchWithRetryAndTimeout(context.Background(), mockClient, objectToPatch, patch)

		assert.True(t, apierrors.IsInternalError(err))
		mockClient.AssertExpectations(t)
	})

	t.Run("timesout the operation", func(t *testing.T) {
		shortenTimeout(t)

		mockClient := &MockK8SClient{}
		objectToPatch := &discoveryv1.EndpointSlice{}
		patch := client.RawPatch(types.MergePatchType, []byte{})

		mockClient.On("Patch", mock.Anything, objectToPatch, patch, mock.Anything).
			Run(sleepLongerThanOpTimeout).
			Return(apierrors.NewInternalError(errors.New("Internal error"))).
			Times(1)

		err := PatchWithRetryAndTimeout(context.Background(), mockClient, objectToPatch, patch)

		assert.True(t, apierrors.IsInternalError(err))
		mockClient.AssertExpectations(t)
	})
}

func sleepLongerThanOpTimeout(mock.Arguments) {
	time.Sleep(defaultTimeout + 4*time.Second)
}

func shortenTimeout(t *testing.T) {
	originalTimeout := defaultTimeout
	defaultTimeout = 2 * time.Second
	t.Cleanup(func() {
		defaultTimeout = originalTimeout
	})
}
