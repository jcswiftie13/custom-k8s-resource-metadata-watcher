package collector

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// notFoundf returns a NotFound-class apierror so callers can use
// apierrors.IsNotFound to distinguish cache misses from real failures.
func notFoundf(format string, args ...interface{}) error {
	msg := fmt.Sprintf(format, args...)
	return apierrors.NewGenericServerResponse(
		404,
		"GET",
		schema.GroupResource{Resource: "cache"},
		"",
		msg,
		0,
		true,
	)
}

// ensure metav1 import is used; some tools optimise it out otherwise.
var _ = metav1.ListOptions{}
