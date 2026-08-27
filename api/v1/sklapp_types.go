/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package v1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Important: Run "make" to regenerate code after modifying this file

// The following markers will use OpenAPI v3 schema to validate the value
// More info: https://book.kubebuilder.io/reference/markers/crd-validation.html

// For Kubernetes API conventions, see:
// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

// SklAppSpec defines the desired state of SklApp
type SklAppSpec struct {
	// Image to use for the application
	// +kubebuilder:validation:Pattern="^(?:.+/)?[^:/]+(?::[^/]+|@sha256:[0-9a-f]{64})$"
	Image string `json:"image"`
	// ApplicationType specifies what kind of apps/v1 to use for running the app. Defaults to Deployment.
	// +kubebuilder:default=Deployment
	// +kubebuilder:validation:Enum=Deployment;StatefulSet
	// +kubebuilder:validation:Optional
	ApplicationType *string `json:"application_type"`
	// Replicas for the application
	// +kubebuilder:default=2
	// +kubebuilder:validation:Optional
	Replicas *int32 `json:"replicas"`
	// Resources for the application
	Resources corev1.ResourceRequirements `json:"resources"`
	// Port for the application
	// +kubebuilder:validation:Optional
	Port int32 `json:"port"`
	// Env variables to pass to the application container
	// +kubebuilder:validation:Optional
	Env []corev1.EnvVar `json:"env"`
	// EnvFrom to pass to the application container
	// +kubebuilder:validation:Optional
	EnvFrom []corev1.EnvFromSource `json:"envFrom"`
	// Volumes to make available to the pod
	// +kubebuilder:validation:Optional
	Volumes []corev1.Volume `json:"volumes"`
	// VolumeMounts to mount into the application container
	// +kubebuilder:validation:Optional
	VolumeMounts []corev1.VolumeMount `json:"volumeMounts"`
	// RunAsUser overrides the pod's securityContext.runAsUser. The pod
	// always runs with securityContext.runAsNonRoot=true; images whose
	// default user is already non-root need no further configuration, but
	// images that default to root (uid 0) will fail to start unless
	// RunAsUser is set here to a non-root UID the image supports.
	// +kubebuilder:validation:Optional
	RunAsUser *int64 `json:"runAsUser"`
	// URL exposes the application to the world. When set, a Service and an
	// HTTPRoute are created routing traffic for this hostname to the app,
	// fronted by an oauth2-proxy sidecar that authenticates requests before
	// they reach the application container.
	// +kubebuilder:validation:Optional
	URL *string `json:"url"`
	// Gateway identifies the Gateway the HTTPRoute (created when URL is set)
	// should attach to. If unset, the controller's configured default
	// Gateway (--gateway-name/--gateway-namespace flags) is used instead.
	// +kubebuilder:validation:Optional
	Gateway *GatewayReference `json:"gateway"`
	// OAuth2ProxySecretName names the Secret providing the oauth2-proxy
	// sidecar's envFrom (OIDC client credentials, cookie secret, etc.),
	// only used when URL is set. Defaults to "<name>-oauth2-proxy-config".
	// +kubebuilder:validation:Optional
	OAuth2ProxySecretName *string `json:"oauth2ProxySecretName"`
}

// GatewayReference identifies a Gateway API Gateway resource.
type GatewayReference struct {
	// Name of the Gateway.
	Name string `json:"name"`
	// Namespace of the Gateway. Defaults to the SklApp's own namespace.
	// +kubebuilder:validation:Optional
	Namespace *string `json:"namespace"`
}

// SklAppStatus defines the observed state of SklApp.
type SklAppStatus struct {
	// conditions represent the current state of the SklApp resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// The "Ready" condition reflects the outcome of the most recent reconcile
	// pass: True once the desired Deployment/StatefulSet exists and is owned
	// by this SklApp, False on a reconciliation error, Unknown while still
	// being created or updated.
	// Healthy reflects the readiness of the underlying Deployment/StatefulSet,
	// formatted as "readyReplicas/replicas".
	// +optional
	Healthy string `json:"healthy,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Ready",type="string",JSONPath=".status.ready"
// +kubebuilder:printcolumn:name="URL",type="string",JSONPath=".spec.url"
// +kubebuilder:printcolumn:name="Healthy",type="string",JSONPath=".status.conditions[?(@.type=='Healthy')].status"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// SklApp is the Schema for the sklapps API
type SklApp struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of SklApp
	// +required
	Spec SklAppSpec `json:"spec"`

	// status defines the observed state of SklApp
	// +optional
	Status SklAppStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// SklAppList contains a list of SklApp
type SklAppList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []SklApp `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &SklApp{}, &SklAppList{})
		return nil
	})
}
