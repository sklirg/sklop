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

package controller

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	thingsv1 "github.com/sklirg/sklop/api/v1"
)

const (
	// SklappTakeoverLabel can be used for existing resources to connect them to a SklApp if the name doesn't match.
	SklappTakeoverLabel string = "sklapp.things.sklirg.io/name"
)

type loggerSklapp string

const (
	loggerSklappName loggerSklapp = "sklapp_name"
)

// SklAppReconciler reconciles a SklApp object
type SklAppReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=things.sklirg.io,resources=sklapps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=things.sklirg.io,resources=sklapps/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=things.sklirg.io,resources=sklapps/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=deployments;statefulsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *SklAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := logf.FromContext(ctx)

	// - Ignore deleted app and requeue other errors
	// - Create SA for app
	// - Check if deployment/sts exists - support taking over existing resources by labels

	var app thingsv1.SklApp
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, &app)
	if err != nil && errors.IsNotFound(err) {
		return ctrl.Result{}, nil
	} else if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	ctx = context.WithValue(ctx, loggerSklappName, app.Name)
	logger = logger.WithValues("sklapp", app.Name)

	logger.Info("reconciling")

	// Ensure SA exists
	var serviceAccount corev1.ServiceAccount
	err = r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &serviceAccount)
	if err != nil && errors.IsNotFound(err) {
		serviceAccount = corev1.ServiceAccount{
			ObjectMeta: v1.ObjectMeta{
				Name:      app.Name,
				Namespace: app.Namespace,
			},
			AutomountServiceAccountToken: ptr.To(false),
		}
		err := r.Create(ctx, &serviceAccount)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	} else if err != nil && !errors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	updated, err := r.reconcileApplicationType(ctx, &app)
	if err != nil {
		return ctrl.Result{}, fmt.Errorf("failed to reconcile application type: %w", err)
	} else if updated {
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	return ctrl.Result{}, nil
}

func (r *SklAppReconciler) reconcileApplicationType(ctx context.Context, app *thingsv1.SklApp) (bool, error) {
	if app.Spec.ApplicationType == nil {
		return false, fmt.Errorf("app \"%s\" does not have application type", app.Name)
	}

	switch *app.Spec.ApplicationType {
	case "Deployment":
		_, updated, err := r.deployment(ctx, app)
		if err != nil {
			return false, err
		}
		return updated, nil
	case "StatefulSet":
		_, err := r.statefulset(ctx)
		if err != nil {
			return false, err
		}

	default:
		return false, fmt.Errorf("application type \"%s\" not valid", *app.Spec.ApplicationType)
	}

	return false, nil
}

func (r *SklAppReconciler) deployment(ctx context.Context, app *thingsv1.SklApp) (*appsv1.Deployment, bool, error) {
	// look for apps via same name (default)
	// if no found, look for apps via labels => if found, set ownerreference
	// if still no found, create it

	var deploy *appsv1.Deployment
	err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, deploy)
	if err != nil && !errors.IsNotFound(err) {
		return nil, false, err
	}

	// Look for the deployment via labels instead
	if deploy == nil {
		req, err := labels.NewRequirement(SklappTakeoverLabel, selection.Equals, []string{app.Name})
		if err != nil {
			return nil, false, err
		}
		selector := labels.NewSelector().Add(*req)
		var deploymentlist appsv1.DeploymentList
		err = r.List(ctx, &deploymentlist, &client.ListOptions{
			LabelSelector: client.MatchingLabelsSelector{
				Selector: selector,
			},
		})
		if err != nil && !errors.IsNotFound(err) {
			return nil, false, err
		}
		if len(deploymentlist.Items) > 1 {
			return nil, false, fmt.Errorf("found more than 1 deployment labelled with this app")
		}
		if len(deploymentlist.Items) == 1 {
			deploy = &deploymentlist.Items[0]
		}
	}

	// If still no deployment, create it
	if deploy == nil {
		deploy = &appsv1.Deployment{
			ObjectMeta: v1.ObjectMeta{
				Name:            app.Name,
				Namespace:       app.Namespace,
				OwnerReferences: []v1.OwnerReference{ownerReference(app)},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: app.Spec.Replicas,
				Selector: &v1.LabelSelector{
					MatchLabels: map[string]string{
						"app":                    app.Name,
						"app.kubernetes.io/name": app.Name,
					},
				},
				Template: podTemplate(app),
			},
		}
		err := r.Create(ctx, deploy)
		if err != nil {
			return nil, false, err
		}
		return deploy, true, nil
	}

	hasOwnerRef := false
	owners := deploy.GetOwnerReferences()
	for _, owner := range owners {
		if owner.UID == app.UID {
			hasOwnerRef = true
			break
		}
	}
	if !hasOwnerRef {
		deploy.OwnerReferences = append(deploy.OwnerReferences, ownerReference(app))
		err := r.Update(ctx, deploy)
		if err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}

	return deploy, false, nil
}

func (r *SklAppReconciler) statefulset(ctx context.Context) (*appsv1.StatefulSet, error) {
	var sts appsv1.StatefulSet
	return &sts, nil
}

func podTemplate(app *thingsv1.SklApp) corev1.PodTemplateSpec {
	containers := make([]corev1.Container, 1)
	containers[0] = corev1.Container{
		Name:      app.Name,
		Image:     app.Spec.Image,
		Resources: app.Spec.Resources,
		Ports: []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: app.Spec.Port,
			},
		},
		Env:     app.Spec.Env,
		EnvFrom: app.Spec.EnvFrom,
	}
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			ServiceAccountName: app.Name,
			Containers:         containers,
		},
	}
}

func ownerReference(app *thingsv1.SklApp) v1.OwnerReference {
	return v1.OwnerReference{
		APIVersion: "v1",
		Kind:       "SklApp",
		Name:       app.Name,
		UID:        app.UID,
		Controller: ptr.To(true),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *SklAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&thingsv1.SklApp{}).
		Named("sklapp").
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
