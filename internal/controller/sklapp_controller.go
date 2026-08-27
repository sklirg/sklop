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
	"errors"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	thingsv1 "github.com/sklirg/sklop/api/v1"
)

const (
	// SklappTakeoverLabel can be used for existing resources to connect them to a SklApp if the name doesn't match.
	SklappTakeoverLabel string = "sklapp.things.sklirg.io/name"

	SklappAppLabel     string = "app"
	SklappAppNameLabel string = "app.kubernetes.io/name"

	SklappStatusProgressing string = "Progressing"
	SklappStatusAvailable   string = "Available"
	SklappStatusDegraded    string = "Degraded"

	SklappStatusReconciling         string = "Reconciling"
	SklappStatusReconciliationError string = "ReconciliationError"
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
// +kubebuilder:rbac:groups="",resources=serviceaccounts;services,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.24.1/pkg/reconcile
func (r *SklAppReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, reterr error) {
	logger := logf.FromContext(ctx)

	// - Ignore deleted app and requeue other errors
	// - Create SA for app
	// - Check if deployment/sts exists - support taking over existing resources by labels

	var app thingsv1.SklApp
	err := r.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, &app)
	if err != nil && apierrors.IsNotFound(err) {
		return ctrl.Result{}, nil
	} else if err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}

	ctx = context.WithValue(ctx, loggerSklappName, app.Name)
	logger = logger.WithValues("sklapp", app.Name)

	logger.Info("reconciling")

	// Update status when exiting
	defer func() {
		if statusErr := r.Status().Update(ctx, &app); statusErr != nil {
			reterr = errors.Join(reterr, statusErr)
		}
	}()

	if len(app.Status.Conditions) == 0 {
		meta.SetStatusCondition(&app.Status.Conditions, v1.Condition{
			Type:    SklappStatusProgressing,
			Status:  v1.ConditionUnknown,
			Reason:  SklappStatusReconciling,
			Message: "Reconciling",
		})
	} else {
		for _, condition := range app.Status.Conditions {
			if condition.Type == SklappStatusDegraded {
				meta.SetStatusCondition(&app.Status.Conditions, v1.Condition{
					Type:    SklappStatusProgressing,
					Status:  v1.ConditionUnknown,
					Reason:  SklappStatusReconciling,
					Message: "Reconciling",
				})
				break
			}
		}
	}

	// Ensure SA exists
	var serviceAccount corev1.ServiceAccount
	err = r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &serviceAccount)
	if err != nil && apierrors.IsNotFound(err) {
		serviceAccount = corev1.ServiceAccount{
			ObjectMeta: v1.ObjectMeta{
				Name:            app.Name,
				Namespace:       app.Namespace,
				OwnerReferences: []v1.OwnerReference{ownerReference(&app)},
			},
			AutomountServiceAccountToken: new(false),
		}
		err := r.Create(ctx, &serviceAccount)
		if err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	} else if err != nil && !apierrors.IsNotFound(err) {
		meta.SetStatusCondition(&app.Status.Conditions, v1.Condition{
			Type:    SklappStatusDegraded,
			Status:  v1.ConditionTrue,
			Reason:  SklappStatusReconciliationError,
			Message: fmt.Sprintf("Failed to get ServiceAccount: %s", err),
		})
		return ctrl.Result{}, err
	}

	updated, err := r.reconcileApplicationType(ctx, &app)
	if err != nil {
		errmsg := fmt.Sprintf("failed to reconcile application type \"%s\": %s", *app.Spec.ApplicationType, err)
		meta.SetStatusCondition(&app.Status.Conditions, v1.Condition{
			Type:    SklappStatusDegraded,
			Status:  v1.ConditionTrue,
			Reason:  SklappStatusReconciliationError,
			Message: errmsg,
		})
		return ctrl.Result{}, errors.New(errmsg)
	} else if updated {
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	meta.SetStatusCondition(&app.Status.Conditions, v1.Condition{
		Type:    SklappStatusAvailable,
		Status:  v1.ConditionTrue,
		Reason:  "Reconciled",
		Message: "Reconciled",
	})

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
		_, updated, err := r.statefulset(ctx, app)
		if err != nil {
			return false, err
		}
		return updated, nil

	default:
		return false, fmt.Errorf("application type \"%s\" not valid", *app.Spec.ApplicationType)
	}
}

func (r *SklAppReconciler) deployment(ctx context.Context, app *thingsv1.SklApp) (*appsv1.Deployment, bool, error) {
	// look for apps via same name (default)
	// if no found, look for apps via labels => if found, set ownerreference
	// if still no found, create it

	var deploy appsv1.Deployment
	found := true
	err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &deploy)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, err
	} else if err != nil {
		found = false
	}

	// Look for the deployment via labels instead
	if !found {
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
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, false, err
		}
		if len(deploymentlist.Items) > 1 {
			return nil, false, fmt.Errorf("found more than 1 deployment labelled with this app")
		}
		if len(deploymentlist.Items) == 1 {
			deploy = deploymentlist.Items[0]
			found = true
		}
	}

	// If still no deployment, create it
	if !found {
		deploy = appsv1.Deployment{
			ObjectMeta: v1.ObjectMeta{
				Name:            app.Name,
				Namespace:       app.Namespace,
				OwnerReferences: []v1.OwnerReference{ownerReference(app)},
			},
			Spec: appsv1.DeploymentSpec{
				Replicas: app.Spec.Replicas,
				Selector: &v1.LabelSelector{
					MatchLabels: map[string]string{
						SklappAppLabel:     app.Name,
						SklappAppNameLabel: app.Name,
					},
				},
				Template: podTemplate(app),
			},
		}
		err := r.Create(ctx, &deploy)
		if err != nil {
			return nil, false, err
		}
		return &deploy, true, nil
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
		err := r.Update(ctx, &deploy)
		if err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}

	return &deploy, false, nil
}

func (r *SklAppReconciler) statefulset(ctx context.Context, app *thingsv1.SklApp) (*appsv1.StatefulSet, bool, error) {
	// look for apps via same name (default)
	// if no found, look for apps via labels => if found, set ownerreference
	// if still no found, create it

	var sts appsv1.StatefulSet
	found := true
	err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &sts)
	if err != nil && !apierrors.IsNotFound(err) {
		return nil, false, err
	} else if err != nil {
		found = false
	}

	// Look for the statefulset via labels instead
	if !found {
		req, err := labels.NewRequirement(SklappTakeoverLabel, selection.Equals, []string{app.Name})
		if err != nil {
			return nil, false, err
		}
		selector := labels.NewSelector().Add(*req)
		var statefulsetlist appsv1.StatefulSetList
		err = r.List(ctx, &statefulsetlist, &client.ListOptions{
			LabelSelector: client.MatchingLabelsSelector{
				Selector: selector,
			},
		})
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, false, err
		}
		if len(statefulsetlist.Items) > 1 {
			return nil, false, fmt.Errorf("found more than 1 statefulset labelled with this app")
		}
		if len(statefulsetlist.Items) == 1 {
			sts = statefulsetlist.Items[0]
			found = true
		}
	}

	// If still no statefulset, create it
	if !found {
		sts = appsv1.StatefulSet{
			ObjectMeta: v1.ObjectMeta{
				Name:            app.Name,
				Namespace:       app.Namespace,
				OwnerReferences: []v1.OwnerReference{ownerReference(app)},
			},
			Spec: appsv1.StatefulSetSpec{
				Replicas: app.Spec.Replicas,
				Selector: &v1.LabelSelector{
					MatchLabels: map[string]string{
						SklappAppLabel:     app.Name,
						SklappAppNameLabel: app.Name,
					},
				},
				Template: podTemplate(app),
			},
		}
		err := r.Create(ctx, &sts)
		if err != nil {
			return nil, false, err
		}
		return &sts, true, nil
	}

	hasOwnerRef := false
	owners := sts.GetOwnerReferences()
	for _, owner := range owners {
		if owner.UID == app.UID {
			hasOwnerRef = true
			break
		}
	}
	if !hasOwnerRef {
		sts.OwnerReferences = append(sts.OwnerReferences, ownerReference(app))
		err := r.Update(ctx, &sts)
		if err != nil {
			return nil, false, err
		}
		return nil, true, nil
	}

	return &sts, false, nil
}

func podTemplate(app *thingsv1.SklApp) corev1.PodTemplateSpec {
	containers := make([]corev1.Container, 1)
	containers[0] = corev1.Container{
		Name:      app.Name,
		Image:     app.Spec.Image,
		Resources: app.Spec.Resources,
		Env:       app.Spec.Env,
		EnvFrom:   app.Spec.EnvFrom,
	}
	if app.Spec.Port != 0 {
		containers[0].Ports = []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: app.Spec.Port,
			},
		}
	}
	return corev1.PodTemplateSpec{
		ObjectMeta: v1.ObjectMeta{
			Labels: map[string]string{
				SklappAppLabel:     app.Name,
				SklappAppNameLabel: app.Name,
			},
		},
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
		Controller: new(true),
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *SklAppReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&thingsv1.SklApp{}).
		Named("sklapp").
		Owns(&appsv1.Deployment{}).
		Owns(&appsv1.StatefulSet{}).
		Owns(&corev1.ServiceAccount{}).
		Owns(&corev1.Service{}).
		Complete(r)
}
