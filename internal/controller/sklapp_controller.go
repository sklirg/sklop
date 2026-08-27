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
	"k8s.io/apimachinery/pkg/api/equality"
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

	// SklappStatusReady is the single canonical condition type reflecting
	// the outcome of the most recent reconcile pass.
	SklappStatusReady string = "Ready"

	SklappStatusReconciling         string = "Reconciling"
	SklappStatusReconciliationError string = "ReconciliationError"
	SklappStatusReconciled          string = "Reconciled"
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
	ctx = logf.IntoContext(ctx, logger)

	logger.Info("reconciling")

	ready := false

	// Update the Ready condition and persist status when exiting, so it
	// always reflects the outcome of this reconcile pass - regardless of
	// which return path below was taken - rather than a per-branch
	// condition type that may not get updated on every pass.
	defer func() {
		condition := v1.Condition{
			Type:    SklappStatusReady,
			Status:  v1.ConditionUnknown,
			Reason:  SklappStatusReconciling,
			Message: "Reconciling",
		}
		switch {
		case reterr != nil:
			condition.Status = v1.ConditionFalse
			condition.Reason = SklappStatusReconciliationError
			condition.Message = reterr.Error()
		case ready:
			condition.Status = v1.ConditionTrue
			condition.Reason = SklappStatusReconciled
			condition.Message = "Reconciled"
		}
		meta.SetStatusCondition(&app.Status.Conditions, condition)

		logger.Info("updating sklapp status", "readyCondition", condition.Status, "reason", condition.Reason, "ready", app.Status.Ready)
		if statusErr := r.Status().Update(ctx, &app); statusErr != nil {
			// A non-nil error must never be paired with a non-empty Result:
			// controller-runtime ignores Result whenever error is set and
			// requeues with backoff regardless, so keep the return value
			// itself unambiguous rather than carrying a stale RequeueAfter.
			reterr = errors.Join(reterr, statusErr)
			result = ctrl.Result{}
		}
	}()

	// Ensure SA exists
	var serviceAccount corev1.ServiceAccount
	err = r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &serviceAccount)
	if err != nil && apierrors.IsNotFound(err) {
		logger.Info("serviceaccount not found, creating it")
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
			logger.Error(err, "failed to create serviceaccount")
			return ctrl.Result{}, err
		}
		logger.Info("created serviceaccount, ending this reconcile pass")
		return ctrl.Result{}, nil
	} else if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to get serviceaccount")
		return ctrl.Result{}, fmt.Errorf("failed to get ServiceAccount: %w", err)
	} else if !hasOwnerReference(&serviceAccount, &app) {
		logger.Info("serviceaccount missing owner reference, backfilling it and requeueing")
		serviceAccount.OwnerReferences = append(serviceAccount.OwnerReferences, ownerReference(&app))
		if err := r.Update(ctx, &serviceAccount); err != nil {
			logger.Error(err, "failed to update serviceaccount owner reference")
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	logger.Info("reconciling application type", "applicationType", *app.Spec.ApplicationType)
	updated, readyStatus, err := r.reconcileApplicationType(ctx, &app)
	if err != nil {
		logger.Error(err, "failed to reconcile application type", "applicationType", *app.Spec.ApplicationType)
		return ctrl.Result{}, fmt.Errorf("failed to reconcile application type %q: %w", *app.Spec.ApplicationType, err)
	}
	if readyStatus != "" {
		app.Status.Ready = readyStatus
	}
	if updated {
		logger.Info("application type reconciliation made changes, requeueing", "applicationType", *app.Spec.ApplicationType)
		return ctrl.Result{RequeueAfter: 1 * time.Second}, nil
	}

	logger.Info("reconcile complete, nothing left to do", "ready", readyStatus)
	ready = true

	return ctrl.Result{}, nil
}

func (r *SklAppReconciler) reconcileApplicationType(ctx context.Context, app *thingsv1.SklApp) (bool, string, error) {
	if app.Spec.ApplicationType == nil {
		return false, "", fmt.Errorf("app \"%s\" does not have application type", app.Name)
	}

	switch *app.Spec.ApplicationType {
	case "Deployment":
		deploy, updated, err := r.deployment(ctx, app)
		if err != nil {
			return false, "", err
		}
		return updated, deploymentReadyStatus(deploy), nil
	case "StatefulSet":
		sts, updated, err := r.statefulset(ctx, app)
		if err != nil {
			return false, "", err
		}
		return updated, statefulSetReadyStatus(sts), nil

	default:
		return false, "", fmt.Errorf("application type \"%s\" not valid", *app.Spec.ApplicationType)
	}
}

// deploymentReadyStatus formats a Deployment's readiness as "readyReplicas/replicas".
// deploy may be nil (e.g. right after an owner-reference backfill), in which case an
// empty string is returned so the previous value is left as the caller's default.
func deploymentReadyStatus(deploy *appsv1.Deployment) string {
	if deploy == nil {
		return ""
	}
	desired := int32(1)
	if deploy.Spec.Replicas != nil {
		desired = *deploy.Spec.Replicas
	}
	return fmt.Sprintf("%d/%d", deploy.Status.ReadyReplicas, desired)
}

// statefulSetReadyStatus formats a StatefulSet's readiness as "readyReplicas/replicas".
func statefulSetReadyStatus(sts *appsv1.StatefulSet) string {
	if sts == nil {
		return ""
	}
	desired := int32(1)
	if sts.Spec.Replicas != nil {
		desired = *sts.Spec.Replicas
	}
	return fmt.Sprintf("%d/%d", sts.Status.ReadyReplicas, desired)
}

func (r *SklAppReconciler) deployment(ctx context.Context, app *thingsv1.SklApp) (*appsv1.Deployment, bool, error) {
	// look for apps via same name (default)
	// if no found, look for apps via labels => if found, set ownerreference
	// if still no found, create it

	logger := logf.FromContext(ctx)

	var deploy appsv1.Deployment
	found := true
	err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &deploy)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to get deployment")
		return nil, false, err
	} else if err != nil {
		logger.Info("deployment not found by name, looking for a takeover candidate by label")
		found = false
	} else {
		logger.Info("found deployment by name")
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
			logger.Error(err, "failed to list deployments by takeover label")
			return nil, false, err
		}
		if len(deploymentlist.Items) > 1 {
			return nil, false, fmt.Errorf("found more than 1 deployment labelled with this app")
		}
		if len(deploymentlist.Items) == 1 {
			logger.Info("found deployment by takeover label", "deployment", deploymentlist.Items[0].Name)
			deploy = deploymentlist.Items[0]
			found = true
		}
	}

	// If still no deployment, create it
	if !found {
		logger.Info("no deployment found, creating it")
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
			logger.Error(err, "failed to create deployment")
			return nil, false, err
		}
		logger.Info("created deployment, requeueing")
		return &deploy, true, nil
	}

	if !hasOwnerReference(&deploy, app) {
		logger.Info("deployment missing owner reference, backfilling it and requeueing")
		deploy.OwnerReferences = append(deploy.OwnerReferences, ownerReference(app))
		err := r.Update(ctx, &deploy)
		if err != nil {
			logger.Error(err, "failed to update deployment owner reference")
			return nil, false, err
		}
		return nil, true, nil
	}

	// Reconcile drift: bring the replica count and pod template back in
	// line with the SklApp spec if anything has changed them.
	// Comparing only the projected, SklApp-managed fields (rather than the whole
	// Template) avoids false positives from fields the API server defaults
	// on write, such as Container.ImagePullPolicy.
	desiredTemplate := podTemplate(app)
	replicasDrifted := !equality.Semantic.DeepEqual(deploy.Spec.Replicas, app.Spec.Replicas)
	templateDrifted := !equality.Semantic.DeepEqual(projectPodTemplate(deploy.Spec.Template), projectPodTemplate(desiredTemplate))
	if replicasDrifted || templateDrifted {
		logger.Info("deployment spec drifted from SklApp spec, correcting it and requeueing",
			"replicasDrifted", replicasDrifted, "templateDrifted", templateDrifted)
		deploy.Spec.Replicas = app.Spec.Replicas
		deploy.Spec.Template = desiredTemplate
		if err := r.Update(ctx, &deploy); err != nil {
			logger.Error(err, "failed to correct deployment drift")
			return nil, false, err
		}
		return &deploy, true, nil
	}

	logger.Info("deployment already matches SklApp spec, nothing to do")
	return &deploy, false, nil
}

func (r *SklAppReconciler) statefulset(ctx context.Context, app *thingsv1.SklApp) (*appsv1.StatefulSet, bool, error) {
	// look for apps via same name (default)
	// if no found, look for apps via labels => if found, set ownerreference
	// if still no found, create it

	logger := logf.FromContext(ctx)

	var sts appsv1.StatefulSet
	found := true
	err := r.Get(ctx, types.NamespacedName{Namespace: app.Namespace, Name: app.Name}, &sts)
	if err != nil && !apierrors.IsNotFound(err) {
		logger.Error(err, "failed to get statefulset")
		return nil, false, err
	} else if err != nil {
		logger.Info("statefulset not found by name, looking for a takeover candidate by label")
		found = false
	} else {
		logger.Info("found statefulset by name")
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
			logger.Error(err, "failed to list statefulsets by takeover label")
			return nil, false, err
		}
		if len(statefulsetlist.Items) > 1 {
			return nil, false, fmt.Errorf("found more than 1 statefulset labelled with this app")
		}
		if len(statefulsetlist.Items) == 1 {
			logger.Info("found statefulset by takeover label", "statefulset", statefulsetlist.Items[0].Name)
			sts = statefulsetlist.Items[0]
			found = true
		}
	}

	// If still no statefulset, create it
	if !found {
		logger.Info("no statefulset found, creating it")
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
			logger.Error(err, "failed to create statefulset")
			return nil, false, err
		}
		logger.Info("created statefulset, requeueing")
		return &sts, true, nil
	}

	if !hasOwnerReference(&sts, app) {
		logger.Info("statefulset missing owner reference, backfilling it and requeueing")
		sts.OwnerReferences = append(sts.OwnerReferences, ownerReference(app))
		err := r.Update(ctx, &sts)
		if err != nil {
			logger.Error(err, "failed to update statefulset owner reference")
			return nil, false, err
		}
		return nil, true, nil
	}

	// Reconcile drift: bring the replica count and pod template back in
	// line with the SklApp spec if anything has changed them.
	// Comparing only the projected, SklApp-managed fields (rather than the whole
	// Template) avoids false positives from fields the API server defaults
	// on write, such as Container.ImagePullPolicy.
	desiredTemplate := podTemplate(app)
	replicasDrifted := !equality.Semantic.DeepEqual(sts.Spec.Replicas, app.Spec.Replicas)
	templateDrifted := !equality.Semantic.DeepEqual(projectPodTemplate(sts.Spec.Template), projectPodTemplate(desiredTemplate))
	if replicasDrifted || templateDrifted {
		logger.Info("statefulset spec drifted from SklApp spec, correcting it and requeueing",
			"replicasDrifted", replicasDrifted, "templateDrifted", templateDrifted)
		sts.Spec.Replicas = app.Spec.Replicas
		sts.Spec.Template = desiredTemplate
		if err := r.Update(ctx, &sts); err != nil {
			logger.Error(err, "failed to correct statefulset drift")
			return nil, false, err
		}
		return &sts, true, nil
	}

	logger.Info("statefulset already matches SklApp spec, nothing to do")
	return &sts, false, nil
}

func podTemplate(app *thingsv1.SklApp) corev1.PodTemplateSpec {
	containers := make([]corev1.Container, 1)
	containers[0] = corev1.Container{
		Name:         app.Name,
		Image:        app.Spec.Image,
		Resources:    app.Spec.Resources,
		Env:          app.Spec.Env,
		EnvFrom:      app.Spec.EnvFrom,
		VolumeMounts: app.Spec.VolumeMounts,
		// Adhere to the "restricted" Pod Security Standard.
		// https://kubernetes.io/docs/concepts/security/pod-security-standards/#restricted
		SecurityContext: &corev1.SecurityContext{
			ReadOnlyRootFilesystem:   new(true),
			AllowPrivilegeEscalation: new(false),
			Capabilities: &corev1.Capabilities{
				Drop: []corev1.Capability{"ALL"},
			},
		},
	}
	if app.Spec.Port != 0 {
		containers[0].Ports = []corev1.ContainerPort{
			{
				Name:          "http",
				ContainerPort: app.Spec.Port,
				Protocol:      corev1.ProtocolTCP,
			},
		}
	}
	// RunAsNonRoot is always enforced: images with their own non-root
	// default user (a Dockerfile USER directive, etc.) run fine as-is.
	// Only an image whose default user is root (uid 0) will fail to start
	// under this policy ("container has runAsNonRoot and image will run
	// as root") - RunAsUser is the escape hatch for that case, overriding
	// the user to a non-root UID the image supports.
	podSecurityContext := &corev1.PodSecurityContext{
		RunAsNonRoot: new(true),
		RunAsUser:    app.Spec.RunAsUser,
		SeccompProfile: &corev1.SeccompProfile{
			Type: corev1.SeccompProfileTypeRuntimeDefault,
		},
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
			Volumes:            app.Spec.Volumes,
			SecurityContext:    podSecurityContext,
		},
	}
}

// managedPodSpec is the subset of a PodTemplateSpec that SklApp actually
// manages. Comparing this projection (rather than the whole PodTemplateSpec)
// between the live and desired templates avoids false-positive drift from
// fields the API server defaults on write - e.g. Container.TerminationMessagePolicy, PodSpec.DNSPolicy.
type managedPodSpec struct {
	Labels             map[string]string
	ServiceAccountName string
	Volumes            []corev1.Volume
	Image              string
	Resources          corev1.ResourceRequirements
	Env                []corev1.EnvVar
	EnvFrom            []corev1.EnvFromSource
	VolumeMounts       []corev1.VolumeMount
	Ports              []corev1.ContainerPort
	PodSecurityContext *corev1.PodSecurityContext
	SecurityContext    *corev1.SecurityContext
}

func projectPodTemplate(tmpl corev1.PodTemplateSpec) managedPodSpec {
	var container corev1.Container
	if len(tmpl.Spec.Containers) > 0 {
		container = tmpl.Spec.Containers[0]
	}
	return managedPodSpec{
		Labels:             tmpl.Labels,
		ServiceAccountName: tmpl.Spec.ServiceAccountName,
		Volumes:            tmpl.Spec.Volumes,
		Image:              container.Image,
		Resources:          container.Resources,
		Env:                container.Env,
		EnvFrom:            container.EnvFrom,
		VolumeMounts:       container.VolumeMounts,
		Ports:              container.Ports,
		PodSecurityContext: tmpl.Spec.SecurityContext,
		SecurityContext:    container.SecurityContext,
	}
}

func hasOwnerReference(obj v1.Object, app *thingsv1.SklApp) bool {
	for _, owner := range obj.GetOwnerReferences() {
		if owner.UID == app.UID {
			return true
		}
	}
	return false
}

func ownerReference(app *thingsv1.SklApp) v1.OwnerReference {
	return v1.OwnerReference{
		APIVersion: thingsv1.GroupVersion.String(),
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
