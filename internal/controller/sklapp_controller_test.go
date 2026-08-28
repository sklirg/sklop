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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	gatewayv1 "sigs.k8s.io/gateway-api/apis/v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	thingsv1 "github.com/sklirg/sklop/api/v1"
)

const nginxImage = "nginx:latest"

// probeCommand is a trivially-successful exec probe command, reused across
// the startup/readiness/liveness probe test cases below.
var probeCommand = []string{"true"}

// nginxWritableVolumes backs the directories the stock nginx image needs to
// write to with emptyDir volumes, to satisfy the pod's ReadOnlyRootFilesystem.
func nginxWritableVolumes() ([]corev1.Volume, []corev1.VolumeMount) {
	dirs := []struct{ name, path string }{
		{"cache", "/var/cache/nginx"},
		{"run", "/var/run"},
		{"tmp", "/tmp"},
	}
	volumes := make([]corev1.Volume, len(dirs))
	mounts := make([]corev1.VolumeMount, len(dirs))
	for i, dir := range dirs {
		volumes[i] = corev1.Volume{Name: dir.name, VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}}
		mounts[i] = corev1.VolumeMount{Name: dir.name, MountPath: dir.path}
	}
	return volumes, mounts
}

var _ = Describe("SklApp Controller", func() {
	Context("When reconciling a resource", func() {
		const (
			resourceName      = "test-resource"
			resourceNamespace = "default"
		)

		ctx := context.Background()

		typeNamespacedName := types.NamespacedName{
			Name:      resourceName,
			Namespace: resourceNamespace,
		}
		sklapp := &thingsv1.SklApp{}

		BeforeEach(func() {
			By("creating the custom resource for the Kind SklApp")
			err := k8sClient.Get(ctx, typeNamespacedName, sklapp)
			if err != nil && errors.IsNotFound(err) {
				volumes, volumeMounts := nginxWritableVolumes()
				resource := &thingsv1.SklApp{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: thingsv1.SklAppSpec{
						Image:        nginxImage,
						RunAsUser:    new(int64(1000)),
						Volumes:      volumes,
						VolumeMounts: volumeMounts,
					},
				}
				Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			}
		})

		AfterEach(func() {
			// TODO(user): Cleanup logic after each test, like removing the resource instance.
			resource := &thingsv1.SklApp{}
			err := k8sClient.Get(ctx, typeNamespacedName, resource)
			Expect(err).NotTo(HaveOccurred())

			By("Cleanup the specific resource instance SklApp")
			Expect(k8sClient.Delete(ctx, resource)).To(Succeed())

			// envtest has no garbage-collector controller, so owned
			// resources aren't reaped via OwnerReferences - delete them
			// explicitly to keep specs isolated.
			By("Cleanup owned resources")
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.ServiceAccount{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			}))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &appsv1.Deployment{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName, Namespace: resourceNamespace},
			}))).To(Succeed())
		})
		It("should successfully reconcile the resource", func() {
			By("Reconciling the created resource")
			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// TODO(user): Add more specific assertions depending on your controller's reconciliation logic.
			// Example: If you expect a certain status condition after reconciliation, verify it here.
		})

		It("should progress, create a Deployment, and become available", func() {
			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling for the first time to create the ServiceAccount")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			updated := &thingsv1.SklApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			healthyCondition := meta.FindStatusCondition(updated.Status.Conditions, SklappStatusHealthy)
			Expect(healthyCondition).NotTo(BeNil())
			Expect(healthyCondition.Status).To(Equal(metav1.ConditionUnknown))

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(HaveOccurred())

			By("reconciling again to create the Deployment")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())

			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			healthyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusHealthy)
			Expect(healthyCondition).NotTo(BeNil())
			Expect(healthyCondition.Status).To(Equal(metav1.ConditionUnknown))

			By("reconciling a third time to become Ready")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			healthyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusHealthy)
			Expect(healthyCondition).NotTo(BeNil())
			Expect(healthyCondition.Status).To(Equal(metav1.ConditionTrue))
		})

		It("should propagate startup, readiness, and liveness probes to the Deployment's container", func() {
			app := &thingsv1.SklApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())

			app.Spec.StartupProbe = &corev1.Probe{
				ProbeHandler:     corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}},
				FailureThreshold: 30,
				PeriodSeconds:    1,
			}
			app.Spec.ReadinessProbe = &corev1.Probe{
				ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}},
				PeriodSeconds: 5,
			}
			app.Spec.LivenessProbe = &corev1.Probe{
				ProbeHandler:  corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: probeCommand}},
				PeriodSeconds: 10,
			}
			Expect(k8sClient.Update(ctx, app)).To(Succeed())
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())

			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling until the Deployment is created")
			for range 2 {
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())
			container := deploy.Spec.Template.Spec.Containers[0]
			// Compare handler/threshold fields individually rather than the
			// whole struct: the API server defaults TimeoutSeconds and
			// SuccessThreshold on the built-in Deployment it creates, but
			// the CRD's own spec (fetched above) never receives that
			// defaulting, so a deep Equal would spuriously fail.
			Expect(container.StartupProbe.Exec).To(Equal(app.Spec.StartupProbe.Exec))
			Expect(container.StartupProbe.FailureThreshold).To(Equal(app.Spec.StartupProbe.FailureThreshold))
			Expect(container.StartupProbe.PeriodSeconds).To(Equal(app.Spec.StartupProbe.PeriodSeconds))
			Expect(container.ReadinessProbe.Exec).To(Equal(app.Spec.ReadinessProbe.Exec))
			Expect(container.ReadinessProbe.PeriodSeconds).To(Equal(app.Spec.ReadinessProbe.PeriodSeconds))
			Expect(container.LivenessProbe.Exec).To(Equal(app.Spec.LivenessProbe.Exec))
			Expect(container.LivenessProbe.PeriodSeconds).To(Equal(app.Spec.LivenessProbe.PeriodSeconds))
		})

		It("should reconcile manual drift on the Deployment back to the SklApp spec", func() {
			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling until the Deployment is created")
			for range 2 {
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: typeNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			app := &thingsv1.SklApp{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, app)).To(Succeed())

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())

			By("manually drifting the Deployment away from the SklApp spec")
			driftedReplicas := int32(5)
			deploy.Spec.Replicas = &driftedReplicas
			deploy.Spec.Template.Spec.Containers[0].Image = "busybox:latest"
			Expect(k8sClient.Update(ctx, deploy)).To(Succeed())

			By("reconciling to correct the drift")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())
			Expect(deploy.Spec.Replicas).To(Equal(app.Spec.Replicas))
			Expect(deploy.Spec.Template.Spec.Containers[0].Image).To(Equal(app.Spec.Image))
		})

		It("should not perpetually redrift a projected volume with no defaultMode set", func() {
			// Regression test: a projected volume (e.g. a ConfigMap source)
			// left without an explicit defaultMode gets DefaultMode=0644
			// defaulted by the API server on write. If the desired template
			// doesn't account for that, every reconcile sees "drift" against
			// the live, already-defaulted object and re-applies the same
			// no-op update forever.
			projResourceName := "test-resource-projvol"
			projNamespacedName := types.NamespacedName{Name: projResourceName, Namespace: resourceNamespace}

			resource := &thingsv1.SklApp{
				ObjectMeta: metav1.ObjectMeta{
					Name:      projResourceName,
					Namespace: resourceNamespace,
				},
				Spec: thingsv1.SklAppSpec{
					Image: nginxImage,
					Volumes: []corev1.Volume{
						{
							Name: "config",
							VolumeSource: corev1.VolumeSource{
								Projected: &corev1.ProjectedVolumeSource{
									Sources: []corev1.VolumeProjection{
										{ConfigMap: &corev1.ConfigMapProjection{
											LocalObjectReference: corev1.LocalObjectReference{Name: "does-not-need-to-exist"},
										}},
									},
								},
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{Name: projResourceName, Namespace: resourceNamespace},
				}))).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: projResourceName, Namespace: resourceNamespace},
				}))).To(Succeed())
			})

			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling until the Deployment is created and settled")
			for range 3 {
				_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
					NamespacedName: projNamespacedName,
				})
				Expect(err).NotTo(HaveOccurred())
			}

			By("reconciling once more and confirming no further drift is detected")
			result, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: projNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			// A RequeueAfter here means the drift check found (and "fixed")
			// a difference again - the perpetual-loop regression - since by
			// this point the Deployment should already match the SklApp spec.
			Expect(result.RequeueAfter).To(BeZero())
		})

		It("should progress, create a StatefulSet, and become available", func() {
			stsResourceName := "test-resource-sts"
			stsNamespacedName := types.NamespacedName{Name: stsResourceName, Namespace: resourceNamespace}
			applicationType := "StatefulSet"

			stsVolumes, stsVolumeMounts := nginxWritableVolumes()
			resource := &thingsv1.SklApp{
				ObjectMeta: metav1.ObjectMeta{
					Name:      stsResourceName,
					Namespace: resourceNamespace,
				},
				Spec: thingsv1.SklAppSpec{
					Image:           nginxImage,
					ApplicationType: &applicationType,
					RunAsUser:       new(int64(1000)),
					Volumes:         stsVolumes,
					VolumeMounts:    stsVolumeMounts,
				},
			}
			Expect(k8sClient.Create(ctx, resource)).To(Succeed())
			DeferCleanup(func() {
				Expect(k8sClient.Delete(ctx, resource)).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &corev1.ServiceAccount{
					ObjectMeta: metav1.ObjectMeta{Name: stsResourceName, Namespace: resourceNamespace},
				}))).To(Succeed())
				Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, &appsv1.StatefulSet{
					ObjectMeta: metav1.ObjectMeta{Name: stsResourceName, Namespace: resourceNamespace},
				}))).To(Succeed())
			})

			controllerReconciler := &SklAppReconciler{
				Client: k8sClient,
				Scheme: k8sClient.Scheme(),
			}

			By("reconciling for the first time to create the ServiceAccount")
			_, err := controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: stsNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			sts := &appsv1.StatefulSet{}
			Expect(k8sClient.Get(ctx, stsNamespacedName, sts)).To(HaveOccurred())

			By("reconciling again to create the StatefulSet")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: stsNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, stsNamespacedName, sts)).To(Succeed())

			updated := &thingsv1.SklApp{}
			Expect(k8sClient.Get(ctx, stsNamespacedName, updated)).To(Succeed())
			healthyCondition := meta.FindStatusCondition(updated.Status.Conditions, SklappStatusHealthy)
			Expect(healthyCondition).NotTo(BeNil())
			Expect(healthyCondition.Status).To(Equal(metav1.ConditionUnknown))

			By("reconciling a third time to become Ready")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: stsNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, stsNamespacedName, updated)).To(Succeed())
			healthyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusHealthy)
			Expect(healthyCondition).NotTo(BeNil())
			Expect(healthyCondition.Status).To(Equal(metav1.ConditionTrue))
		})
	})
})

var _ = Describe("gatewayParentRef", func() {
	const defaultGatewayName = "default-gw"
	const defaultGatewayNamespace = "gw-ns"

	app := func(gateway *thingsv1.GatewayReference) *thingsv1.SklApp {
		return &thingsv1.SklApp{
			ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "app-ns"},
			Spec:       thingsv1.SklAppSpec{Gateway: gateway},
		}
	}
	reconcilerWithDefault := func() *SklAppReconciler {
		return &SklAppReconciler{DefaultGatewayName: defaultGatewayName, DefaultGatewayNamespace: defaultGatewayNamespace}
	}

	It("errors when neither spec.gateway nor a controller default is configured", func() {
		r := &SklAppReconciler{}
		_, err := r.gatewayParentRef(app(nil))
		Expect(err).To(HaveOccurred())
	})

	It("falls back to the controller-wide default when spec.gateway is unset", func() {
		ref, err := reconcilerWithDefault().gatewayParentRef(app(nil))
		Expect(err).NotTo(HaveOccurred())
		Expect(ref.Name).To(Equal(gatewayv1.ObjectName(defaultGatewayName)))
		Expect(ref.Namespace).NotTo(BeNil())
		Expect(*ref.Namespace).To(Equal(gatewayv1.Namespace(defaultGatewayNamespace)))
	})

	It("prefers spec.gateway over the controller-wide default", func() {
		ns := "own-ns"
		ref, err := reconcilerWithDefault().gatewayParentRef(app(&thingsv1.GatewayReference{Name: "own-gw", Namespace: &ns}))
		Expect(err).NotTo(HaveOccurred())
		Expect(ref.Name).To(Equal(gatewayv1.ObjectName("own-gw")))
		Expect(ref.Namespace).NotTo(BeNil())
		Expect(*ref.Namespace).To(Equal(gatewayv1.Namespace("own-ns")))
	})

	It("omits Namespace when spec.gateway sets a name but no namespace, meaning same-namespace", func() {
		ref, err := reconcilerWithDefault().gatewayParentRef(app(&thingsv1.GatewayReference{Name: "own-gw"}))
		Expect(err).NotTo(HaveOccurred())
		Expect(ref.Name).To(Equal(gatewayv1.ObjectName("own-gw")))
		Expect(ref.Namespace).To(BeNil())
	})
})

var _ = Describe("oauth2ProxySecretName", func() {
	It("defaults to <name>-oauth2-proxy-config when unset", func() {
		app := &thingsv1.SklApp{ObjectMeta: metav1.ObjectMeta{Name: "grafana"}}
		Expect(oauth2ProxySecretName(app)).To(Equal("grafana-oauth2-proxy-config"))
	})

	It("uses OAuth2ProxySecretName when set", func() {
		secretName := "shared-oauth2-proxy-secret"
		app := &thingsv1.SklApp{
			ObjectMeta: metav1.ObjectMeta{Name: "grafana"},
			Spec:       thingsv1.SklAppSpec{OAuth2ProxySecretName: &secretName},
		}
		Expect(oauth2ProxySecretName(app)).To(Equal(secretName))
	})
})

var _ = Describe("oauth2ProxyImage", func() {
	It("defaults to DefaultOauth2ProxyImage when unset", func() {
		r := &SklAppReconciler{}
		Expect(r.oauth2ProxyImage()).To(Equal(DefaultOauth2ProxyImage))
	})

	It("uses the reconciler's configured Oauth2ProxyImage when set", func() {
		r := &SklAppReconciler{Oauth2ProxyImage: "example.com/oauth2-proxy:custom"}
		Expect(r.oauth2ProxyImage()).To(Equal("example.com/oauth2-proxy:custom"))
	})
})
