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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	thingsv1 "github.com/sklirg/sklop/api/v1"
)

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
				resource := &thingsv1.SklApp{
					ObjectMeta: metav1.ObjectMeta{
						Name:      resourceName,
						Namespace: resourceNamespace,
					},
					Spec: thingsv1.SklAppSpec{
						Image: "nginx:latest",
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
			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, SklappStatusReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionUnknown))

			deploy := &appsv1.Deployment{}
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(HaveOccurred())

			By("reconciling again to create the Deployment")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(k8sClient.Get(ctx, typeNamespacedName, deploy)).To(Succeed())

			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			readyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionUnknown))

			By("reconciling a third time to become Ready")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: typeNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, typeNamespacedName, updated)).To(Succeed())
			readyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
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

		It("should progress, create a StatefulSet, and become available", func() {
			stsResourceName := "test-resource-sts"
			stsNamespacedName := types.NamespacedName{Name: stsResourceName, Namespace: resourceNamespace}
			applicationType := "StatefulSet"

			resource := &thingsv1.SklApp{
				ObjectMeta: metav1.ObjectMeta{
					Name:      stsResourceName,
					Namespace: resourceNamespace,
				},
				Spec: thingsv1.SklAppSpec{
					Image:           "nginx:latest",
					ApplicationType: &applicationType,
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
			readyCondition := meta.FindStatusCondition(updated.Status.Conditions, SklappStatusReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionUnknown))

			By("reconciling a third time to become Ready")
			_, err = controllerReconciler.Reconcile(ctx, reconcile.Request{
				NamespacedName: stsNamespacedName,
			})
			Expect(err).NotTo(HaveOccurred())

			Expect(k8sClient.Get(ctx, stsNamespacedName, updated)).To(Succeed())
			readyCondition = meta.FindStatusCondition(updated.Status.Conditions, SklappStatusReady)
			Expect(readyCondition).NotTo(BeNil())
			Expect(readyCondition.Status).To(Equal(metav1.ConditionTrue))
		})
	})
})
