// kubernetes_test.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2023-2024 Apple Inc. and the FoundationDB project authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllertest"
)

var _ = Describe("Testing FDB Pod client", func() {
	var enableNodeWatcher bool
	var fakeClient client.WithWatch
	var podClient *kubernetesClient
	var namespace, podName, nodeName string
	var internalCache *informertest.FakeInformers

	BeforeEach(func() {
		scheme := runtime.NewScheme()
		Expect(clientgoscheme.AddToScheme(scheme)).NotTo(HaveOccurred())
		fakeClient = fake.NewClientBuilder().WithScheme(scheme).Build()

		namespace = "fdb-testing"
		podName = "storage-1"
		nodeName = "node1"

		GinkgoT().Setenv("FDB_POD_NAMESPACE", namespace)
		GinkgoT().Setenv("FDB_POD_NAME", podName)
		GinkgoT().Setenv("FDB_NODE_NAME", nodeName)

		internalCache = &informertest.FakeInformers{}
		internalCache.Scheme = fakeClient.Scheme()

		Expect(fakeClient.Create(context.Background(), &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      podName,
				Namespace: namespace,
			},
		})).To(Succeed())

		Expect(fakeClient.Create(context.Background(), &corev1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Name: nodeName,
			},
		})).To(Succeed())
	})

	It("recovers from an API outage without discovery", func() {
		var failedRequest atomic.Bool
		var discoveryRequests atomic.Int32
		patches := make(chan map[string]string, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			writer.Header().Set("Content-Type", "application/json")
			metadata := metav1.PartialObjectMetadata{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1"},
				ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"},
			}
			switch request.URL.Path {
			case "/api/v1/namespaces/" + namespace + "/pods":
				metadata.Kind, metadata.Name, metadata.Namespace = "Pod", podName, namespace
			case "/api/v1/nodes":
				metadata.Kind, metadata.Name = "Node", nodeName
			case "/api/v1/namespaces/" + namespace + "/pods/" + podName:
				if request.Method != http.MethodPatch {
					http.Error(writer, "expected Pod patch", http.StatusMethodNotAllowed)
					return
				}
				pod := &corev1.Pod{}
				if err := json.NewDecoder(request.Body).Decode(pod); err != nil {
					http.Error(writer, err.Error(), http.StatusBadRequest)
					return
				}
				patches <- pod.Annotations
				_ = json.NewEncoder(writer).Encode(pod)
				return
			default:
				discoveryRequests.Add(1)
				http.Error(writer, "API discovery is unavailable", http.StatusServiceUnavailable)
				return
			}
			if failedRequest.CompareAndSwap(false, true) {
				http.Error(writer, "API unavailable", http.StatusServiceUnavailable)
				return
			}
			if strings.Contains(request.Header.Get("Accept"), "as=PartialObjectMetadata") {
				metadata.APIVersion, metadata.Kind = "meta.k8s.io/v1", "PartialObjectMetadata"
			}
			encoder := json.NewEncoder(writer)
			if request.URL.Query().Get("watch") == "true" {
				if request.URL.Query().Get("sendInitialEvents") == "true" {
					_ = encoder.Encode(metav1.WatchEvent{Type: "ADDED", Object: runtime.RawExtension{Object: &metadata}})
					metadata.ObjectMeta = metav1.ObjectMeta{
						ResourceVersion: "1",
						Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
					}
					_ = encoder.Encode(metav1.WatchEvent{Type: "BOOKMARK", Object: runtime.RawExtension{Object: &metadata}})
				}
				writer.(http.Flusher).Flush()
				<-request.Context().Done()
				return
			}
			_ = encoder.Encode(metav1.PartialObjectMetadataList{
				TypeMeta: metav1.TypeMeta{APIVersion: metadata.APIVersion, Kind: metadata.Kind + "List"},
				ListMeta: metav1.ListMeta{ResourceVersion: "1"},
				Items:    []metav1.PartialObjectMetadata{metadata},
			})
		}))
		DeferCleanup(server.Close)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		DeferCleanup(cancel)

		podClient, err := createPodClient(ctx, GinkgoLogr, true, func(namespace, podName, nodeName string) (client.WithWatch, cache.Cache, error) {
			return setupCacheWithConfig(&rest.Config{Host: server.URL}, namespace, podName, nodeName)
		})
		Expect(err).NotTo(HaveOccurred())
		podMetadata, err := podClient.getPodMetadata(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(podMetadata.Name).To(Equal(podName))
		nodeMetadata, err := podClient.getNodeMetadata(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(nodeMetadata.Name).To(Equal(nodeName))
		Expect(failedRequest.Load()).To(BeTrue())
		Expect(podClient.updateAnnotationsOnPod(ctx, map[string]string{"foundationdb.org/test": "ready"})).To(Succeed())
		Expect(patches).To(Receive(HaveKeyWithValue("foundationdb.org/test", "ready")))
		Expect(discoveryRequests.Load()).To(BeZero())
	})

	When("the kubernetesClient was started", func() {
		JustBeforeEach(func() {
			var err error
			podClient, err = createPodClient(context.Background(), GinkgoLogr, enableNodeWatcher, func(fncNamespace string, fncPodName string, fncNodeName string) (client.WithWatch, cache.Cache, error) {
				Expect(fncNamespace).To(Equal(namespace))
				Expect(fncPodName).To(Equal(podName))
				Expect(fncNodeName).To(Equal(nodeName))

				return fakeClient, internalCache, nil
			})

			Expect(err).NotTo(HaveOccurred())
		})

		When("the node watch feature is disabled", func() {
			BeforeEach(func() {
				enableNodeWatcher = false
			})

			It("should have the metadata for the pod but not the node", func() {
				podMetadata, err := podClient.getPodMetadata(context.Background())
				Expect(podMetadata).NotTo(BeNil())
				Expect(err).NotTo(HaveOccurred())
				nodeMetadata, err := podClient.getNodeMetadata(context.Background())
				Expect(nodeMetadata).To(BeNil())
				Expect(err).NotTo(HaveOccurred())
				Expect(internalCache.InformersByGVK).To(HaveLen(1))
			})
		})

		When("the node watch feature is enabled", func() {
			BeforeEach(func() {
				enableNodeWatcher = true
			})

			It("should have the metadata for the pod and node", func() {
				podMetadata, err := podClient.getPodMetadata(context.Background())
				Expect(podMetadata).NotTo(BeNil())
				Expect(err).NotTo(HaveOccurred())
				nodeMetadata, err := podClient.getNodeMetadata(context.Background())
				Expect(nodeMetadata).NotTo(BeNil())
				Expect(err).NotTo(HaveOccurred())
				Expect(internalCache.InformersByGVK).To(HaveLen(2))
			})
		})
	})

	When("the kubernetesClient handles events", func() {
		BeforeEach(func() {
			var err error
			podClient, err = createPodClient(context.Background(), GinkgoLogr, enableNodeWatcher, func(fncNamespace string, fncPodName string, fncNodeName string) (client.WithWatch, cache.Cache, error) {
				Expect(fncNamespace).To(Equal(namespace))
				Expect(fncPodName).To(Equal(podName))
				Expect(fncNodeName).To(Equal(nodeName))

				return fakeClient, internalCache, nil
			})

			Expect(err).NotTo(HaveOccurred())
		})

		When("events for the pod are received", func() {
			var fakeInformer *controllertest.FakeInformer
			var pod *corev1.Pod

			BeforeEach(func() {
				pod = &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      podName,
						Namespace: namespace,
						Labels: map[string]string{
							"testing": "testing",
						},
					},
				}
				var err error
				fakeInformer, err = internalCache.FakeInformerFor(context.Background(), pod)
				Expect(err).NotTo(HaveOccurred())
			})

			When("an UpdateEvent is handled that updates the OutdatedConfigMapAnnotation", func() {
				var timestamp int64

				BeforeEach(func() {
					timestamp = time.Now().Unix()
					pod.Annotations = map[string]string{
						api.OutdatedConfigMapAnnotation: strconv.FormatInt(timestamp, 10),
					}
					fakeInformer.Update(nil, pod)
				})

				It("should update the pod information and receive an event", func() {
					Expect(podClient.TimestampFeed).To(Receive(&timestamp))
				})
			})

			When("an UpdateEvent is handled that updates the OutdatedConfigMapAnnotation with a bad value", func() {
				BeforeEach(func() {
					pod.Annotations = map[string]string{
						api.OutdatedConfigMapAnnotation: "boom!",
					}
					fakeInformer.Update(nil, pod)
				})

				It("should update the Pod information", func() {
					Expect(podClient.TimestampFeed).NotTo(Receive())
				})
			})

			When("an UpdateEvent is handled that updates the IsolateProcessGroupAnnotation where the value is changed", func() {
				BeforeEach(func() {
					oldPod := pod.DeepCopy()
					pod.Annotations = map[string]string{
						api.IsolateProcessGroupAnnotation: "true",
					}

					fakeInformer.Update(oldPod, pod)
				})

				It("should update the Pod information", func() {
					Expect(podClient.TimestampFeed).To(Receive())
				})
			})

			When("an UpdateEvent is handled that updates the IsolateProcessGroupAnnotation where the value is not changed", func() {
				BeforeEach(func() {
					pod.Annotations = map[string]string{
						api.IsolateProcessGroupAnnotation: "true",
					}

					fakeInformer.Update(pod.DeepCopy(), pod)
				})

				It("should update the Pod information", func() {
					Expect(podClient.TimestampFeed).NotTo(Receive())
				})
			})
		})
	})

	When("the kubernetesClient should update the annotations", func() {
		var mon *monitor

		JustBeforeEach(func() {
			var err error
			podClient, err = createPodClient(context.Background(), GinkgoLogr, enableNodeWatcher, func(fncNamespace string, fncPodName string, fncNodeName string) (client.WithWatch, cache.Cache, error) {
				Expect(fncNamespace).To(Equal(namespace))
				Expect(fncPodName).To(Equal(podName))
				Expect(fncNodeName).To(Equal(nodeName))

				return fakeClient, internalCache, nil
			})

			Expect(err).NotTo(HaveOccurred())
			// Execute the update annotations.
			Expect(podClient.updateAnnotations(context.Background(), mon)).NotTo(HaveOccurred())
		})

		When("no additional env variables are set", func() {
			BeforeEach(func() {
				mon = &monitor{
					activeConfiguration: &api.ProcessConfiguration{
						BinaryPath: "/usr/bin",
					},
				}
			})

			It("should update the annotations", func() {
				pod := &corev1.Pod{}
				Expect(fakeClient.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: podName}, pod)).NotTo(HaveOccurred())

				Expect(pod.Annotations).To(HaveKeyWithValue(api.CurrentConfigurationAnnotation, ""))
				Expect(pod.Annotations).To(HaveKeyWithValue(api.EnvironmentAnnotation, "{\"BINARY_DIR\":\"/usr\"}"))
			})
		})

		When("one flat additional env variable is set", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("TEST", "test-value")
				mon = &monitor{
					activeConfiguration: &api.ProcessConfiguration{
						BinaryPath: "/usr/bin",
						Arguments: []api.Argument{
							{
								ArgumentType: api.EnvironmentAnnotation,
								Source:       "TEST",
							},
						},
					},
				}
			})

			It("should update the annotations", func() {
				pod := &corev1.Pod{}
				Expect(fakeClient.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: podName}, pod)).NotTo(HaveOccurred())

				Expect(pod.Annotations).To(HaveKeyWithValue(api.CurrentConfigurationAnnotation, ""))
				Expect(pod.Annotations).To(HaveKeyWithValue(api.EnvironmentAnnotation, "{\"BINARY_DIR\":\"/usr\",\"TEST\":\"test-value\"}"))
			})
		})

		When("one nested flat additional env variable is set", func() {
			BeforeEach(func() {
				GinkgoT().Setenv("TEST", "test-value")
				mon = &monitor{
					activeConfiguration: &api.ProcessConfiguration{
						BinaryPath: "/usr/bin",
						Arguments: []api.Argument{
							{
								ArgumentType: api.ConcatenateArgumentType,
								Values: []api.Argument{
									{
										ArgumentType: api.EnvironmentArgumentType,
										Source:       "TEST",
									},
								},
							},
						},
					},
				}
			})

			It("should update the annotations", func() {
				pod := &corev1.Pod{}
				Expect(fakeClient.Get(context.Background(), client.ObjectKey{Namespace: namespace, Name: podName}, pod)).NotTo(HaveOccurred())

				Expect(pod.Annotations).To(HaveKeyWithValue(api.CurrentConfigurationAnnotation, ""))
				Expect(pod.Annotations).To(HaveKeyWithValue(api.EnvironmentAnnotation, "{\"BINARY_DIR\":\"/usr\",\"TEST\":\"test-value\"}"))
			})
		})
	})
})
