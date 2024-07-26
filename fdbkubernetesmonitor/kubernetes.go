// kubernetes.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2023 Apple Inc. and the FoundationDB project authors
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
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	"k8s.io/client-go/util/retry"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// kubernetesClient is a wrapper around the Kubernetes API.
type kubernetesClient struct {
	// podMetadata is the latest metadata that was seen by the fdb-kubernetes-monitor for the according Pod.
	podMetadata *metav1.PartialObjectMetadata

	// nodeMetadata is the latest metadata that was seen by the fdb-kubernetes-monitor for the according node that hosts the Pod.
	nodeMetadata *metav1.PartialObjectMetadata

	// TimestampFeed is a channel where the kubernetes client will send updates with
	// the values from api.OutdatedConfigMapAnnotation. If the api.IsolateProcessGroupAnnotation is changed the current
	// timestamp will be sent to the channel to force the monitor to change its configuration accordingly.
	TimestampFeed chan int64

	// Logger is the logger we use for this client.
	Logger logr.Logger

	// Adds the controller runtime client to the kubernetesClient.
	client.Client
}

func setupCache(namespace string, podName string, nodeName string) (client.WithWatch, cache.Cache, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, nil, err
	}

	scheme := runtime.NewScheme()
	err = clientgoscheme.AddToScheme(scheme)
	if err != nil {
		return nil, nil, err
	}

	// Create the new client for writes. This client will also be used to setup the cache.
	internalClient, err := client.NewWithWatch(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, nil, err
	}

	internalCache, err := cache.New(config, cache.Options{
		Scheme:    scheme,
		Mapper:    internalClient.RESTMapper(),
		Namespace: namespace,
		SelectorsByObject: map[client.Object]cache.ObjectSelector{
			&corev1.Pod{}: {
				Field: fields.OneTermEqualSelector(metav1.ObjectNameField, podName),
			},
			&corev1.Node{}: {
				Field: fields.OneTermEqualSelector(metav1.ObjectNameField, nodeName),
			},
		},
	})
	if err != nil {
		return nil, nil, err
	}

	return internalClient, internalCache, nil
}

// createPodClient creates a new client for working with the pod object.
func createPodClient(ctx context.Context, logger logr.Logger, enableNodeWatcher bool, setupCache func(string, string, string) (client.WithWatch, cache.Cache, error)) (*kubernetesClient, error) {
	namespace := os.Getenv("FDB_POD_NAMESPACE")
	podName := os.Getenv("FDB_POD_NAME")
	nodeName := os.Getenv("FDB_NODE_NAME")

	internalClient, internalCache, err := setupCache(namespace, podName, nodeName)
	if err != nil {
		return nil, err
	}

	podClient := &kubernetesClient{
		podMetadata:   nil,
		nodeMetadata:  nil,
		TimestampFeed: make(chan int64, 10),
		Logger:        logger,
	}

	// Fetch the informer for the Pod resource.
	podInformer, err := internalCache.GetInformer(ctx, &corev1.Pod{})
	if err != nil {
		return nil, err
	}

	// Setup an event handler to make sure we get events for the Pod and directly reload the information.
	_, err = podInformer.AddEventHandler(podClient)
	if err != nil {
		return nil, err
	}

	if enableNodeWatcher {
		var nodeInformer cache.Informer
		// Fetch the informer for the node resource.
		nodeInformer, err = internalCache.GetInformer(ctx, &corev1.Node{})
		if err != nil {
			return nil, err
		}

		// Setup an event handler to make sure we get events for the node and directly reload the information.
		_, err = nodeInformer.AddEventHandler(podClient)
		if err != nil {
			return nil, err
		}
	}

	// Make sure the internal cache is started.
	go func() {
		_ = internalCache.Start(ctx)
	}()

	// This should be fairly quick as no informers are provided by default.
	internalCache.WaitForCacheSync(ctx)
	controllerClient, err := client.NewDelegatingClient(client.NewDelegatingClientInput{
		CacheReader:       internalCache,
		Client:            internalClient,
		UncachedObjects:   nil,
		CacheUnstructured: false,
	})

	if err != nil {
		return nil, err
	}

	podClient.Client = controllerClient

	// Fetch the current metadata before returning the kubernetesClient
	currentPodMetadata := &metav1.PartialObjectMetadata{}
	currentPodMetadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	err = podClient.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, currentPodMetadata)
	if err != nil {
		return nil, err
	}

	podClient.podMetadata = currentPodMetadata

	// Only if the fdb-kubernetes-monitor should update the node information, add the watcher here by fetching the node
	// information once during start up.
	if enableNodeWatcher {
		currentNodeMetadata := &metav1.PartialObjectMetadata{}
		currentNodeMetadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Node"))
		err = podClient.Client.Get(ctx, client.ObjectKey{Name: nodeName}, currentNodeMetadata)
		if err != nil {
			return nil, err
		}

		podClient.nodeMetadata = currentNodeMetadata
	}

	return podClient, nil
}

// retrieveEnvironmentVariables extracts the environment variables we have for
// an argument into a map.
func retrieveEnvironmentVariables(monitor *monitor, argument api.Argument, target map[string]string) {
	if argument.Source != "" {
		value, err := argument.LookupEnv(monitor.customEnvironment)
		if err == nil {
			target[argument.Source] = value
		}
	}
	if argument.Values != nil {
		for _, childArgument := range argument.Values {
			retrieveEnvironmentVariables(monitor, childArgument, target)
		}
	}
}

// updateAnnotations updates annotations on the pod after loading new
// configuration.
func (podClient *kubernetesClient) updateAnnotations(monitor *monitor) error {
	environment := make(map[string]string)
	for _, argument := range monitor.activeConfiguration.Arguments {
		retrieveEnvironmentVariables(monitor, argument, environment)
	}
	environment["BINARY_DIR"] = path.Dir(monitor.activeConfiguration.BinaryPath)
	jsonEnvironment, err := json.Marshal(environment)
	if err != nil {
		return err
	}

	return podClient.updateAnnotationsOnPod(map[string]string{
		api.CurrentConfigurationAnnotation: string(monitor.activeConfigurationBytes),
		api.EnvironmentAnnotation:          string(jsonEnvironment),
	})
}

// updateFdbClusterTimestampAnnotation updates the ClusterFileChangeDetectedAnnotation annotation on the pod
// after a change to the fdb.cluster file was detected, e.g. because the coordinators were changed.
func (podClient *kubernetesClient) updateFdbClusterTimestampAnnotation() error {
	return podClient.updateAnnotationsOnPod(map[string]string{
		api.ClusterFileChangeDetectedAnnotation: strconv.FormatInt(time.Now().Unix(), 10),
	})
}

// updateAnnotationsOnPod will update the annotations with the provided annotationChanges. If an annotation exists, it
// will be updated if the annotation is absent it will be added.
func (podClient *kubernetesClient) updateAnnotationsOnPod(annotationChanges map[string]string) error {
	if podClient.podMetadata == nil {
		return fmt.Errorf("pod client has no metadata present")
	}

	if !podClient.podMetadata.DeletionTimestamp.IsZero() {
		return fmt.Errorf("pod is marked for deletion, cannot update annotations")
	}

	// If any error occurs during the update of the annotations, make sure we retry it.
	return retry.OnError(retry.DefaultRetry, func(err error) bool {
		podClient.Logger.Error(err, "could not update annotations of pod")
		// If the pod is marked for deletion, we don't have to retry the patch.
		if !podClient.podMetadata.DeletionTimestamp.IsZero() {
			return false
		}

		// If the resource is not found or the process is forbidden to update the metadata, don't retry it.
		if k8serrors.IsNotFound(err) || k8serrors.IsForbidden(err) {
			return false
		}

		return true
	}, func() error {
		currentAnnotations := podClient.podMetadata.Annotations
		if len(currentAnnotations) == 0 {
			currentAnnotations = map[string]string{}
		}

		var hasChanges bool
		for key, val := range annotationChanges {
			currentValue, present := currentAnnotations[key]
			if !present || currentValue != val {
				podClient.Logger.Info("update annotation with new value", "annotation", key, "currentValue", currentValue, "newValue", val, "present", present)
				currentAnnotations[key] = val
				hasChanges = true
			}
		}

		// If no changes are present, we can skip the patch.
		if !hasChanges {
			return nil
		}

		return podClient.Patch(context.Background(), &corev1.Pod{
			TypeMeta: metav1.TypeMeta{
				Kind:       "Pod",
				APIVersion: "v1",
			},
			ObjectMeta: metav1.ObjectMeta{
				Namespace:   podClient.podMetadata.Namespace,
				Name:        podClient.podMetadata.Name,
				Annotations: currentAnnotations,
			},
		}, client.Apply, client.FieldOwner("fdb-kubernetes-monitor"), client.ForceOwnership)
	})
}

// OnAdd is called when an object is added.
func (podClient *kubernetesClient) OnAdd(obj interface{}) {
	switch castedObj := obj.(type) {
	case *corev1.Pod:
		podClient.Logger.Info("Got event for OnAdd for Pod resource", "name", castedObj.Name, "namespace", castedObj.Namespace)
		podClient.podMetadata = &metav1.PartialObjectMetadata{
			TypeMeta:   castedObj.TypeMeta,
			ObjectMeta: castedObj.ObjectMeta,
		}
	case *corev1.Node:
		podClient.Logger.Info("Got event for OnAdd for Node resource", "name", castedObj.Name)
		podClient.nodeMetadata = &metav1.PartialObjectMetadata{
			TypeMeta:   castedObj.TypeMeta,
			ObjectMeta: castedObj.ObjectMeta,
		}
	}
}

// OnUpdate is also called when a re-list happens, and it will
// get called even if nothing changed. This is useful for periodically
// evaluating or syncing something.
func (podClient *kubernetesClient) OnUpdate(_, newObj interface{}) {
	switch castedObj := newObj.(type) {
	case *corev1.Pod:
		podClient.Logger.Info("Got event for OnUpdate for Pod resource", "name", castedObj.Name, "namespace", castedObj.Namespace, "generation", castedObj.Generation)
		var previousAnnotations map[string]string
		if podClient.podMetadata != nil {
			previousAnnotations = podClient.podMetadata.Annotations
		}

		podClient.podMetadata = &metav1.PartialObjectMetadata{
			TypeMeta:   castedObj.TypeMeta,
			ObjectMeta: castedObj.ObjectMeta,
		}

		if podClient.podMetadata.Annotations == nil {
			return
		}

		// If the IsolateProcessGroupAnnotation changes force a reload of the configuration to make sure the processes
		// will be shutdown.
		previousIsolateProcessGroupAnnotationValue := previousAnnotations[api.IsolateProcessGroupAnnotation]
		newIsolateProcessGroupAnnotationValue := podClient.podMetadata.Annotations[api.IsolateProcessGroupAnnotation]
		if previousIsolateProcessGroupAnnotationValue != newIsolateProcessGroupAnnotationValue {
			podClient.Logger.Info("Got change in isolate process group annotation", "previous", previousIsolateProcessGroupAnnotationValue, "new", newIsolateProcessGroupAnnotationValue)
			podClient.TimestampFeed <- time.Now().Unix()
			// In this case we can return as the timestamp feed already has a new value.
			return
		}

		annotation := podClient.podMetadata.Annotations[api.OutdatedConfigMapAnnotation]
		if annotation == "" {
			return
		}

		timestamp, err := strconv.ParseInt(annotation, 10, 64)
		if err != nil {
			podClient.Logger.Error(err, "Error parsing annotation", "key", api.OutdatedConfigMapAnnotation, "rawAnnotation", annotation)
			return
		}

		podClient.TimestampFeed <- timestamp
	case *corev1.Node:
		podClient.Logger.Info("Got event for OnUpdate for Node resource", "name", castedObj.Name)
		podClient.nodeMetadata = &metav1.PartialObjectMetadata{
			TypeMeta:   castedObj.TypeMeta,
			ObjectMeta: castedObj.ObjectMeta,
		}
	}
}

// OnDelete will get the final state of the item if it is known, otherwise
// it will get an object of type DeletedFinalStateUnknown. This can
// happen if the watch is closed and misses the delete event and we don't
// notice the deletion until the subsequent re-list.
func (podClient *kubernetesClient) OnDelete(obj interface{}) {
	switch castedObj := obj.(type) {
	case *corev1.Pod:
		podClient.Logger.Info("Got event for OnDelete for Pod resource", "name", castedObj.Name, "namespace", castedObj.Namespace)
		podClient.podMetadata = nil
	case *corev1.Node:
		podClient.Logger.Info("Got event for OnDelete for Node resource", "name", castedObj.Name)
		podClient.nodeMetadata = nil
	}
}
