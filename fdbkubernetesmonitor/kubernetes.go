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
	"os"
	"path"
	"strconv"

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// CurrentConfigurationAnnotation is the annotation we use to store the
	// latest configuration.
	CurrentConfigurationAnnotation = "foundationdb.org/launcher-current-configuration"

	// EnvironmentAnnotation is the annotation we use to store the environment
	// variables.
	EnvironmentAnnotation = "foundationdb.org/launcher-environment"

	// OutdatedConfigMapAnnotation is the annotation we read to get notified of
	// outdated configuration.
	OutdatedConfigMapAnnotation = "foundationdb.org/outdated-config-map-seen"

	// DelayShutdownAnnotation defines how long the FDB Kubernetes monitor process should sleep before shutting itself down.
	// The FDB Kubernetes monitor will always shutdown all fdbserver processes, independent of this setting.
	// The value of this annotation must be a duration like "60s".
	DelayShutdownAnnotation = "foundationdb.org/delay-shutdown"
)

// PodClient is a wrapper around the pod API.
type PodClient struct {
	// metadata is the latest metadata that was seen by the fdb-kubernetes-monitor for the according Pod.
	metadata *metav1.PartialObjectMetadata

	// TimestampFeed is a channel where the pod client will send updates with
	// the values from OutdatedConfigMapAnnotation.
	TimestampFeed chan int64

	// Logger is the logger we use for this client.
	Logger logr.Logger

	// Adds the controller runtime client to the PodClient.
	client.Client
}

// CreatePodClient creates a new client for working with the pod object.
func CreatePodClient(ctx context.Context, logger logr.Logger) (*PodClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}

	scheme := runtime.NewScheme()
	err = clientgoscheme.AddToScheme(scheme)
	if err != nil {
		return nil, err
	}

	// Create the new client for writes. This client will also be used to setup the cache.
	internalClient, err := client.NewWithWatch(config, client.Options{
		Scheme: scheme,
	})
	if err != nil {
		return nil, err
	}

	namespace := os.Getenv("FDB_POD_NAMESPACE")
	podName := os.Getenv("FDB_POD_NAME")

	internalCache, err := cache.New(config, cache.Options{
		Scheme:    scheme,
		Mapper:    internalClient.RESTMapper(),
		Namespace: namespace,
		SelectorsByObject: map[client.Object]cache.ObjectSelector{
			&corev1.Pod{}: {
				Field: fields.OneTermEqualSelector(metav1.ObjectNameField, podName),
			},
		},
	})
	if err != nil {
		return nil, err
	}

	// Fetch the informer for the FoundationDBCluster resource
	informer, err := internalCache.GetInformer(ctx, &corev1.Pod{})
	if err != nil {
		return nil, err
	}

	podClient := &PodClient{
		metadata:      nil,
		TimestampFeed: make(chan int64, 10),
		Logger:        logger,
	}

	// Setup an event handler to make sue we get events for new clusters and directly reload.
	_, err = informer.AddEventHandler(podClient)
	if err != nil {
		return nil, err
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

	// Fetch the current metadata before returning the PodClient
	currentMetadata := &metav1.PartialObjectMetadata{}
	currentMetadata.SetGroupVersionKind(corev1.SchemeGroupVersion.WithKind("Pod"))
	err = podClient.Client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: podName}, currentMetadata)
	if err != nil {
		return nil, err
	}

	podClient.metadata = currentMetadata

	return podClient, nil
}

// retrieveEnvironmentVariables extracts the environment variables we have for
// an argument into a map.
func retrieveEnvironmentVariables(monitor *Monitor, argument api.Argument, target map[string]string) {
	if argument.Source != "" {
		value, err := argument.LookupEnv(monitor.CustomEnvironment)
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

// UpdateAnnotations updates annotations on the pod after loading new
// configuration.
func (podClient *PodClient) UpdateAnnotations(monitor *Monitor) error {
	environment := make(map[string]string)
	for _, argument := range monitor.ActiveConfiguration.Arguments {
		retrieveEnvironmentVariables(monitor, argument, environment)
	}
	environment["BINARY_DIR"] = path.Dir(monitor.ActiveConfiguration.BinaryPath)
	jsonEnvironment, err := json.Marshal(environment)
	if err != nil {
		return err
	}

	annotations := podClient.metadata.Annotations
	if len(annotations) > 0 {
		annotations = map[string]string{}
	}
	annotations[CurrentConfigurationAnnotation] = string(monitor.ActiveConfigurationBytes)
	annotations[EnvironmentAnnotation] = string(jsonEnvironment)

	return podClient.Patch(context.Background(), &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   podClient.metadata.Namespace,
			Name:        podClient.metadata.Name,
			Annotations: annotations,
		},
	}, client.Apply, client.FieldOwner("fdb-kubernetes-monitor"), client.ForceOwnership)
}

// OnAdd is called when an object is added.
func (podClient *PodClient) OnAdd(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	podClient.Logger.Info("Got event for OnAdd", "name", pod.Name, "namespace", pod.Namespace)
	podClient.metadata = &metav1.PartialObjectMetadata{
		TypeMeta:   pod.TypeMeta,
		ObjectMeta: pod.ObjectMeta,
	}
}

// OnUpdate is called when an object is modified. Note that oldObj is the
// last known state of the object-- it is possible that several changes
// were combined together, so you can't use this to see every single
// change. OnUpdate is also called when a re-list happens, and it will
// get called even if nothing changed. This is useful for periodically
// evaluating or syncing something.
func (podClient *PodClient) OnUpdate(_, newObj interface{}) {
	pod, ok := newObj.(*corev1.Pod)
	if !ok {
		return
	}

	podClient.Logger.Info("Got event for OnAdd", "name", pod.Name, "namespace", pod.Namespace, "generation", pod.Generation)

	podClient.metadata = &metav1.PartialObjectMetadata{
		TypeMeta:   pod.TypeMeta,
		ObjectMeta: pod.ObjectMeta,
	}

	if podClient.metadata == nil {
		return
	}

	annotation := podClient.metadata.Annotations[OutdatedConfigMapAnnotation]
	if annotation == "" {
		return
	}

	timestamp, err := strconv.ParseInt(annotation, 10, 64)
	if err != nil {
		podClient.Logger.Error(err, "Error parsing annotation", "key", OutdatedConfigMapAnnotation, "rawAnnotation", annotation)
		return
	}

	podClient.TimestampFeed <- timestamp
}

// OnDelete will get the final state of the item if it is known, otherwise
// it will get an object of type DeletedFinalStateUnknown. This can
// happen if the watch is closed and misses the delete event and we don't
// notice the deletion until the subsequent re-list.
func (podClient *PodClient) OnDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	podClient.Logger.Info("Got event for OnDelete", "name", pod.Name, "namespace", pod.Namespace)

	podClient.metadata = &metav1.PartialObjectMetadata{
		TypeMeta:   pod.TypeMeta,
		ObjectMeta: pod.ObjectMeta,
	}
}
