// kubernetes.go
//
// This source file is part of the FoundationDB open source project
//
// Copyright 2021 Apple Inc. and the FoundationDB project authors
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

	"github.com/apple/foundationdb/fdbkubernetesmonitor/api"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	typedv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
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
)

// PodClient is a wrapper around the pod API.
type PodClient struct {
	// podApi is the raw API
	podApi typedv1.PodInterface

	// pod is the latest pod configuration
	pod *corev1.Pod

	// TimestampFeed is a channel where the pod client will send updates with
	// the values from OutdatedConfigMapAnnotation.
	TimestampFeed chan int64

	// Logger is the logger we use for this client.
	Logger logr.Logger
}

// CreatePodClient creates a new client for working with the pod object.
func CreatePodClient(logger logr.Logger) (*PodClient, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	podApi := client.CoreV1().Pods(os.Getenv("FDB_POD_NAMESPACE"))
	pod, err := podApi.Get(context.Background(), os.Getenv("FDB_POD_NAME"), metav1.GetOptions{ResourceVersion: "0"})
	if err != nil {
		return nil, err
	}

	podClient := &PodClient{podApi: podApi, pod: pod, TimestampFeed: make(chan int64, 10), Logger: logger}
	err = podClient.watchPod()
	if err != nil {
		return nil, err
	}

	return podClient, nil
}

// retrieveEnvironmentVariables extracts the environment variables we have for
// an argument into a map.
func retrieveEnvironmentVariables(argument api.Argument, target map[string]string) {
	if argument.Source != "" {
		target[argument.Source] = os.Getenv(argument.Source)
	}
	if argument.Values != nil {
		for _, childArgument := range argument.Values {
			retrieveEnvironmentVariables(childArgument, target)
		}
	}
}

// UpdateAnnotations updates annotations on the pod after loading new
// configuration.
func (client *PodClient) UpdateAnnotations(monitor *Monitor) error {
	environment := make(map[string]string)
	for _, argument := range monitor.ActiveConfiguration.Arguments {
		retrieveEnvironmentVariables(argument, environment)
	}
	environment["BINARY_DIR"] = path.Dir(monitor.ActiveConfiguration.BinaryPath)
	jsonEnvironment, err := json.Marshal(environment)
	if err != nil {
		return err
	}

	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{
				CurrentConfigurationAnnotation: string(monitor.ActiveConfigurationBytes),
				EnvironmentAnnotation:          string(jsonEnvironment),
			},
		},
	}

	patchJson, err := json.Marshal(patch)
	if err != nil {
		return err
	}

	pod, err := client.podApi.Patch(context.Background(), client.pod.Name, types.MergePatchType, patchJson, metav1.PatchOptions{})
	if err != nil {
		return err
	}
	client.pod = pod
	return nil
}

// watchPod starts a watch on the pod.
func (client *PodClient) watchPod() error {
	podWatch, err := client.podApi.Watch(
		context.Background(),
		metav1.ListOptions{
			Watch:           true,
			ResourceVersion: "0",
			FieldSelector:   fmt.Sprintf("metadata.name=%s", os.Getenv("FDB_POD_NAME")),
		},
	)
	if err != nil {
		return err
	}
	results := podWatch.ResultChan()
	go func() {
		for event := range results {
			if event.Type == watch.Modified {
				pod, valid := event.Object.(*corev1.Pod)
				if !valid {
					client.Logger.Error(nil, "Error getting pod information from watch", "event", event)
				}
				client.processPodUpdate(pod)
			}
		}
	}()

	return nil
}

// processPodUpdate handles an update for a pod.
func (client *PodClient) processPodUpdate(pod *corev1.Pod) {
	client.pod = pod
	if pod.Annotations == nil {
		return
	}
	annotation := client.pod.Annotations[OutdatedConfigMapAnnotation]
	if annotation == "" {
		return
	}
	timestamp, err := strconv.ParseInt(annotation, 10, 64)
	if err != nil {
		client.Logger.Error(err, "Error parsing annotation", "key", OutdatedConfigMapAnnotation, "rawAnnotation", annotation)
		return
	}

	client.TimestampFeed <- timestamp
}
