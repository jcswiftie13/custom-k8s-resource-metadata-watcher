package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		namespace     = flag.String("namespace", "default", "Namespace to watch")
		fieldSelector = flag.String("field-selector", "", "Server-side field selector for pod watch (empty = no field filtering)")
		labelSelector = flag.String("label-selector", "", "Server-side label selector for pod watch (empty = no label filtering)")
		kubeconfig    = flag.String("kubeconfig", "", "Path to kubeconfig (empty = in-cluster, then default kubeconfig)")
	)
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	restCfg, err := buildRestConfig(*kubeconfig)
	if err != nil {
		log.Fatalf("build kube client config failed: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		log.Fatalf("create kubernetes client failed: %v", err)
	}

	parsedFieldSelector := fields.Everything()
	if strings.TrimSpace(*fieldSelector) != "" {
		parsedFieldSelector, err = fields.ParseSelector(*fieldSelector)
		if err != nil {
			log.Fatalf("invalid --field-selector %q: %v", *fieldSelector, err)
		}
	}

	parsedLabelSelector := labels.Everything()
	if strings.TrimSpace(*labelSelector) != "" {
		parsedLabelSelector, err = labels.Parse(*labelSelector)
		if err != nil {
			log.Fatalf("invalid --label-selector %q: %v", *labelSelector, err)
		}
	}

	log.Printf(
		"starting informer observer namespace=%q fieldSelector=%q labelSelector=%q",
		*namespace,
		parsedFieldSelector.String(),
		parsedLabelSelector.String(),
	)

	lw := cache.NewFilteredListWatchFromClient(
		clientset.CoreV1().RESTClient(),
		"pods",
		*namespace,
		func(options *metav1.ListOptions) {
			options.FieldSelector = parsedFieldSelector.String()
			options.LabelSelector = parsedLabelSelector.String()
		},
	)
	informer := cache.NewSharedIndexInformer(
		lw,
		&corev1.Pod{},
		0,
		cache.Indexers{},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod, ok := obj.(*corev1.Pod)
			if !ok {
				return
			}
			printWatchEvent("ADD", nil, pod)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if !ok1 || !ok2 {
				return
			}
			printWatchEvent("UPDATE", oldPod, newPod)
		},
		DeleteFunc: func(obj interface{}) {
			var pod *corev1.Pod
			switch t := obj.(type) {
			case *corev1.Pod:
				pod = t
			case cache.DeletedFinalStateUnknown:
				typed, ok := t.Obj.(*corev1.Pod)
				if ok {
					pod = typed
				}
			}
			if pod == nil {
				return
			}
			printWatchEvent("DELETE", pod, nil)
		},
	})

	stopCh := make(chan struct{})
	defer close(stopCh)
	go informer.Run(stopCh)

	if ok := cache.WaitForCacheSync(ctx.Done(), informer.HasSynced); !ok {
		log.Fatal("informer cache did not sync")
	}
	log.Println("informer cache synced, now streaming events")

	<-ctx.Done()
	log.Println("shutdown signal received, exiting")
}

func buildRestConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	if cfg, err := rest.InClusterConfig(); err == nil {
		return cfg, nil
	}
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		loadingRules,
		&clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func podImages(p *corev1.Pod) []string {
	out := make([]string, 0, len(p.Spec.Containers))
	for _, c := range p.Spec.Containers {
		out = append(out, fmt.Sprintf("%s:%s", c.Name, c.Image))
	}
	return out
}

func formatImagesList(imgs []string) string {
	if len(imgs) == 0 {
		return "[]"
	}
	return "[" + strings.Join(imgs, ",") + "]"
}

func printWatchEvent(eventType string, oldPod, newPod *corev1.Pod) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var ns, name string
	var oldPhase, newPhase corev1.PodPhase
	var oldRV, newRV string
	var oldImages, newImages []string

	if oldPod != nil {
		ns = oldPod.Namespace
		name = oldPod.Name
		oldPhase = oldPod.Status.Phase
		oldRV = oldPod.ResourceVersion
		oldImages = podImages(oldPod)
	}
	if newPod != nil {
		ns = newPod.Namespace
		name = newPod.Name
		newPhase = newPod.Status.Phase
		newRV = newPod.ResourceVersion
		newImages = podImages(newPod)
	}

	oldImagesStr := formatImagesList(oldImages)
	newImagesStr := formatImagesList(newImages)

	oldToNew := ""
	if oldPod != nil && newPod != nil {
		oldToNew = fmt.Sprintf("images: old=%s -> new=%s", oldImagesStr, newImagesStr)
	} else if newPod != nil {
		oldToNew = fmt.Sprintf("images: old=[] -> new=%s", newImagesStr)
	} else {
		oldToNew = fmt.Sprintf("images: old=%s -> new=[]", oldImagesStr)
	}

	fmt.Printf(
		"[%s] %s pod=%s/%s phase=%s->%s rv=%s->%s\n  %s\n",
		now, eventType, ns, name, oldPhase, newPhase, oldRV, newRV, oldToNew,
	)
}
