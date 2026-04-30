//go:build integration
package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/yaml"
)

const pendingPodYAML = `
apiVersion: v1
kind: Pod
metadata:
  name: informer-pending-image-pod
spec:
  # Keep the Pod from being scheduled/running in Kind.
  # Without a real node with this name, status.phase should remain Pending.
  nodeName: nonexistent-node
  restartPolicy: Never
  containers:
    - name: pause-main
      image: registry.k8s.io/pause:3.9
`

type watchUpdateEvent struct {
	podKey         string
	oldImages      []string
	newImages      []string
	newPhase       corev1.PodPhase
	newResourceVer string
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
	var phase corev1.PodPhase
	var rv string
	var oldImages, newImages []string

	if oldPod != nil {
		ns = oldPod.Namespace
		name = oldPod.Name
		oldImages = podImages(oldPod)
	}
	if newPod != nil {
		ns = newPod.Namespace
		name = newPod.Name
		phase = newPod.Status.Phase
		rv = newPod.ResourceVersion
		newImages = podImages(newPod)
	}

	matchesPending := false
	if newPod != nil && newPod.Status.Phase == corev1.PodPending {
		matchesPending = true
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

	// Single fmt.Println keeps the handler output readable in `go test -v`.
	fmt.Println(fmt.Sprintf(
		"[%s] %s pod=%s/%s phase=%s rv=%s\n  %s\n  matchesPending=%t",
		now, eventType, ns, name, phase, rv, oldToNew, matchesPending,
	))
}

func TestInformer_PodPendingImageUpdateNotifies(t *testing.T) {
	t.Cleanup(func() {
		if t.Failed() {
			dumpLogs(t)
		}
	})

	cs := mustClient(t)

	ns := "e2e-informer-pending-image-0"
	createNamespaces(t, ns)
	t.Cleanup(func() { deleteNamespaces(t, ns) })

	const (
		podName      = "informer-pending-image-pod"
		updatedImage = "registry.k8s.io/pause:3.10"
	)

	// Set up an informer that *only* watches Pods whose status.phase is Pending
	// via server-side FieldSelector.
	addedCh := make(chan []string, 10)
	updatedCh := make(chan watchUpdateEvent, 10)

	informerStopCh := make(chan struct{})
	defer close(informerStopCh)

	lw := cache.NewListWatchFromClient(
		cs.CoreV1().RESTClient(),
		"pods",
		ns,
		fields.OneTermEqualSelector("status.phase", string(corev1.PodPending)),
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
			if pod.Name == podName && pod.Status.Phase == corev1.PodPending {
				select {
				case addedCh <- podImages(pod):
				default:
				}
			}
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if !ok1 || !ok2 {
				return
			}
			printWatchEvent("UPDATE", oldPod, newPod)

			// Only treat the image update we care about as the assertion signal.
			if newPod.Name == podName &&
				newPod.Status.Phase == corev1.PodPending &&
				len(newPod.Spec.Containers) > 0 &&
				newPod.Spec.Containers[0].Image == updatedImage {
				ev := watchUpdateEvent{
					podKey:         fmt.Sprintf("%s/%s", newPod.Namespace, newPod.Name),
					oldImages:      podImages(oldPod),
					newImages:      podImages(newPod),
					newPhase:       newPod.Status.Phase,
					newResourceVer: newPod.ResourceVersion,
				}
				select {
				case updatedCh <- ev:
				default:
				}
			}
		},
	})

	go informer.Run(informerStopCh)

	// Wait for informer cache to be synced before we create the Pod.
	syncCtx, syncCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer syncCancel()
	if err := waitFor(syncCtx, 250*time.Millisecond, func(ctx context.Context) (bool, error) {
		return informer.HasSynced(), nil
	}); err != nil {
		t.Fatalf("informer cache did not sync: %v", err)
	}

	// Create the pending Pod fixture from YAML (not via Deployment).
	var pod corev1.Pod
	if err := yaml.Unmarshal([]byte(pendingPodYAML), &pod); err != nil {
		t.Fatalf("unmarshal pending pod yaml: %v", err)
	}
	pod.Namespace = ns

	createCtx, createCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer createCancel()
	_, err := cs.CoreV1().Pods(ns).Create(createCtx, &pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create pending pod %s/%s: %v", ns, podName, err)
	}

	// Wait for API-side confirmation that the Pod remains Pending.
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer waitCancel()
	if err := waitFor(waitCtx, 250*time.Millisecond, func(ctx context.Context) (bool, error) {
		p, err := cs.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return p.Status.Phase == corev1.PodPending, nil
	}); err != nil {
		t.Fatalf("pod did not stay Pending: %v", err)
	}

	// Wait for informer ADD.
	var addedImages []string
	addCtx, addCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer addCancel()
	if err := waitFor(addCtx, 200*time.Millisecond, func(ctx context.Context) (bool, error) {
		select {
		case imgs := <-addedCh:
			addedImages = imgs
			return true, nil
		default:
			return false, nil
		}
	}); err != nil {
		t.Fatalf("informer did not receive ADD for %s/%s pending pod: %v", ns, podName, err)
	}
	t.Logf("ADD received for %s images=%v", podName, addedImages)

	// Patch image while phase remains Pending.
	patchCtx, patchCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer patchCancel()
	patchJSON := fmt.Sprintf(
		`[{"op":"replace","path":"/spec/containers/0/image","value":%q}]`,
		updatedImage,
	)
	if _, err := cs.CoreV1().Pods(ns).Patch(patchCtx, podName, types.JSONPatchType, []byte(patchJSON), metav1.PatchOptions{}); err != nil {
		t.Fatalf("patch pod image: %v (patch=%s)", err, patchJSON)
	}

	// Wait for informer UPDATE notification.
	var ev watchUpdateEvent
	updateCtx, updateCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer updateCancel()
	if err := waitFor(updateCtx, 200*time.Millisecond, func(ctx context.Context) (bool, error) {
		select {
		case e := <-updatedCh:
			ev = e
			return true, nil
		default:
			return false, nil
		}
	}); err != nil {
		t.Fatalf("informer did not receive UPDATE (image=%s) for %s/%s: %v", updatedImage, ns, podName, err)
	}

	// Re-check API object for the final truth.
	after, err := cs.CoreV1().Pods(ns).Get(context.Background(), podName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod after patch: %v", err)
	}
	if after.Status.Phase != corev1.PodPending {
		t.Fatalf("expected pod to remain Pending after image patch; got phase=%s", after.Status.Phase)
	}
	if len(after.Spec.Containers) == 0 || after.Spec.Containers[0].Image != updatedImage {
		t.Fatalf("expected pod spec.containers[0].image=%q after patch; got %q", updatedImage, after.Spec.Containers[0].Image)
	}

	// Sanity: the update event we captured should reflect the new image.
	foundUpdated := false
	for _, img := range ev.newImages {
		if strings.Contains(img, updatedImage) {
			foundUpdated = true
			break
		}
	}
	if !foundUpdated {
		t.Fatalf("captured update event did not include updated image %q; newImages=%v", updatedImage, ev.newImages)
	}

	t.Logf("SUCCESS: informer UPDATE received. oldImages=%v newImages=%v rv=%s", ev.oldImages, ev.newImages, ev.newResourceVer)
}

