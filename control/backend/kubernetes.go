//go:build kubernetes

package backend

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Kubernetes runs each job as a batch/v1 Job on a cluster — the same jobs
// the Docker backend runs locally, so the controller, reconciler, API, and
// web UI are unchanged. A Job (not a Deployment) is used so a completed
// pod is not auto-restarted by Kubernetes: weibo's own reconciler owns
// restart decisions (backoffLimit is 0).
//
// Per job: a per-job PVC (state + checkpoints, reused across restarts), a
// ConfigMap (the workflow), an optional Secret (env), and a ClusterIP
// Service so the controller can reach the agent's control surface.
type Kubernetes struct {
	cs                     kubernetes.Interface
	namespace              string
	image                  string
	pvcSize                string
	storageClass           string
	imagePullSecrets       []string
	controlAddressTemplate string
}

// KubernetesOptions configures the backend.
type KubernetesOptions struct {
	Kubeconfig   string // path; empty → in-cluster, else default loading rules
	Namespace    string // default "default"
	Image        string // runner image (must be pullable by the cluster)
	PVCSize      string // per-job volume size, default "1Gi"
	StorageClass string // optional; empty → cluster default
	// ImagePullSecrets names pre-created dockerconfigjson Secrets in the
	// namespace, referenced on every job pod so the cluster can pull from
	// private registries. Weibo references them; it does not create them.
	ImagePullSecrets []string
	// ControlAddressTemplate makes the agent reachable from the controller.
	// Tokens: {service}, {namespace}, {port}. Empty uses cluster-local DNS.
	ControlAddressTemplate string
}

// NewKubernetes connects using in-cluster config (when running in a Pod)
// or the kubeconfig file otherwise.
func NewKubernetes(opts KubernetesOptions) (*Kubernetes, error) {
	cfg, err := restConfig(opts.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("k8s: config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("k8s: client: %w", err)
	}
	return newK8s(cs, opts), nil
}

// newK8s builds the backend from an injected client (used by tests).
func newK8s(cs kubernetes.Interface, opts KubernetesOptions) *Kubernetes {
	k := &Kubernetes{
		cs:                     cs,
		namespace:              orString(opts.Namespace, "default"),
		image:                  opts.Image,
		pvcSize:                orString(opts.PVCSize, "1Gi"),
		storageClass:           opts.StorageClass,
		imagePullSecrets:       opts.ImagePullSecrets,
		controlAddressTemplate: opts.ControlAddressTemplate,
	}
	return k
}

func restConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if c, err := rest.InClusterConfig(); err == nil {
			return c, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

// Ping verifies the API server is reachable.
func (k *Kubernetes) Ping(ctx context.Context) error {
	_, err := k.cs.Discovery().ServerVersion()
	return err
}

func (k *Kubernetes) Capacity(ctx context.Context, cfg CapacityConfig) (CapacitySnapshot, error) {
	snap := CapacitySnapshot{
		Backend:     "kubernetes",
		Health:      "healthy",
		Source:      "kubernetes_api",
		At:          time.Now().UTC(),
		MaxJobs:     cfg.MaxJobs,
		Unsupported: true,
		Reason:      "detailed kubernetes capacity requires quota/node-resource aggregation",
	}
	if err := k.Ping(ctx); err != nil {
		snap.Health = "unreachable"
		snap.Reason = err.Error()
		return snap, nil
	}
	jobs, err := k.cs.BatchV1().Jobs(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: "app.kubernetes.io/managed-by=weibo"})
	if err != nil {
		snap.Health = "degraded"
		snap.Reason = err.Error()
		return snap, nil
	}
	for _, j := range jobs.Items {
		jobID := j.Labels["weibo.job"]
		state := "starting"
		switch {
		case j.Status.Active > 0:
			snap.RunningContainers += int(j.Status.Active)
			snap.UsedSlots += int(j.Status.Active)
			state = "running"
		case j.Status.Succeeded > 0 || j.Status.Failed > 0:
			snap.ExitedContainers++
			state = "exited"
		default:
			snap.StartingContainers++
			snap.UsedSlots++
		}
		snap.Containers = append(snap.Containers, ContainerStats{
			ID:        j.Name,
			Name:      j.Name,
			JobID:     jobID,
			Managed:   jobID != "",
			Image:     firstContainerImage(j.Spec.Template.Spec.Containers),
			State:     state,
			StartedAt: j.CreationTimestamp.Unix(),
		})
	}
	// Namespace ResourceQuota is authoritative when available. Derive slots
	// from remaining requested CPU/memory and the configured per-job defaults.
	if quotas, err := k.cs.CoreV1().ResourceQuotas(k.namespace).List(ctx, metav1.ListOptions{}); err == nil {
		for _, q := range quotas.Items {
			cpuHard, cpuOK := q.Status.Hard[corev1.ResourceRequestsCPU]
			memHard, memOK := q.Status.Hard[corev1.ResourceRequestsMemory]
			if !cpuOK && !memOK {
				continue
			}
			snap.Unsupported, snap.Reason, snap.Source = false, "", "resource_quota"
			if cpuOK {
				used := q.Status.Used[corev1.ResourceRequestsCPU]
				snap.CPUTotalMilli, snap.CPUReservedMilli = cpuHard.MilliValue(), used.MilliValue()
				snap.CPUAvailableMilli = max(0, snap.CPUTotalMilli-snap.CPUReservedMilli)
			}
			if memOK {
				used := q.Status.Used[corev1.ResourceRequestsMemory]
				snap.MemoryTotalBytes, snap.MemoryReservedBytes = memHard.Value(), used.Value()
				snap.MemoryAvailableBytes = max(0, snap.MemoryTotalBytes-snap.MemoryReservedBytes)
			}
			break
		}
	}
	if q, err := resource.ParseQuantity(cfg.DefaultJobCPU); err == nil {
		snap.DefaultJobCPUMilli = q.MilliValue()
	}
	if q, err := resource.ParseQuantity(cfg.DefaultJobMemory); err == nil {
		snap.DefaultJobMemoryBytes = q.Value()
	}
	if !snap.Unsupported {
		slots := -1
		if snap.DefaultJobCPUMilli > 0 && snap.CPUTotalMilli > 0 {
			slots = int(snap.CPUAvailableMilli / snap.DefaultJobCPUMilli)
		}
		if snap.DefaultJobMemoryBytes > 0 && snap.MemoryTotalBytes > 0 {
			m := int(snap.MemoryAvailableBytes / snap.DefaultJobMemoryBytes)
			if slots < 0 || m < slots {
				slots = m
			}
		}
		if slots >= 0 {
			available := slots
			if cfg.MaxJobs > 0 && available > cfg.MaxJobs-snap.UsedSlots {
				available = max(0, cfg.MaxJobs-snap.UsedSlots)
			}
			total := snap.UsedSlots + available
			snap.AvailableSlots, snap.TotalSlots = &available, &total
		}
	}
	if cfg.MaxJobs > 0 && snap.Unsupported {
		available := cfg.MaxJobs - snap.UsedSlots
		if available < 0 {
			available = 0
		}
		total := snap.UsedSlots + available
		snap.AvailableSlots = &available
		snap.TotalSlots = &total
		snap.Source = "configured_limit"
	}
	return snap, nil
}

const (
	k8sWorkflowPath = "/wf/workflow.yaml"
	k8sDataDir      = "/data"
	// Savepoints live under the per-job data volume, so a same-job
	// restart-from-savepoint works without cluster-wide shared storage.
	// Cross-host/cross-job savepoints need an object store (deferred).
	k8sSavepointDir = "/data/savepoints"
	nonRootUID      = int64(65532) // distroless "nonroot"
)

func (k *Kubernetes) Launch(ctx context.Context, spec LaunchSpec) (string, error) {
	image := orString(spec.Image, k.image)
	port := spec.ControlPort
	if port == 0 {
		port = 8080
	}
	jobID := spec.JobID
	run := fmt.Sprintf("weibo-%s-%d", jobID, time.Now().UnixNano())
	pvc := "weibo-" + jobID + "-data"

	// Per-job volume, reused across restarts for state/checkpoints.
	if err := k.ensurePVC(ctx, pvc, jobID); err != nil {
		return "", err
	}

	// An SDK job's image has the pipeline compiled in — no workflow doc to
	// mount. YAML jobs get the document as a ConfigMap.
	cmName := ""
	if len(spec.WorkflowDoc) > 0 {
		cm := &corev1.ConfigMap{
			ObjectMeta: k.meta(run, jobID, run),
			Data:       map[string]string{"workflow.yaml": string(spec.WorkflowDoc)},
		}
		if _, err := k.cs.CoreV1().ConfigMaps(k.namespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
			return "", fmt.Errorf("k8s: create configmap: %w", err)
		}
		cmName = run
	}

	// Env (including any secrets) as a Secret, referenced via envFrom.
	var secretName string
	if len(spec.Env) > 0 {
		sec := &corev1.Secret{
			ObjectMeta: k.meta(run, jobID, run),
			StringData: spec.Env,
		}
		if _, err := k.cs.CoreV1().Secrets(k.namespace).Create(ctx, sec, metav1.CreateOptions{}); err != nil {
			_ = k.cleanup(ctx, run)
			return "", fmt.Errorf("k8s: create secret: %w", err)
		}
		secretName = run
	}

	job := k.buildJob(run, jobID, pvc, cmName, secretName, image, port, spec.RestoreSavepoint, spec.PullPolicy, spec.Resources)
	if _, err := k.cs.BatchV1().Jobs(k.namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
		_ = k.cleanup(ctx, run)
		return "", fmt.Errorf("k8s: create job: %w", err)
	}

	svc := k.buildService(run, jobID, port)
	if _, err := k.cs.CoreV1().Services(k.namespace).Create(ctx, svc, metav1.CreateOptions{}); err != nil {
		_ = k.cleanup(ctx, run)
		return "", fmt.Errorf("k8s: create service: %w", err)
	}
	return run, nil
}

func (k *Kubernetes) buildJob(run, jobID, pvc, cmName, secretName, image string, port int, restore, pullPolicy string, resources *ResourceLimits) *batchv1.Job {
	env := []corev1.EnvVar{
		{Name: "DATA_DIR", Value: k8sDataDir},
		{Name: "SAVEPOINT_DIR", Value: k8sSavepointDir},
		{Name: "PORT", Value: strconv.Itoa(port)},
	}
	if cmName != "" { // YAML job: point the runner at the mounted document
		env = append(env, corev1.EnvVar{Name: "WORKFLOW", Value: k8sWorkflowPath})
	}
	if restore != "" {
		env = append(env, corev1.EnvVar{Name: "RESTORE_SAVEPOINT", Value: restore})
	}
	var envFrom []corev1.EnvFromSource
	if secretName != "" {
		envFrom = []corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: secretName},
		}}}
	}
	liveness := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{
			Path: "/livez", Port: intstr.FromInt(port),
		}},
		InitialDelaySeconds: 3, PeriodSeconds: 5, FailureThreshold: 6,
	}
	readiness := liveness.DeepCopy()
	readiness.HTTPGet.Path = "/readyz"

	// Every job mounts its data PVC; YAML jobs also mount the workflow
	// ConfigMap (SDK jobs have the pipeline compiled into the image).
	mounts := []corev1.VolumeMount{{Name: "data", MountPath: k8sDataDir}}
	volumes := []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc},
	}}}
	if cmName != "" {
		mounts = append(mounts, corev1.VolumeMount{Name: "wf", MountPath: "/wf", ReadOnly: true})
		volumes = append(volumes, corev1.Volume{Name: "wf", VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: cmName}},
		}})
	}

	var pullSecrets []corev1.LocalObjectReference
	for _, name := range k.imagePullSecrets {
		pullSecrets = append(pullSecrets, corev1.LocalObjectReference{Name: name})
	}

	return &batchv1.Job{
		ObjectMeta: k.meta(run, jobID, run),
		Spec: batchv1.JobSpec{
			BackoffLimit: int32Ptr(0), // no k8s retries; weibo reconciler restarts
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels(jobID, run), Annotations: map[string]string{
					"prometheus.io/scrape": "true", "prometheus.io/path": "/metrics", "prometheus.io/port": strconv.Itoa(port),
				}},
				Spec: corev1.PodSpec{
					RestartPolicy:                 corev1.RestartPolicyNever,
					TerminationGracePeriodSeconds: int64Ptr(45), // room to drain + final checkpoint
					ImagePullSecrets:              pullSecrets,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtr(true),
						RunAsUser:    int64Ptr(nonRootUID),
						RunAsGroup:   int64Ptr(nonRootUID),
						// fsGroup makes the PVC writable by the nonroot user
						// (the same ownership issue the Docker image hit).
						FSGroup: int64Ptr(nonRootUID),
					},
					Containers: []corev1.Container{{
						Name:            "runner",
						Image:           image,
						ImagePullPolicy: k8sPullPolicy(pullPolicy),
						Env:             env,
						EnvFrom:         envFrom,
						Ports:           []corev1.ContainerPort{{ContainerPort: int32(port)}},
						VolumeMounts:    mounts,
						LivenessProbe:   liveness,
						ReadinessProbe:  readiness,
						SecurityContext: &corev1.SecurityContext{
							AllowPrivilegeEscalation: boolPtr(false), ReadOnlyRootFilesystem: boolPtr(true),
							Capabilities:   &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
							SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault},
						},
						Resources: k8sResources(resources),
					}},
					Volumes: volumes,
				},
			},
		},
	}
}

func (k *Kubernetes) buildService(run, jobID string, port int) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: k.meta(run, jobID, run),
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"weibo.run": run},
			Ports: []corev1.ServicePort{{
				Port: int32(port), TargetPort: intstr.FromInt(port),
			}},
		},
	}
}

func (k *Kubernetes) ensurePVC(ctx context.Context, name, jobID string) error {
	_, err := k.cs.CoreV1().PersistentVolumeClaims(k.namespace).Get(ctx, name, metav1.GetOptions{})
	if err == nil {
		return nil // reuse existing
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("k8s: get pvc: %w", err)
	}
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: k.meta(name, jobID, ""),
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(k.pvcSize)},
			},
		},
	}
	if k.storageClass != "" {
		pvc.Spec.StorageClassName = &k.storageClass
	}
	_, err = k.cs.CoreV1().PersistentVolumeClaims(k.namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (k *Kubernetes) Status(ctx context.Context, id string) (Status, error) {
	job, err := k.cs.BatchV1().Jobs(k.namespace).Get(ctx, id, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return Status{Phase: PhaseGone}, nil
	}
	if err != nil {
		return Status{}, err
	}
	st := Status{}
	switch {
	case job.Status.Succeeded > 0:
		st.Phase = PhaseExited
		st.ExitCode = 0
	case job.Status.Failed > 0:
		st.Phase = PhaseExited
		st.ExitCode = k.podExitCode(ctx, id)
	case job.Status.Active > 0:
		st.Phase, st.Reason = k.podRuntimeStatus(ctx, id)
	default:
		st.Phase = PhasePending
		st.Reason = k.podPendingReason(ctx, id)
	}
	// The control surface is reachable in-cluster via the Service DNS.
	if svc, err := k.cs.CoreV1().Services(k.namespace).Get(ctx, id, metav1.GetOptions{}); err == nil && len(svc.Spec.Ports) > 0 {
		port := strconv.Itoa(int(svc.Spec.Ports[0].Port))
		tmpl := k.controlAddressTemplate
		if tmpl == "" {
			tmpl = "{service}.{namespace}.svc.cluster.local:{port}"
		}
		st.Address = strings.NewReplacer("{service}", id, "{namespace}", k.namespace, "{port}", port).Replace(tmpl)
	}
	return st, nil
}

func (k *Kubernetes) podRuntimeStatus(ctx context.Context, run string) (Phase, string) {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: "weibo.run=" + run})
	if err != nil || len(pods.Items) == 0 {
		return PhasePending, "waiting for pod"
	}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Running != nil && cs.Ready {
				return PhaseRunning, ""
			}
		}
	}
	return PhasePending, k.podPendingReason(ctx, run)
}

func (k *Kubernetes) podPendingReason(ctx context.Context, run string) string {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: "weibo.run=" + run})
	if err != nil || len(pods.Items) == 0 {
		return "waiting for pod"
	}
	p := pods.Items[0]
	for _, c := range p.Status.Conditions {
		if c.Status == corev1.ConditionFalse && c.Reason != "" {
			return c.Reason + ": " + c.Message
		}
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Waiting != nil {
			return cs.State.Waiting.Reason + ": " + cs.State.Waiting.Message
		}
	}
	return string(p.Status.Phase)
}

// podExitCode returns a failed pod's container exit code, or 1 if unknown.
func (k *Kubernetes) podExitCode(ctx context.Context, run string) int {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: "weibo.run=" + run})
	if err != nil {
		return 1
	}
	for _, p := range pods.Items {
		for _, cs := range p.Status.ContainerStatuses {
			if cs.State.Terminated != nil {
				return int(cs.State.Terminated.ExitCode)
			}
		}
	}
	return 1
}

func (k *Kubernetes) Stop(ctx context.Context, id string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	stopCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	seconds := int64(timeout.Round(time.Second) / time.Second)
	if seconds < 1 {
		seconds = 1
	}
	prop := metav1.DeletePropagationForeground
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(stopCtx, metav1.ListOptions{LabelSelector: "weibo.run=" + id})
	if err != nil {
		return fmt.Errorf("k8s: list pods for stop %s: %w", id, err)
	}
	for _, pod := range pods.Items {
		if err := k.cs.CoreV1().Pods(k.namespace).Delete(stopCtx, pod.Name, metav1.DeleteOptions{GracePeriodSeconds: &seconds}); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("k8s: stop pod %s: %w", pod.Name, err)
		}
	}
	err = k.cs.BatchV1().Jobs(k.namespace).Delete(stopCtx, id, metav1.DeleteOptions{
		PropagationPolicy: &prop, GracePeriodSeconds: &seconds,
	})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("k8s: stop job %s: %w", id, err)
	}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		_, jobErr := k.cs.BatchV1().Jobs(k.namespace).Get(stopCtx, id, metav1.GetOptions{})
		pods, podErr := k.cs.CoreV1().Pods(k.namespace).List(stopCtx, metav1.ListOptions{LabelSelector: "weibo.run=" + id})
		if apierrors.IsNotFound(jobErr) && podErr == nil && len(pods.Items) == 0 {
			return nil
		}
		if jobErr != nil && !apierrors.IsNotFound(jobErr) {
			return fmt.Errorf("k8s: wait for job %s termination: %w", id, jobErr)
		}
		if podErr != nil {
			return fmt.Errorf("k8s: wait for pods of %s: %w", id, podErr)
		}
		select {
		case <-stopCtx.Done():
			return fmt.Errorf("k8s: job %s did not terminate within %s: %w", id, timeout, stopCtx.Err())
		case <-ticker.C:
		}
	}
}

func (k *Kubernetes) Logs(ctx context.Context, id string, tail int) (string, error) {
	pods, err := k.cs.CoreV1().Pods(k.namespace).List(ctx, metav1.ListOptions{LabelSelector: "weibo.run=" + id})
	if err != nil {
		return "", err
	}
	if len(pods.Items) == 0 {
		return "", nil
	}
	opts := &corev1.PodLogOptions{}
	if tail > 0 {
		t := int64(tail)
		opts.TailLines = &t
	}
	req := k.cs.CoreV1().Pods(k.namespace).GetLogs(pods.Items[0].Name, opts)
	rc, err := req.Stream(ctx)
	if err != nil {
		return "", err
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	return string(b), err
}

func (k *Kubernetes) Remove(ctx context.Context, id string) error {
	return k.cleanup(ctx, id)
}

// cleanup best-effort deletes a run's Job, Service, ConfigMap, and Secret
// (the per-job PVC is kept so state survives).
func (k *Kubernetes) cleanup(ctx context.Context, run string) error {
	prop := metav1.DeletePropagationBackground
	var errs []error
	for kind, err := range map[string]error{
		"job":       k.cs.BatchV1().Jobs(k.namespace).Delete(ctx, run, metav1.DeleteOptions{PropagationPolicy: &prop}),
		"service":   k.cs.CoreV1().Services(k.namespace).Delete(ctx, run, metav1.DeleteOptions{}),
		"configmap": k.cs.CoreV1().ConfigMaps(k.namespace).Delete(ctx, run, metav1.DeleteOptions{}),
		"secret":    k.cs.CoreV1().Secrets(k.namespace).Delete(ctx, run, metav1.DeleteOptions{}),
	} {
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete %s: %w", kind, err))
		}
	}
	return errors.Join(errs...)
}

func (k *Kubernetes) meta(name, jobID, run string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name, Namespace: k.namespace, Labels: labels(jobID, run)}
}

func labels(jobID, run string) map[string]string {
	m := map[string]string{"app.kubernetes.io/managed-by": "weibo", "weibo.job": jobID}
	if run != "" {
		m["weibo.run"] = run
	}
	return m
}

func int32Ptr(v int32) *int32 { return &v }
func int64Ptr(v int64) *int64 { return &v }

func firstContainerImage(containers []corev1.Container) string {
	if len(containers) == 0 {
		return ""
	}
	return containers[0].Image
}
func boolPtr(v bool) *bool { return &v }

// k8sPullPolicy maps a LaunchSpec.PullPolicy to a Kubernetes pull policy.
// Empty (or unknown) → "" so the cluster applies its own default (Always for
// :latest / untagged, IfNotPresent otherwise).
func k8sPullPolicy(policy string) corev1.PullPolicy {
	switch policy {
	case PullAlways:
		return corev1.PullAlways
	case PullNever:
		return corev1.PullNever
	case PullIfNotPresent:
		return corev1.PullIfNotPresent
	default:
		return ""
	}
}

// k8sResources maps ResourceLimits to a container's ResourceRequirements
// with requests == limits (a fixed, guaranteed-QoS reservation). Nil or
// empty fields are omitted, leaving that dimension unconstrained. The
// controller validates the quantity strings, so ParseQuantity cannot fail
// here — MustParse would only panic on an already-rejected value.
func k8sResources(r *ResourceLimits) corev1.ResourceRequirements {
	if r == nil || (r.CPU == "" && r.Memory == "") {
		return corev1.ResourceRequirements{}
	}
	list := corev1.ResourceList{}
	if r.CPU != "" {
		list[corev1.ResourceCPU] = resource.MustParse(r.CPU)
	}
	if r.Memory != "" {
		list[corev1.ResourceMemory] = resource.MustParse(r.Memory)
	}
	return corev1.ResourceRequirements{Requests: list, Limits: list}
}

// compile-time check.
var _ ContainerBackend = (*Kubernetes)(nil)
