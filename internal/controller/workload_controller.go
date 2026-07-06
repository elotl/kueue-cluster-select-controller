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
	"fmt"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// WorkloadReconciler reconciles a Workload object
type WorkloadReconciler struct {
	client.Client  // local cluster
	Scheme         *runtime.Scheme
	SchedAPIClient kubernetes.Interface // Nova scheduling API server inside cluster
}

const TargetClusterLabelKey = "nova.elotl.co/target-cluster"

// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch
func (r *WorkloadReconciler) getOwnerJob(ctx context.Context, c client.Client, wl *kueuev1beta1.Workload) (*batchv1.Job, error) {
	logger := log.FromContext(ctx)

	for _, ref := range wl.OwnerReferences {
		if ref.Kind == "Job" && ref.APIVersion == "batch/v1" {
			job := &batchv1.Job{}
			err := c.Get(ctx, types.NamespacedName{
				Namespace: wl.Namespace,
				Name:      ref.Name,
			}, job)
			if err != nil {
				return nil, err
			}
			// Verify UID matches to guard against name reuse
			if job.UID != ref.UID {
				return nil, fmt.Errorf("job UID mismatch")
			}
			return job, nil
		}
		logger.Info("Non-job owner", "clusters", ref)
	}
	return nil, fmt.Errorf("no matching job owner found")
}

func sanitizeJobForSubmission(job *batchv1.Job) *batchv1.Job {
	j := job.DeepCopy()

	// Clear server-populated metadata
	j.ResourceVersion = ""
	j.UID = ""
	j.CreationTimestamp = metav1.Time{}
	j.ManagedFields = nil
	j.OwnerReferences = nil
	j.Finalizers = nil
	j.Generation = 0

	// Clear the auto-generated selector — the target API server will generate its own
	j.Spec.Selector = nil

	// Remove the auto-generated controller labels from the pod template
	// These are tied to the UID of the Job on the SOURCE cluster
	labelsToRemove := []string{
		"controller-uid",
		"job-name",
		"batch.kubernetes.io/controller-uid",
		"batch.kubernetes.io/job-name",
	}
	for _, label := range labelsToRemove {
		delete(j.Spec.Template.Labels, label)
	}

	// Let the target API server auto-generate the selector and labels
	if j.Spec.Template.Labels == nil {
		j.Spec.Template.Labels = map[string]string{}
	}

	return j
}

func (r *WorkloadReconciler) getJobFromSchedAPI(
	ctx context.Context,
	job *batchv1.Job,
) (*batchv1.Job, error) {

	existing, err := r.SchedAPIClient.BatchV1().Jobs(job.Namespace).Get(
		ctx,
		job.Name,
		metav1.GetOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("fetching job from sched API: %w", err)
	}

	return existing, nil
}

func (r *WorkloadReconciler) submitJobToSchedAPI(
	ctx context.Context,
	job *batchv1.Job,
) (*batchv1.Job, error) {

	sanitizedJob := sanitizeJobForSubmission(job)
	result, err := r.SchedAPIClient.BatchV1().Jobs(job.Namespace).Create(
		ctx,
		sanitizedJob,
		metav1.CreateOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("submitting job to sched API: %w", err)
	}

	return result, nil
}

func (r *WorkloadReconciler) delJobFromSchedAPI(
	ctx context.Context,
	job *batchv1.Job,
) error {

	err := r.SchedAPIClient.BatchV1().Jobs(job.Namespace).Delete(
		ctx,
		job.Name,
		metav1.DeleteOptions{},
	)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	return nil
}

func (r *WorkloadReconciler) delNovaJob(ctx context.Context, workload *kueuev1beta1.Workload) {
	logger := log.FromContext(ctx)

	ownerJob, err := r.getOwnerJob(ctx, r.Client, workload)
	if err != nil {
		logger.Error(err, "Workload ownerJob fetch error in delNovaJob")
		return
	}

	novaJob, err := r.getJobFromSchedAPI(ctx, ownerJob)
	if err != nil {
		if !errors.IsNotFound(err) {
			logger.Error(err, "Nova job fetch error in delNovaJob")
		}
		return
	}

	if novaJob != nil {
		logger.Info("Successfully got Nova job in delNovaJob", "clusters", novaJob)
		err = r.delJobFromSchedAPI(ctx, novaJob)
		if err != nil {
			logger.Error(err, "Nova job delete error in delNovaJob")
		}
	}
}

func (r *WorkloadReconciler) getRecommendedClusters(ctx context.Context, workload *kueuev1beta1.Workload) []string {
	logger := log.FromContext(ctx)

	ownerJob, err := r.getOwnerJob(ctx, r.Client, workload)
	if err != nil {
		logger.Error(err, "Workload ownerJob fetch error in getRecommendedClusters")
		return []string{}
	}
	logger.Info("Successfully got Workload owner job", "clusters", ownerJob)

	novaJob, err := r.getJobFromSchedAPI(ctx, ownerJob)
	if err != nil {
		logger.Error(err, "Nova job fetch error in getRecommendedClusters")
		return []string{}
	}

	// If job was already submitted to Nova, check for target cluster label
	if novaJob != nil {
		logger.Info("Successfully got Nova job in getRecommendedClusters", "clusters", novaJob)
		if targetCluster, found := novaJob.GetLabels()[TargetClusterLabelKey]; found {
			logger.Info("Nova job has assigned target cluster", "clusters", targetCluster)
			return []string{targetCluster}
		}
		logger.Info("Nova job has not yet been assigned a target cluster")
		return []string{}
	}

	// Submit Job to Nova for target cluster schedule selection
	result, err := r.submitJobToSchedAPI(ctx, ownerJob)
	if err != nil {
		logger.Error(err, "Nova job submission error")
		return []string{}
	}
	logger.Info("Successfully submitted job to sched endpoint", "clusters", result)

	return []string{}
}

// RBAC permissions required to watch and patch Kueue Workloads
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch

// This MultiKueue job workload reconciler fetches a target cluster recommendation from Nova running
// in schedule-only mode.  And when the MultiKueue workload is complete, it deletes the job from Nova.
func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the Workload
	var workload kueuev1beta1.Workload
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Reconcile failed to fetch Workload")
		return ctrl.Result{}, err
	}

	// 2. Check if the workload is finalized/admitted to stop redundant target cluster patching
	// In Kueue, once .status.clusterName is set, it becomes immutable and overrides nominations.
	if workload.Status.ClusterName != nil && len(*workload.Status.ClusterName) > 0 {

		// If workload is finished, clean up associated Nova job
		finishedCond := meta.FindStatusCondition(workload.Status.Conditions, kueuev1beta1.WorkloadFinished)
		if finishedCond != nil && finishedCond.Status == metav1.ConditionTrue {
			r.delNovaJob(ctx, &workload)
		}

		return ctrl.Result{}, nil
	}

	// 3. Compute cluster recommendation
	recommendedClusters := r.getRecommendedClusters(ctx, &workload)

	if len(recommendedClusters) == 0 {
		return ctrl.Result{RequeueAfter: 20 * time.Second}, nil
	}

	// 4. Evaluate if the patch is actually required to avoid infinite loops
	if len(workload.Status.NominatedClusterNames) > 0 {
		// Optimization: Check if your recommendation matches what's already there
		return ctrl.Result{}, nil
	}

	// 5. Use client.MergeFrom to safely patch the status subresource
	patch := client.MergeFrom(workload.DeepCopy())
	workload.Status.NominatedClusterNames = recommendedClusters

	if err := r.Status().Patch(ctx, &workload, patch); err != nil {
		logger.Error(err, "Reconcile failed to patch nominatedClusterNames on Workload status")
		return ctrl.Result{}, err
	}

	logger.Info("Successfully patched nominated clusters into Workload", "clusters", recommendedClusters)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kueuev1beta1.Workload{}).
		Complete(r)
}
