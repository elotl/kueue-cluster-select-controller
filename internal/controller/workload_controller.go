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

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kueuev1beta1 "sigs.k8s.io/kueue/apis/kueue/v1beta1"
)

// WorkloadReconciler reconciles a Workload object
type WorkloadReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// RBAC permissions required to watch and patch Kueue Workloads
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch

func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// 1. Fetch the Workload
	var workload kueuev1beta1.Workload
	if err := r.Get(ctx, req.NamespacedName, &workload); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "Failed to fetch Workload")
		return ctrl.Result{}, err
	}

	// 2. Check if the workload is finalized/admitted to stop redundant patching
	// In Kueue, once .status.clusterName is set, it becomes immutable and overrides nominations.
	if workload.Status.ClusterName != nil && len(*workload.Status.ClusterName) > 0 {
		return ctrl.Result{}, nil
	}

	// 3. Compute your custom recommendations
	// Replace this mock with your genuine multicluster recommendation engine
	recommendedClusters := []string{"anne-dra"}

	// 4. Evaluate if the patch is actually required to avoid infinite loops
	if len(workload.Status.NominatedClusterNames) > 0 {
		// Optimization: Check if your recommendation matches what's already there
		return ctrl.Result{}, nil
	}

	// 5. Use client.MergeFrom to safely patch the status subresource
	patch := client.MergeFrom(workload.DeepCopy())
	workload.Status.NominatedClusterNames = recommendedClusters

	if err := r.Status().Patch(ctx, &workload, patch); err != nil {
		logger.Error(err, "Failed to patch nominatedClusterNames on Workload status")
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
