/*
Copyright 2026 The Kubernetes Authors.

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

package e2e

import (
	"fmt"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	leaderworkerset "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/lws/pkg/schedulerprovider"
	testing "sigs.k8s.io/lws/test/testutils"
	"sigs.k8s.io/lws/test/wrappers"
)

const (
	// wasTopologyKey matches the node labels in test/e2e/config/kind-was.yaml.
	// topology.kubernetes.io/zone is on kubelet's --node-labels allow-list.
	wasTopologyKey   = "topology.kubernetes.io/zone"
	wasTopologyRackA = "zone-a"

	wasRolloutTimeout = 3 * time.Minute
)

var _ = ginkgo.Describe("leaderWorkerSet workload-aware scheduling behavior", ginkgo.Label("WorkloadAwareScheduling"), func() {
	var ns *corev1.Namespace
	var lws *leaderworkerset.LeaderWorkerSet

	ginkgo.BeforeEach(func() {
		if schedulerProvider != schedulerprovider.Kubernetes {
			ginkgo.Skip("WAS tests require SCHEDULER_PROVIDER=kubernetes")
		}
		ns = &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "was-e2e-",
			},
		}
		gomega.Expect(k8sClient.Create(ctx, ns)).To(gomega.Succeed())
		lws = wrappers.BuildLeaderWorkerSet(ns.Name).Obj()
		lws.Spec.Scheduling = &leaderworkerset.LeaderWorkerSetScheduling{}
	})

	ginkgo.AfterEach(func() {
		gomega.Expect(testing.DeleteNamespace(ctx, k8sClient, ns)).To(gomega.Succeed())
	})

	ginkgo.It("Should create separate leader and worker PodGroups in role mode", func() {
		lws.Spec.Replicas = ptr.To[int32](1)
		lws.Spec.LeaderWorkerTemplate.Size = ptr.To[int32](3)
		lws.Spec.Scheduling = &leaderworkerset.LeaderWorkerSetScheduling{
			Replica: &leaderworkerset.LeaderWorkerSetReplicaScheduling{
				Leader: &leaderworkerset.LeaderWorkerSetLeaderScheduling{},
				Worker: &leaderworkerset.LeaderWorkerSetWorkerScheduling{
					SchedulingPolicy: &schedulingv1alpha3.WorkloadPodGroupSchedulingPolicy{
						Gang: &schedulingv1alpha3.WorkloadPodGroupGangSchedulingPolicy{},
					},
				},
			},
		}
		testing.MustCreateLws(ctx, k8sClient, lws)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")

		gomega.Eventually(func(g gomega.Gomega) {
			workload := &schedulingv1beta1.Workload{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: schedulerprovider.KubernetesWorkloadName(lws)}, workload)).To(gomega.Succeed())
			g.Expect(metav1.IsControlledBy(workload, lws)).To(gomega.BeTrue())
			g.Expect(workload.Spec.PodGroupTemplates).To(gomega.HaveLen(2))
			templates := map[string]schedulingv1beta1.PodGroupTemplate{}
			for _, template := range workload.Spec.PodGroupTemplates {
				templates[template.Name] = template
			}
			g.Expect(templates).To(gomega.HaveKey("leader"))
			g.Expect(templates).To(gomega.HaveKey("worker"))
			g.Expect(templates["leader"].SchedulingPolicy.Basic).NotTo(gomega.BeNil())
			g.Expect(templates["leader"].SchedulingPolicy.Gang).To(gomega.BeNil())
			g.Expect(templates["worker"].SchedulingPolicy.Gang).NotTo(gomega.BeNil())
			g.Expect(templates["worker"].SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(2)))

			pods := listLiveLWSPods(g, lws)
			g.Expect(pods).To(gomega.HaveLen(3))
			groups := listLWSPodGroups(g, lws)
			g.Expect(groups).To(gomega.HaveLen(2))
			byRole := map[string]schedulingv1beta1.PodGroup{}
			for _, group := range groups {
				g.Expect(metav1.IsControlledBy(&group, lws)).To(gomega.BeTrue())
				g.Expect(group.Spec.WorkloadRef).NotTo(gomega.BeNil())
				g.Expect(group.Spec.WorkloadRef.WorkloadName).To(gomega.Equal(workload.Name))
				byRole[group.Labels[schedulerprovider.PodGroupRoleLabelKey]] = group
			}
			leaderGroup, hasLeader := byRole["leader"]
			workerGroup, hasWorker := byRole["worker"]
			g.Expect(hasLeader).To(gomega.BeTrue())
			g.Expect(hasWorker).To(gomega.BeTrue())
			g.Expect(leaderGroup.Name).NotTo(gomega.Equal(workerGroup.Name))
			g.Expect(leaderGroup.Spec.SchedulingPolicy.Basic).NotTo(gomega.BeNil())
			g.Expect(leaderGroup.Spec.SchedulingPolicy.Gang).To(gomega.BeNil())
			g.Expect(workerGroup.Spec.SchedulingPolicy.Gang).NotTo(gomega.BeNil())
			g.Expect(workerGroup.Spec.SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(2)))
			g.Expect(apimeta.IsStatusConditionTrue(workerGroup.Status.Conditions, schedulingv1beta1.PodGroupInitiallyScheduled)).To(gomega.BeTrue())

			var leaders, workers int
			for _, pod := range pods {
				g.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty())
				g.Expect(podIsReady(pod)).To(gomega.BeTrue())
				g.Expect(pod.Spec.SchedulingGroup).NotTo(gomega.BeNil())
				g.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(gomega.BeNil())
				if pod.Labels[leaderworkerset.WorkerIndexLabelKey] == "0" {
					leaders++
					g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
					g.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(gomega.Equal(leaderGroup.Name))
				} else {
					workers++
					g.Expect(*pod.Spec.SchedulingGroup.PodGroupName).To(gomega.Equal(workerGroup.Name))
				}
			}
			g.Expect(leaders).To(gomega.Equal(1))
			g.Expect(workers).To(gomega.Equal(2))
		}, timeout, interval).Should(gomega.Succeed())
	})

	ginkgo.It("Should keep old-revision gangs at their previous size while a size increase rolls out", func() {
		lws.Spec.Replicas = ptr.To[int32](2)
		lws.Spec.LeaderWorkerTemplate.Size = ptr.To[int32](2)
		testing.MustCreateLws(ctx, k8sClient, lws)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 4, 2, 2)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")

		initial := podsByGroupIndex(listLiveLWSPods(specG(), lws))
		gomega.Expect(initial["0"]).NotTo(gomega.BeEmpty())
		oldRevision := initial["0"][0].Labels[leaderworkerset.RevisionKey]
		gomega.Expect(oldRevision).NotTo(gomega.BeEmpty())
		oldGroup := podGroupName(specG(), initial["0"][0])

		ginkgo.By("increasing size while partition holds ordinal 0 on the old revision")
		updateRollingPartition(lws, 1)
		testing.UpdateSize(ctx, k8sClient, lws, 3)

		gomega.Eventually(func(g gomega.Gomega) {
			workload := &schedulingv1beta1.Workload{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: schedulerprovider.KubernetesWorkloadName(lws)}, workload)).To(gomega.Succeed())
			g.Expect(workload.Spec.PodGroupTemplates).To(gomega.HaveLen(1))
			g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(gomega.BeNil())
			g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(3)))

			byIndex := podsByGroupIndex(listLiveLWSPods(g, lws))
			g.Expect(byIndex["0"]).To(gomega.HaveLen(2))
			g.Expect(byIndex["1"]).To(gomega.HaveLen(3))
			expectReplicaGang(g, byIndex["0"], 2, oldRevision, oldGroup)
			newRevision := byIndex["1"][0].Labels[leaderworkerset.RevisionKey]
			g.Expect(newRevision).NotTo(gomega.Equal(oldRevision))
			expectReplicaGang(g, byIndex["1"], 3, newRevision, "")
		}, wasRolloutTimeout, interval).Should(gomega.Succeed())

		gomega.Consistently(func(g gomega.Gomega) {
			byIndex := podsByGroupIndex(listLiveLWSPods(g, lws))
			g.Expect(byIndex["0"]).To(gomega.HaveLen(2))
			expectReplicaGang(g, byIndex["0"], 2, oldRevision, oldGroup)
		}, 5*time.Second, interval).Should(gomega.Succeed())

		ginkgo.By("clearing the partition so the old replica rolls forward")
		updateRollingPartition(lws, 0)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 6, 3, 3)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")
	})

	ginkgo.It("Should schedule a whole-LWS gang through a size increase", func() {
		lws.Spec.Replicas = ptr.To[int32](2)
		lws.Spec.LeaderWorkerTemplate.Size = ptr.To[int32](2)
		lws.Spec.Scheduling.SchedulingPolicy = &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
			Gang: &schedulingv1alpha3.WorkloadCompositePodGroupGangSchedulingPolicy{},
		}
		testing.MustCreateLws(ctx, k8sClient, lws)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 4, 4)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")

		ginkgo.By("increasing size while one replica is still the old size")
		updateRollingPartition(lws, 1)
		testing.UpdateSize(ctx, k8sClient, lws, 3)

		// replicas*newSize is 6, but only 5 pods exist until ordinal 0 rolls.
		// A minCount of 6 never passes PreEnqueue and the rollout deadlocks.
		gomega.Eventually(func(g gomega.Gomega) {
			workload := &schedulingv1beta1.Workload{}
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: ns.Name, Name: schedulerprovider.KubernetesWorkloadName(lws)}, workload)).To(gomega.Succeed())
			g.Expect(workload.Spec.PodGroupTemplates).To(gomega.HaveLen(1))
			g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang).NotTo(gomega.BeNil())
			g.Expect(workload.Spec.PodGroupTemplates[0].SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(6)))

			byIndex := podsByGroupIndex(listLiveLWSPods(g, lws))
			g.Expect(byIndex["0"]).To(gomega.HaveLen(2))
			g.Expect(byIndex["1"]).To(gomega.HaveLen(3))
			for _, pod := range byIndex["0"] {
				g.Expect(pod.Annotations[leaderworkerset.SizeAnnotationKey]).To(gomega.Equal("2"))
			}
			for _, pod := range byIndex["1"] {
				g.Expect(pod.Annotations[leaderworkerset.SizeAnnotationKey]).To(gomega.Equal("3"))
			}
			pods := append(append([]corev1.Pod{}, byIndex["0"]...), byIndex["1"]...)
			expectSharedGang(g, pods, 5)
		}, wasRolloutTimeout, interval).Should(gomega.Succeed())

		ginkgo.By("clearing the partition and waiting for the full gang")
		updateRollingPartition(lws, 0)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 6, 6)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")
	})

	ginkgo.It("Should roll out a template change with maxSurge while old gangs stay scheduled", func() {
		lws.Spec.Replicas = ptr.To[int32](2)
		lws.Spec.LeaderWorkerTemplate.Size = ptr.To[int32](2)
		lws.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxSurge = intstr.FromInt(1)
		lws.Spec.RolloutStrategy.RollingUpdateConfiguration.MaxUnavailable = intstr.FromInt(0)
		testing.MustCreateLws(ctx, k8sClient, lws)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 4, 2, 2)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")

		initial := podsByGroupIndex(listLiveLWSPods(specG(), lws))
		originalUID := map[string]types.UID{}
		originalGroup := map[string]string{}
		for _, index := range []string{"0", "1"} {
			leader := leaderFrom(specG(), initial[index])
			originalUID[index] = leader.UID
			originalGroup[index] = podGroupName(specG(), leader)
		}

		testing.UpdateWorkerTemplate(ctx, k8sClient, lws)

		gomega.Eventually(func(g gomega.Gomega) {
			byIndex := podsByGroupIndex(listLiveLWSPods(g, lws))
			g.Expect(byIndex).To(gomega.HaveKey("2"))
			for _, index := range []string{"0", "1"} {
				leader := leaderFrom(g, byIndex[index])
				g.Expect(leader.UID).To(gomega.Equal(originalUID[index]))
				g.Expect(podIsReady(leader)).To(gomega.BeTrue())
				g.Expect(podGroupName(g, leader)).To(gomega.Equal(originalGroup[index]))
				expectReplicaGang(g, byIndex[index], 2, leader.Labels[leaderworkerset.RevisionKey], originalGroup[index])
			}
			surgeLeader := leaderFrom(g, byIndex["2"])
			g.Expect(podIsReady(surgeLeader)).To(gomega.BeTrue())
			g.Expect(surgeLeader.Spec.SchedulingGates).To(gomega.BeEmpty())
			g.Expect(podGroupName(g, surgeLeader)).NotTo(gomega.Equal(originalGroup["0"]))
			g.Expect(podGroupName(g, surgeLeader)).NotTo(gomega.Equal(originalGroup["1"]))
			expectReplicaGang(g, byIndex["2"], 2, surgeLeader.Labels[leaderworkerset.RevisionKey], podGroupName(g, surgeLeader))
		}, wasRolloutTimeout, interval).Should(gomega.Succeed())

		testing.ExpectValidLeaderStatefulSet(ctx, k8sClient, lws, 2)
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, 4, 2, 2)
		testing.ExpectLeaderWorkerSetAvailable(ctx, k8sClient, lws, "All replicas are ready")

		finalLeaders := podsByGroupIndex(listLiveLWSPods(specG(), lws))
		gomega.Expect(finalLeaders).NotTo(gomega.HaveKey("2"))
		for _, index := range []string{"0", "1"} {
			leader := leaderFrom(specG(), finalLeaders[index])
			gomega.Expect(leader.UID).NotTo(gomega.Equal(originalUID[index]))
			gomega.Expect(podGroupName(specG(), leader)).NotTo(gomega.Equal(originalGroup[index]))
		}
	})

	ginkgo.It("Should place a gang in one topology domain and hold it until a domain can fit", func() {
		schedulable := readySchedulableNodes()
		domains := topologyDomains(schedulable)
		gomega.Expect(domains).To(gomega.HaveLen(2), "kind-was.yaml must label schedulable nodes with %s in two domains, got %v", wasTopologyKey, domainSizes(domains))
		largest := 0
		for _, nodes := range domains {
			if len(nodes) > largest {
				largest = len(nodes)
			}
		}
		groupSize := len(schedulable)
		gomega.Expect(groupSize).To(gomega.BeNumerically(">", largest), "one topology domain already has room for every schedulable node")
		gomega.Expect(groupSize).To(gomega.BeNumerically(">=", 2))

		resourceName := corev1.ResourceName("example.com/" + ns.Name)
		for _, node := range schedulable {
			setNodeExtendedResource(node.Name, resourceName, "1")
		}
		ginkgo.DeferCleanup(func() {
			for _, node := range schedulable {
				setNodeExtendedResource(node.Name, resourceName, "")
			}
		})
		originalLabels := map[string]string{}
		for _, node := range schedulable {
			originalLabels[node.Name] = node.Labels[wasTopologyKey]
		}
		ginkgo.DeferCleanup(func() {
			for name, value := range originalLabels {
				setNodeLabel(name, wasTopologyKey, value)
			}
		})

		lws.Spec.Replicas = ptr.To[int32](1)
		lws.Spec.LeaderWorkerTemplate.Size = ptr.To(int32(groupSize))
		lws.Spec.Scheduling.Replica = &leaderworkerset.LeaderWorkerSetReplicaScheduling{
			SchedulingConstraints: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints{
				Topology: []schedulingv1alpha3.TopologyConstraint{{Key: wasTopologyKey}},
			},
		}
		spread := map[string]string{"lws-e2e-spread": ns.Name}
		requireOnePodPerNode(lws, resourceName, spread)

		ginkgo.By(fmt.Sprintf("creating a %d-pod gang, larger than every current rack", groupSize))
		testing.MustCreateLws(ctx, k8sClient, lws)

		gomega.Eventually(func(g gomega.Gomega) {
			groups := listLWSPodGroups(g, lws)
			g.Expect(groups).To(gomega.HaveLen(1))
			g.Expect(groups[0].Spec.SchedulingPolicy.Gang).NotTo(gomega.BeNil())
			g.Expect(groups[0].Spec.SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(groupSize)))
			g.Expect(groups[0].Spec.SchedulingConstraints).NotTo(gomega.BeNil())
			g.Expect(groups[0].Spec.SchedulingConstraints.Topology).To(gomega.ConsistOf(schedulingv1beta1.TopologyConstraint{Key: wasTopologyKey}))
		}, timeout, interval).Should(gomega.Succeed())

		assertPending := func(g gomega.Gomega) {
			pods := listLiveLWSPods(g, lws)
			g.Expect(pods).To(gomega.HaveLen(groupSize))
			for _, pod := range pods {
				g.Expect(pod.Spec.SchedulingGroup).NotTo(gomega.BeNil())
				g.Expect(pod.Spec.NodeName).To(gomega.BeEmpty())
				g.Expect(pod.Status.Phase).To(gomega.Equal(corev1.PodPending))
			}
		}
		gomega.Eventually(assertPending, timeout, interval).Should(gomega.Succeed())
		gomega.Consistently(assertPending, 10*time.Second, interval).Should(gomega.Succeed())

		ginkgo.By("moving every schedulable node into one rack")
		for _, node := range schedulable {
			setNodeLabel(node.Name, wasTopologyKey, wasTopologyRackA)
		}
		testing.ExpectWASPodGroupsScheduled(ctx, k8sClient, lws, groupSize, int32(groupSize))

		pods := listLiveLWSPods(specG(), lws)
		gomega.Expect(pods).To(gomega.HaveLen(groupSize))
		nodesUsed := map[string]struct{}{}
		for _, pod := range pods {
			node := &corev1.Node{}
			gomega.Expect(k8sClient.Get(ctx, types.NamespacedName{Name: pod.Spec.NodeName}, node)).To(gomega.Succeed())
			gomega.Expect(node.Labels[wasTopologyKey]).To(gomega.Equal(wasTopologyRackA))
			nodesUsed[pod.Spec.NodeName] = struct{}{}
		}
		gomega.Expect(nodesUsed).To(gomega.HaveLen(groupSize))
	})

	ginkgo.It("Should reject gang scheduling combined with the exclusive-topology annotation", func() {
		lws.Annotations = map[string]string{
			leaderworkerset.ExclusiveKeyAnnotationKey: "kubernetes.io/hostname",
		}
		expectSchedulingRejected(lws)
	})

	ginkgo.It("Should reject topology constraints combined with the exclusive-topology annotation", func() {
		lws.Annotations = map[string]string{
			leaderworkerset.ExclusiveKeyAnnotationKey: "kubernetes.io/hostname",
		}
		// Basic policy keeps the rejection on the topology constraint. An empty
		// replica policy would already be a gang.
		lws.Spec.Scheduling = &leaderworkerset.LeaderWorkerSetScheduling{
			Replica: &leaderworkerset.LeaderWorkerSetReplicaScheduling{
				SchedulingPolicy: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingPolicy{
					Basic: &schedulingv1alpha3.WorkloadCompositePodGroupBasicSchedulingPolicy{},
				},
				SchedulingConstraints: &schedulingv1alpha3.WorkloadCompositePodGroupSchedulingConstraints{
					Topology: []schedulingv1alpha3.TopologyConstraint{{Key: wasTopologyKey}},
				},
			},
		}
		expectSchedulingRejected(lws)
	})
})

func specG() gomega.Gomega {
	return gomega.NewWithT(ginkgo.GinkgoT())
}

func listLiveLWSPods(g gomega.Gomega, lws *leaderworkerset.LeaderWorkerSet) []corev1.Pod {
	pods := &corev1.PodList{}
	g.Expect(k8sClient.List(ctx, pods, client.InNamespace(lws.Namespace), client.MatchingLabels{
		leaderworkerset.SetNameLabelKey: lws.Name,
	})).To(gomega.Succeed())
	live := make([]corev1.Pod, 0, len(pods.Items))
	for _, pod := range pods.Items {
		if pod.DeletionTimestamp == nil {
			live = append(live, pod)
		}
	}
	return live
}

func listLWSPodGroups(g gomega.Gomega, lws *leaderworkerset.LeaderWorkerSet) []schedulingv1beta1.PodGroup {
	groups := &schedulingv1beta1.PodGroupList{}
	g.Expect(k8sClient.List(ctx, groups, client.InNamespace(lws.Namespace), client.MatchingLabels{
		leaderworkerset.SetNameLabelKey: lws.Name,
	})).To(gomega.Succeed())
	live := make([]schedulingv1beta1.PodGroup, 0, len(groups.Items))
	for _, group := range groups.Items {
		if group.DeletionTimestamp == nil {
			live = append(live, group)
		}
	}
	return live
}

func podsByGroupIndex(pods []corev1.Pod) map[string][]corev1.Pod {
	byIndex := make(map[string][]corev1.Pod, len(pods))
	for _, pod := range pods {
		index := pod.Labels[leaderworkerset.GroupIndexLabelKey]
		byIndex[index] = append(byIndex[index], pod)
	}
	return byIndex
}

func leaderFrom(g gomega.Gomega, pods []corev1.Pod) corev1.Pod {
	for _, pod := range pods {
		if pod.Labels[leaderworkerset.WorkerIndexLabelKey] == "0" {
			return pod
		}
	}
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	g.Expect(fmt.Errorf("group pods %v have no leader", names)).NotTo(gomega.HaveOccurred())
	return corev1.Pod{}
}

func podGroupName(g gomega.Gomega, pod corev1.Pod) string {
	g.Expect(pod.Spec.SchedulingGroup).NotTo(gomega.BeNil())
	g.Expect(pod.Spec.SchedulingGroup.PodGroupName).NotTo(gomega.BeNil())
	return *pod.Spec.SchedulingGroup.PodGroupName
}

func podIsReady(pod corev1.Pod) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == corev1.PodReady {
			return condition.Status == corev1.ConditionTrue
		}
	}
	return false
}

func expectReplicaGang(g gomega.Gomega, pods []corev1.Pod, size int, revision, groupName string) {
	g.Expect(pods).To(gomega.HaveLen(size))
	var shared string
	for _, pod := range pods {
		g.Expect(pod.Annotations[leaderworkerset.SizeAnnotationKey]).To(gomega.Equal(strconv.Itoa(size)))
		if revision != "" {
			g.Expect(pod.Labels[leaderworkerset.RevisionKey]).To(gomega.Equal(revision))
		}
		g.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty())
		g.Expect(podIsReady(pod)).To(gomega.BeTrue())
		g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
		name := podGroupName(g, pod)
		if shared == "" {
			shared = name
		}
		g.Expect(name).To(gomega.Equal(shared))
	}
	if groupName != "" {
		g.Expect(shared).To(gomega.Equal(groupName))
	}
	pg := &schedulingv1beta1.PodGroup{}
	g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pods[0].Namespace, Name: shared}, pg)).To(gomega.Succeed())
	g.Expect(pg.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
	g.Expect(pg.Spec.SchedulingPolicy.Gang).NotTo(gomega.BeNil())
	g.Expect(pg.Spec.SchedulingPolicy.Gang.MinCount).To(gomega.Equal(int32(size)))
	g.Expect(apimeta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1beta1.PodGroupInitiallyScheduled)).To(gomega.BeTrue())
}

func expectSharedGang(g gomega.Gomega, pods []corev1.Pod, minCount int32) {
	g.Expect(pods).NotTo(gomega.BeEmpty())
	var shared string
	for _, pod := range pods {
		g.Expect(pod.Spec.NodeName).NotTo(gomega.BeEmpty())
		g.Expect(podIsReady(pod)).To(gomega.BeTrue())
		g.Expect(pod.Spec.SchedulingGates).To(gomega.BeEmpty())
		name := podGroupName(g, pod)
		if shared == "" {
			shared = name
		}
		g.Expect(name).To(gomega.Equal(shared))
	}
	pg := &schedulingv1beta1.PodGroup{}
	g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pods[0].Namespace, Name: shared}, pg)).To(gomega.Succeed())
	g.Expect(pg.Spec.SchedulingPolicy.Gang).NotTo(gomega.BeNil())
	g.Expect(pg.Spec.SchedulingPolicy.Gang.MinCount).To(gomega.Equal(minCount))
	g.Expect(apimeta.IsStatusConditionTrue(pg.Status.Conditions, schedulingv1beta1.PodGroupInitiallyScheduled)).To(gomega.BeTrue())
}

func updateRollingPartition(lws *leaderworkerset.LeaderWorkerSet, partition int32) {
	gomega.Eventually(func() error {
		current := &leaderworkerset.LeaderWorkerSet{}
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(lws), current); err != nil {
			return err
		}
		if current.Spec.RolloutStrategy.RollingUpdateConfiguration == nil {
			current.Spec.RolloutStrategy.RollingUpdateConfiguration = &leaderworkerset.RollingUpdateConfiguration{}
		}
		current.Spec.RolloutStrategy.RollingUpdateConfiguration.Partition = ptr.To(partition)
		return k8sClient.Update(ctx, current)
	}, timeout, interval).Should(gomega.Succeed())
}

func expectSchedulingRejected(lws *leaderworkerset.LeaderWorkerSet) {
	err := k8sClient.Create(ctx, lws)
	gomega.Expect(err).To(gomega.HaveOccurred())
	gomega.Expect(err.Error()).To(gomega.ContainSubstring("cannot be combined with exclusive topology"))
}

func readySchedulableNodes() []corev1.Node {
	nodes := &corev1.NodeList{}
	gomega.Expect(k8sClient.List(ctx, nodes)).To(gomega.Succeed())
	ready := make([]corev1.Node, 0, len(nodes.Items))
	for _, node := range nodes.Items {
		if node.Spec.Unschedulable || nodeHasBlockingTaint(node) {
			continue
		}
		for _, condition := range node.Status.Conditions {
			if condition.Type == corev1.NodeReady && condition.Status == corev1.ConditionTrue {
				ready = append(ready, node)
				break
			}
		}
	}
	return ready
}

func nodeHasBlockingTaint(node corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Effect == corev1.TaintEffectNoSchedule || taint.Effect == corev1.TaintEffectNoExecute {
			return true
		}
	}
	return false
}

func topologyDomains(nodes []corev1.Node) map[string][]corev1.Node {
	domains := make(map[string][]corev1.Node, len(nodes))
	for _, node := range nodes {
		value := node.Labels[wasTopologyKey]
		gomega.Expect(value).NotTo(gomega.BeEmpty(), "node %s is missing label %s", node.Name, wasTopologyKey)
		domains[value] = append(domains[value], node)
	}
	return domains
}

func domainSizes(domains map[string][]corev1.Node) map[string]int {
	sizes := make(map[string]int, len(domains))
	for value, nodes := range domains {
		sizes[value] = len(nodes)
	}
	return sizes
}

func setNodeLabel(name, key, value string) {
	gomega.Eventually(func() error {
		node := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
			return err
		}
		original := node.DeepCopy()
		if node.Labels == nil {
			node.Labels = map[string]string{}
		}
		node.Labels[key] = value
		return k8sClient.Patch(ctx, node, client.MergeFrom(original))
	}, timeout, interval).Should(gomega.Succeed())
}

func setNodeExtendedResource(name string, resourceName corev1.ResourceName, quantity string) {
	gomega.Eventually(func() error {
		node := &corev1.Node{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, node); err != nil {
			return err
		}
		original := node.DeepCopy()
		if quantity == "" {
			delete(node.Status.Capacity, resourceName)
			delete(node.Status.Allocatable, resourceName)
		} else {
			if node.Status.Capacity == nil {
				node.Status.Capacity = corev1.ResourceList{}
			}
			if node.Status.Allocatable == nil {
				node.Status.Allocatable = corev1.ResourceList{}
			}
			parsed := resource.MustParse(quantity)
			node.Status.Capacity[resourceName] = parsed
			node.Status.Allocatable[resourceName] = parsed
		}
		return k8sClient.Status().Patch(ctx, node, client.MergeFrom(original))
	}, timeout, interval).Should(gomega.Succeed())
}

func requireOnePodPerNode(lws *leaderworkerset.LeaderWorkerSet, resourceName corev1.ResourceName, spread map[string]string) {
	resources := corev1.ResourceRequirements{
		Requests: corev1.ResourceList{resourceName: resource.MustParse("1")},
		Limits:   corev1.ResourceList{resourceName: resource.MustParse("1")},
	}
	affinity := &corev1.Affinity{
		PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
				LabelSelector: &metav1.LabelSelector{MatchLabels: spread},
				TopologyKey:   corev1.LabelHostname,
			}},
		},
	}
	lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Labels = spread
	lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Affinity = affinity
	lws.Spec.LeaderWorkerTemplate.LeaderTemplate.Spec.Containers[0].Resources = resources
	lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Labels = spread
	lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Affinity = affinity
	lws.Spec.LeaderWorkerTemplate.WorkerTemplate.Spec.Containers[0].Resources = resources
}
