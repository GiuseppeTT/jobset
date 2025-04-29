package orchestrator

import (
	"k8s.io/klog/v2"
)

func partitionSets(a []string, b []string) ([]string, []string, []string) {
	klog.V(2).Info("Partitioning sets")
	setA := make(map[string]struct{}, len(a))
	for _, item := range a {
		setA[item] = struct{}{}
	}
	setB := make(map[string]struct{}, len(b))
	for _, item := range b {
		setB[item] = struct{}{}
	}
	onlyInA := []string{}
	intersection := []string{}
	onlyInB := []string{}
	for item := range setA {
		if _, foundInB := setB[item]; !foundInB {
			onlyInA = append(onlyInA, item)
		}
	}
	for item := range setB {
		if _, foundInA := setA[item]; !foundInA {
			onlyInB = append(onlyInB, item)
		}
	}
	for item := range setA {
		if _, foundInB := setB[item]; foundInB {
			intersection = append(intersection, item)
		}
	}
	return onlyInA, intersection, onlyInB
}
