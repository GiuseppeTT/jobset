/*
Copyright 2023 The Kubernetes Authors.
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
// Code generated by lister-gen. DO NOT EDIT.

package v1alpha2

import (
	labels "k8s.io/apimachinery/pkg/labels"
	listers "k8s.io/client-go/listers"
	cache "k8s.io/client-go/tools/cache"
	jobsetv1alpha2 "sigs.k8s.io/jobset/api/jobset/v1alpha2"
)

// JobSetLister helps list JobSets.
// All objects returned here must be treated as read-only.
type JobSetLister interface {
	// List lists all JobSets in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*jobsetv1alpha2.JobSet, err error)
	// JobSets returns an object that can list and get JobSets.
	JobSets(namespace string) JobSetNamespaceLister
	JobSetListerExpansion
}

// jobSetLister implements the JobSetLister interface.
type jobSetLister struct {
	listers.ResourceIndexer[*jobsetv1alpha2.JobSet]
}

// NewJobSetLister returns a new JobSetLister.
func NewJobSetLister(indexer cache.Indexer) JobSetLister {
	return &jobSetLister{listers.New[*jobsetv1alpha2.JobSet](indexer, jobsetv1alpha2.Resource("jobset"))}
}

// JobSets returns an object that can list and get JobSets.
func (s *jobSetLister) JobSets(namespace string) JobSetNamespaceLister {
	return jobSetNamespaceLister{listers.NewNamespaced[*jobsetv1alpha2.JobSet](s.ResourceIndexer, namespace)}
}

// JobSetNamespaceLister helps list and get JobSets.
// All objects returned here must be treated as read-only.
type JobSetNamespaceLister interface {
	// List lists all JobSets in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*jobsetv1alpha2.JobSet, err error)
	// Get retrieves the JobSet from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*jobsetv1alpha2.JobSet, error)
	JobSetNamespaceListerExpansion
}

// jobSetNamespaceLister implements the JobSetNamespaceLister
// interface.
type jobSetNamespaceLister struct {
	listers.ResourceIndexer[*jobsetv1alpha2.JobSet]
}
