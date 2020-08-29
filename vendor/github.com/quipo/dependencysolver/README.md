# Dependency Resolver (Golang)

[![Build Status](https://travis-ci.org/quipo/dependencysolver.png?branch=master)](https://travis-ci.org/quipo/dependencysolver) 
[![GoDoc](https://godoc.org/github.com/quipo/dependencysolver?status.png)](http://godoc.org/github.com/quipo/dependencysolver)

## Introduction

Layer-based scheduling algorithm for parallel tasks with dependencies.
Determines which tasks can be executed in parallel, by evaluating dependencies.

Given a list of entries (each with its own dependency list), it can sort them in layers of execution, 
where all entries in the same layer can be executed in parallel, and have no other dependency than the previous layer.

For instance, given entries A, B, C, D, where B and C depend on A, and D depends on B and C, this function will return three layers of execution (as B and C can be executed in parallel after A completes):

```
Dependency tree:

   A
  / \
 B   C
  \ /
   D

Resulting execution layers:

---------------------
Layer 1:       A
---------------------
Layer 2:     B   C
---------------------
Layer 3:       D
---------------------
```

## Installation

    go get github.com/quipo/dependencysolver

## Sample usage

```go
import (
	"github.com/quipo/dependencysolver"
)

type Operation struct {
	ID   string,
	Deps []string,
	// some other properties of the operation	
}


func SortByDependency(operations []Operation) (layers [][]string) {
	entries := make([]dependencysolver.Entry, 0)
	for _, op := range operations {
		entries = append(entries, dependencysolver.Entry{ID: op.ID, Deps: op.Deps})
	}
	return dependencysolver.LayeredTopologicalSort(entries)
}
```

## Credits

This package follows an algorithm described (albeit incorrectly implemented) here: http://msdn.microsoft.com/en-us/magazine/dd569760.aspx

Other interesting articles on the topic:

* http://en.wikipedia.org/wiki/Topological_sorting
* http://www.tu-chemnitz.de/informatik/PI/forschung/projekte/genMTS/index.php.en
* http://www.tu-chemnitz.de/informatik/PI/forschung/pub/download/DKR_parco07.pdf
* http://ertl.jp/~shinpei/papers/rtss10.pdf
* http://www.dur.ac.uk/pedro.gonnet/?page_id=6
* http://www.electricmonk.nl/log/2008/08/07/dependency-resolving-algorithm/
* http://ieeexplore.ieee.org/iel5/5538704/5547051/05547145.pdf?arnumber=5547145
* http://www.ier-institute.org/2070-1918/lnit25/lnit%20v25/141.pdf


## Copyright

See [LICENSE](LICENSE) document
