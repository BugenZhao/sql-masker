package main

import (
	"fmt"
	"sort"

	"github.com/BugenZhao/sql-masker/mask"
)

type ListOption struct {
}

func (o *ListOption) Run() error {
	names := make([]string, 0, len(mask.MaskFuncMap))
	for k := range mask.MaskFuncMap {
		names = append(names, k)
	}
	sort.Strings(names)

	fmt.Println("All avaliable mask functions:")
	for _, name := range names {
		fmt.Printf("%s:\n\t%s\n", name, mask.MaskFuncMap[name].Description)
	}

	return nil
}
