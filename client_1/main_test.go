package main

import (
	"fmt"
	"testing"
)

func TestRemoveDup(t *testing.T) {
	ps := []Price{
		Price{1.2, "BTCETH", 123},
		Price{1.3, "BTCETH", 125},
		Price{1.3, "BTCETH", 125},
		Price{1.5, "BTCETH", 128},
	}
	fmt.Println(removeDup(ps))
}
