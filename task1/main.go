package main

import (
	"fmt"
	"github.com/vistarmedia/gossamr"
	"log"
	"sort"
	"strconv"
	"strings"
)

type inter struct {
}

func (in *inter) Map(p int64, line string, c gossamr.Collector) error {
	fields := strings.Fields(line) // 0: word, 1: year, 2: total count
	key, _ := strconv.Atoi(fields[1][0:3])
	if key >= 180 {
		c.Collect(int64(key*10), fields[0]+"#"+fields[2])
	}
	return nil
}

func (in *inter) Reduce(key int64, inputs chan string, c gossamr.Collector) error {
	// Make a map with the word as key and the sum of words as value
	values := make(map[string]int64)
	for input := range inputs {
		fields := strings.Split(input, "#") //0 word, 1 total count
		if _, ok := values[fields[0]]; ok {
			count, _ := strconv.Atoi(fields[1])
			values[fields[0]] += int64(count) // if the key exist sum
		} else {
			count, _ := strconv.Atoi(fields[1])
			values[fields[0]] = int64(count) // if the key does not exist cretes it
		}
	}

	// Sort by key (word)
	keys := make([]string, 0, len(values))
	for k := range values {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Get the key with the highest value
	var max int64 = 0
	var maxIndex string
	for _, key := range keys {
		if values[key] > max {
			max = values[key]
			maxIndex = key
		}
	}
	c.Collect(key, fmt.Sprintf("%s %d", maxIndex, max))
	return nil
}

func main() {
	task1 := gossamr.NewTask(&inter{})

	err := gossamr.Run(task1)
	if err != nil {
		log.Fatal(err)
	}
}
