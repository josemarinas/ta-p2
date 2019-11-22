package main

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"

	flag "github.com/spf13/pflag"
	"github.com/vistarmedia/gossamr"
)

type inter struct {
}

var (
	decade     int64
	resutltMap string
	outTask1   map[int64]string
)

func init() {
	var flagSet flag.FlagSet
	flagSet.ParseErrorsWhitelist.UnknownFlags = true
	flagSet.StringVarP(&resutltMap, "map", "m", "", "Map of years and words from task 1")
	flagSet.Parse(os.Args[5:])
	os.Args = os.Args[0:5]
	// Parse task 1 output
	outTask1 = make(map[int64]string)
	resultArray := strings.Split(resutltMap, "#")
	resultArray = resultArray[1:]
	for _, line := range resultArray {
		decade, _ := strconv.Atoi(strings.Split(line, ":")[0])
		outTask1[int64(decade)] = strings.Split(line, ":")[1]
	}
}

// Map map1
func (in *inter) Map(p int64, line string, c gossamr.Collector) error {
	fields := strings.Fields(line) // 0: word, 1: year, 2: total count
	key, _ := strconv.Atoi(fields[1])
	dec := int64(key / 10)
	if outTask1[dec*10] == strings.Split(fields[0], "_")[0] && dec > 189 {
		c.Collect(dec*10, fields[0]+"#"+fields[2])
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
	task2 := gossamr.NewTask(&inter{})

	err := gossamr.Run(task2)
	if err != nil {
		log.Fatal(err)
	}
}
