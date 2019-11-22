package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"unicode"

	hdfs "github.com/colinmarc/hdfs"
	log "github.com/sirupsen/logrus"
	flag "github.com/spf13/pflag"
	viper "github.com/spf13/viper"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

var (
	env string
)

type jobConf struct {
	bin     string
	path    string
	inputs  []string
	output  string
	mapper  string
	reducer string
	args    map[string]string
}

func init() {
	viper.SetConfigName("conf") // name of config file (without extension)
	viper.AddConfigPath(".")    // optionally look for config in the working directory
	err := viper.ReadInConfig() // Find and read the config file
	if err != nil {             // Handle errors reading the config file
		log.Errorf("Fatal error config file: %s \n", err)
	}
	flag.StringVarP(&env, "env", "e", "local", "Environment")
	flag.Parse()
}

func main() {
	// TASK 1
	var task1 = jobConf{
		bin:     viper.GetString(env + ".bin"),
		path:    viper.GetString(env + ".path"),
		output:  viper.GetString(env + ".task1.output"),
		mapper:  viper.GetString(env + ".task1.mapper"),
		reducer: viper.GetString(env + ".task1.reducer"),
	}
	task1.inputs = append(task1.inputs, viper.GetString(env+".task1.input"))
	task1.args = make(map[string]string)
	task1.args["io"] = "typedbytes"
	task1.Run()
	// TASK 2

	var scanner *bufio.Scanner
	hdfsClient := viper.GetString(env + ".hdfsClient")
	if hdfsClient != "" {
		client, _ := hdfs.New(hdfsClient)
		outTask1, err := client.Open(task1.output + "/part-00000")
		if err != nil {
			log.Errorf("Cannot open file: %s", err)
		}
		defer outTask1.Close()
		scanner = bufio.NewScanner(outTask1)
	} else {
		outTask1, err := os.Open(task1.output + "/part-00000")
		if err != nil {
			log.Errorf("Cannot open file: %s", err)
		}
		defer outTask1.Close()
		scanner = bufio.NewScanner(outTask1)
	}
	var task2 = jobConf{
		bin:  viper.GetString(env + ".bin"),
		path: viper.GetString(env + ".path"),
		// input:   fmt.Sprintf("%s%s", viper.GetString(env+".task2.input"), bigram),
		output:  viper.GetString(env + ".task2.output"),
		reducer: viper.GetString(env + ".task2.reducer"),
	}
	task2.args = make(map[string]string)
	task2.args["io"] = "typedbytes"

	var (
		bigram     string
		mapperArgs string
	)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		// Eliminar acentos
		t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
		word, _, _ := transform.String(t, fields[1])

		if len(word) == 1 {
			bigram = word + "_"
		} else {
			bigram = word[0:2]
		}
		_, found := findString(task2.inputs, fmt.Sprintf("%s%s", viper.GetString(env+".task2.input"), bigram))
		if !found {
			task2.inputs = append(task2.inputs, fmt.Sprintf("%s%s", viper.GetString(env+".task2.input"), bigram))
		}
		mapperArgs = mapperArgs + "#" + fields[0] + ":" + fields[1]
	}
	task2.mapper = fmt.Sprintf("%s -m %s", viper.GetString(env+".task2.mapper"), mapperArgs)
	task2.Run()

	// if len(char) > 1 {
	// 	log.Errorf("Char arg must have length one")
	// 	os.Exit(0)
	// }
	// if aws {
	// 	job = jobConf{
	// 		bin:    "bin/mapred",
	// 		path:   "/opt/hadoop/hadoop-3.2.1",
	// 		input:  "/user/hadoop/",
	// 		output: "/user/hadoop/output.task1",
	// 	}
	// 	fmt.Println(char)
	// } else {
	// 	job = jobConf{
	// 		bin:    "bin/mapred",
	// 		path:   "/opt/hadoop/hadoop-3.2.1",
	// 		input:  "/opt/hadoop/shared/googlebooks-eng-all-1gram-20120701-a",
	// 		output: "/home/jose/universidad/ta/p2/output/task1",
	// 	}
	// 	fmt.Println(char)
	// }
	// // TASK 1
	// job.mapper = `"/home/jose/universidad/ta/p2/task1/main -task 0 -phase map"`
	// job.reducer = `"/home/jose/universidad/ta/p2/task1/main -task 0 -phase reduce"`
	// job.args = make(map[string]string)
	// job.args["io"] = "typedbytes"
	// job.Run() // ACUERDATE DE DESCOMENTARME CAPULLO

	// // TASK 2
	// client, _ := hdfs.New("localhost:9000")
	// outTask1, err := client.Open(job.output + "/part-00000")
	// if err != nil {
	// 	log.Errorf("Cannot open file: %s", err)
	// }
	// defer outTask1.Close()

	// scanner := bufio.NewScanner(outTask1)
	// for scanner.Scan() {
	// 	var bigram string
	// 	fields := strings.Fields(scanner.Text())
	// 	if len(strings.Split(fields[1], "_")[0]) == 1 {
	// 		bigram = strings.Split(fields[1], "_")[0] + "_"
	// 	} else {
	// 		bigram = strings.Split(fields[1], "_")[0][0:2]
	// 	}
	// 	// job.mapper = fmt.Sprintf(`"/home/jose/universidad/ta/p2/task2/main -task 0 -phase map"`)
	// 	job.mapper = fmt.Sprintf(`"/home/jose/universidad/ta/p2/task2/main -task 0 -phase map -d %s -w %s"`, fields[0], fields[1])
	// 	// job.reducer = fmt.Sprintf(`"/home/jose/universidad/ta/p2/task2/main -task 0 -phase reduce"`)
	// 	job.reducer = fmt.Sprintf(`"/home/jose/universidad/ta/p2/task2/main -task 0 -phase reduce -d %s -w %s"`, fields[0], fields[1])
	// 	job.input = fmt.Sprintf("/opt/hadoop/shared/googlebooks-eng-all-1gram-20120701-%s", bigram)
	// 	job.output = fmt.Sprintf("/home/jose/universidad/ta/p2/output/task2/%s", fields[0])
	// 	job.Run()
	// }
}

func (job jobConf) Run() error {
	args := []string{
		"streaming",
		"-output", job.output,
		"-mapper", job.mapper,
		"-reducer", job.reducer,
	}
	for _, key := range reflect.ValueOf(job.args).MapKeys() {
		args = append(args, fmt.Sprintf("-%s", key.String()))
		args = append(args, job.args[key.String()])
	}
	for _, input := range job.inputs {
		args = append(args, "-input")
		args = append(args, input)
	}
	cmd := exec.Command(
		job.bin,
		args...,
	)
	cmd.Dir = job.path
	log.Infof("Executing command:\n%s\n", cmd.Args)
	var out bytes.Buffer
	var e bytes.Buffer
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	_ = cmd.Run()
	fmt.Printf("%s", e.String())
	fmt.Printf("%s", out.String())
	return nil
}
func findString(slice []string, val string) (int, bool) {
	for i, item := range slice {
		if item == val {
			return i, true
		}
	}
	return -1, false
}
