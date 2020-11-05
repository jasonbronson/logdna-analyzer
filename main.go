package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
)

func main() {
	//file must be uncompressed
	fileFl := flag.String("file", "./export-logdna.jsonl", "a file import")
	outFl := flag.String("out", "./output.txt", "a file output")
	featureFl := flag.String("feature", "count_hit_request", "a feature handle")
	switch *featureFl {
	case "count_hit_request":
		count_hit_request(*fileFl, *outFl)
		break
	default:
		break
	}
}

func count_hit_request(filePath string, outputPath string) {
	listData := readFileByLine(filePath)
	output := &DataOutputHitRequest{
		Path:   make(map[string]int, 0),
		Hours:  make(map[string]int, 0),
		Minute: make(map[string]int, 0),
		Status: make(map[string]int, 0),
		Ignore: make(map[string]int, 0),
	}
	for _, data := range listData {
		obj := parseDataHitRequset(data)
		handleLineHitRequest(output, obj.Line)
	}
	writeOutputHitRequestToFile(output, outputPath)
}

func writeOutputHitRequestToFile(data *DataOutputHitRequest, path string) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	datawriter := bufio.NewWriter(file)
	datawriter.WriteString("#Data count of " + data.Date + "\n")
	datawriter.WriteString("#Status:\n")
	listStatus := make([]string, 0)
	for key := range data.Status {
		listStatus = append(listStatus, key)
	}
	sort.Strings(listStatus)
	for _, key := range listStatus {
		value := data.Status[key]
		datawriter.WriteString("- Status " + key + ": " + strconv.Itoa(value) + " hits\n")
	}
	datawriter.WriteString("#Hours:\n")
	for key, value := range data.Hours {
		hour := strings.ReplaceAll(key, data.Date+"-", "")
		h, _ := strconv.Atoi(hour)
		if h > 12 {
			hour = strconv.Itoa(h-12) + " pm"
		} else {
			hour = strconv.Itoa(h) + " am"
		}
		datawriter.WriteString("- " + hour + ": " + strconv.Itoa(value) + " hits\n")
	}
	datawriter.WriteString("#Minute:\n")
	totalHitMin := 0
	for _, value := range data.Minute {
		totalHitMin = totalHitMin + value
	}
	datawriter.WriteString("- " + strconv.Itoa(totalHitMin/len(data.Minute)) + " hits avg per minute\n")
	datawriter.WriteString("#Top 20 Path:\n")
	listTopPath := sortMapByValue(data.Path)
	for index, objPath := range listTopPath {
		if index == 20 {
			break
		}
		datawriter.WriteString("- " + objPath.Key + ": " + strconv.Itoa(objPath.Value) + " hits\n")
	}

	datawriter.WriteString("====================\n")

	datawriter.Flush()
	file.Close()
}

func handleLineHitRequest(output *DataOutputHitRequest, line string) {
	if strings.Contains(line, "cache") || strings.HasPrefix(line, " DEBUG:") || strings.Contains(line, "TLS handshake") || strings.Contains(line, "+0000 GMT") {
		if val, ok := output.Ignore[line]; ok {
			output.Ignore[line] = val + 1
		} else {
			output.Ignore[line] = 1
		}
		return
	}
	if strings.Contains(line, "event#exception") {
		//ignore bugsnag exceptions
		return
	}
	if strings.Contains(line, "error code") {
		split := strings.Split(line, " ")
		date := split[0]
		time := split[1]
		splitTime := strings.Split(time, ":")
		status := split[4][0 : len(split[4])-1]
		if val, ok := output.Hours[date+"-"+splitTime[0]]; ok {
			output.Hours[date+"-"+splitTime[0]] = val + 1
		} else {
			output.Hours[date+"-"+splitTime[0]] = 1
		}
		if val, ok := output.Minute[date+"-"+splitTime[1]]; ok {
			output.Minute[date+"-"+splitTime[1]] = val + 1
		} else {
			output.Minute[date+"-"+splitTime[1]] = 1
		}
		if val, ok := output.Status[status]; ok {
			output.Status[status] = val + 1
		} else {
			output.Status[status] = 1
		}
		return
	}

	split := strings.Split(line, " ")
	if len(split) < 10 {
		if val, ok := output.Ignore[line]; ok {
			output.Ignore[line] = val + 1
		} else {
			output.Ignore[line] = 1
		}
		return
	}
	date := split[0]
	output.Date = date
	time := split[1]
	splitTime := strings.Split(time, ":")
	method := split[3][1:]
	path := split[4]
	protocol := split[5][:len(split[5])-1]
	if !strings.Contains(protocol, "HTTP") {
		return
	}
	status := split[9]
	if val, ok := output.Path[method+" "+path]; ok {
		output.Path[method+" "+path] = val + 1
	} else {
		output.Path[method+" "+path] = 1
	}
	if val, ok := output.Hours[date+"-"+splitTime[0]]; ok {
		output.Hours[date+"-"+splitTime[0]] = val + 1
	} else {
		output.Hours[date+"-"+splitTime[0]] = 1
	}
	if val, ok := output.Minute[date+"-"+splitTime[0]+"-"+splitTime[1]]; ok {
		output.Minute[date+"-"+splitTime[0]+"-"+splitTime[1]] = val + 1
	} else {
		output.Minute[date+"-"+splitTime[0]+"-"+splitTime[1]] = 1
	}
	if val, ok := output.Status[status]; ok {
		output.Status[status] = val + 1
	} else {
		output.Status[status] = 1
	}
}

func parseDataHitRequset(data string) DataInputHitRequest {
	var obj DataInputHitRequest
	json.Unmarshal([]byte(data), &obj)
	return obj
}

func readFileByLine(filePath string) []string {
	data := make([]string, 0)
	file, err := os.Open(filePath)

	if err != nil {
		log.Fatalf("failed to open")
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		data = append(data, scanner.Text())
	}
	return data
}

func sortMapByValue(input map[string]int) []KV {

	var ss []KV
	for k, v := range input {
		ss = append(ss, KV{k, v})
	}

	sort.Slice(ss, func(i, j int) bool {
		return ss[i].Value > ss[j].Value
	})
	return ss
}

type KV struct {
	Key   string
	Value int
}

type DataInputHitRequest struct {
	Account     string          `json:"_account"`
	Cluster     string          `json:"_cluster"`
	Host        string          `json:"_host"`
	Ingester    string          `json:"_ingester"`
	Label       LabelHitRequest `json:"_label"`
	LogType     string          `json:"_logtype"`
	File        string          `json:"_file"`
	Line        string          `json:"_line"`
	TS          int64           `json:"_ts"`
	App         string          `json:"_app"`
	Pod         string          `json:"pod"`
	NameSpace   string          `json:"namespace"`
	Container   string          `json:"container"`
	ContainerID string          `json:"containerid"`
	Node        string          `json:"node"`
	Key         string          `json:"__key"`
	Bid         string          `json:"_bid"`
	Level       string          `json:"level"`
	ID          string          `json:"_id"`
}

type LabelHitRequest struct {
	TemplateHash string `json:"pod-template-hash"`
	App          string `json:"app"`
}

type DataOutputHitRequest struct {
	Date   string
	Status map[string]int
	Hours  map[string]int
	Minute map[string]int
	Path   map[string]int
	Ignore map[string]int
}
