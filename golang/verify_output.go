package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"

	"gopkg.in/yaml.v3"
)

const dockerFilePath = "./docker-compose.yaml"

type build struct {
	Context    string `yaml:"context"`
	Dockerfile string `yaml:"dockerfile"`
}

type service struct {
	ContainerName string   `yaml:"container_name"`
	Environment   []string `yaml:"environment"`
	Build         build    `yaml:"build"`
}

type dockerCompose struct {
	Services map[string]service `yaml:"services"`
}

func awaitClientContainers(containerNames []string) error {
	args := append([]string{"container", "wait"}, containerNames...)
	cmd := exec.Command("docker", args...)
	output, err := cmd.Output()
	if err != nil {
		return errors.New("Failed to wait for client containers")
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	zeroCount := 0
	for _, line := range lines {
		if strings.TrimSpace(line) == "0" {
			zeroCount++
		}
	}
	if zeroCount != len(containerNames) {
		return errors.New("One or more clients exited with an error code")
	}
	return nil
}

func findEnvVar(environment []string, target string) string {
	for _, env := range environment {
		parts := strings.SplitN(env, "=", 2)
		if len(parts) == 2 && parts[0] == target {
			return parts[1]
		}
	}
	return ""
}

func buildInputFruitTop(inputFile string) ([]fruititem.FruitItem, error) {
	buildInputFruitTopError := errors.New("Couldn't build input file fruit top")

	f, err := os.Open(inputFile)
	if err != nil {
		return nil, buildInputFruitTopError
	}
	defer f.Close()

	amountByFruit := map[string]fruititem.FruitItem{}
	reader := csv.NewReader(f)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, buildInputFruitTopError
		}
		fruit := record[0]
		amount, err := strconv.ParseUint(record[1], 10, 32)
		if err != nil {
			return nil, buildInputFruitTopError
		}

		fruitRecord := fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)}
		_, ok := (amountByFruit)[fruitRecord.Fruit]
		if ok {
			amountByFruit[fruitRecord.Fruit] = amountByFruit[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			amountByFruit[fruitRecord.Fruit] = fruitRecord
		}
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(amountByFruit))
	for _, item := range amountByFruit {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	return fruitItems, nil
}

func readOutputFruitTop(outputFile string) ([]fruititem.FruitItem, error) {
	readOutputFruitTop := errors.New("Couldn't read output file fruit top")

	f, err := os.Open(outputFile)
	if err != nil {
		return nil, readOutputFruitTop
	}
	defer f.Close()

	var items []fruititem.FruitItem
	reader := csv.NewReader(f)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, readOutputFruitTop
		}
		fruit := record[0]
		amount, err := strconv.ParseUint(record[1], 10, 32)
		if err != nil {
			return nil, readOutputFruitTop
		}
		items = append(items, fruititem.FruitItem{Fruit: fruit, Amount: uint32(amount)})
	}
	return items, nil
}

func verifyClientOutput(topSize int, svc service) error {
	clientName := svc.ContainerName
	log.Println(clientName)

	inputFile := "." + findEnvVar(svc.Environment, "INPUT_FILE")
	outputFile := "." + findEnvVar(svc.Environment, "OUTPUT_FILE")

	if inputFile == "." || outputFile == "." {
		return errors.New("Bad file environment variable config")
	}

	expectedFruitTop, err := buildInputFruitTop(inputFile)
	if err != nil {
		return err
	}
	receivedFruitTop, err := readOutputFruitTop(outputFile)
	if err != nil {
		return err
	}

	mismatchFound := false
	for i := 0; i < topSize; i++ {
		expected := expectedFruitTop[i]
		var received fruititem.FruitItem
		if i < len(receivedFruitTop) {
			received = receivedFruitTop[i]
		} else {
			received = fruititem.FruitItem{Fruit: "-", Amount: 0}
		}

		if expected == received {
			log.Printf("%-16s %5d", received.Fruit, received.Amount)
		} else {
			log.Printf("%-16s %5d - Expected: %-16s %5d", received.Fruit, received.Amount, expected.Fruit, expected.Amount)
			mismatchFound = true
		}
	}
	if mismatchFound {
		return errors.New("Mismatch in expected and received fruit tops")
	}

	if topSize != len(receivedFruitTop) {
		return errors.New(fmt.Sprintf("Mismatch in expected and received fruit tops length %d/%d", len(receivedFruitTop), topSize))
	}

	log.Println("OK")
	return nil
}

func findTopSize(services map[string]service) int {
	for _, svc := range services {
		if val := findEnvVar(svc.Environment, "TOP_SIZE"); val != "" {
			topSize, err := strconv.Atoi(val)
			if err == nil {
				return topSize
			}
		}
	}
	return 0
}

func main() {
	data, err := os.ReadFile(dockerFilePath)
	if err != nil {
		log.Printf("Unexpected error: %v", err)
		os.Exit(1)
	}

	var compose dockerCompose
	if err := yaml.Unmarshal(data, &compose); err != nil {
		log.Printf("Unexpected error: %v", err)
		os.Exit(1)
	}

	var clientServiceNames []string
	for name, service := range compose.Services {
		if strings.Contains(service.Build.Dockerfile, "client") {
			clientServiceNames = append(clientServiceNames, name)
		}
	}

	topSize := findTopSize(compose.Services)

	log.Println("Awaiting client containers to exit...")
	if err := awaitClientContainers(clientServiceNames); err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	log.Println("Validating clients...")
	for _, name := range clientServiceNames {
		svc := compose.Services[name]
		if err := verifyClientOutput(topSize, svc); err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	}

	log.Println("All fruit tops match the expected results")
}
