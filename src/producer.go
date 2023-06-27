package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

type Logdata struct {
	mutex     sync.RWMutex
	Resources `json:"resources"`
	Network   `json:"network"`
	Timestamp time.Time `json:"timestamp"`
}

type Resources struct {
	CPU    `json:"cpu"`
	Memory `json:"memory"`
	Disk   `json:"disk"`
	Uptime `json:"uptime"`
}

type CPU struct {
	Usage float64 `json:"cpu usage"`
}

type Memory struct {
	Total     uint64  `json:"memory total"`
	Available uint64  `json:"memory available"`
	Used      uint64  `json:"memory used"`
	Free      uint64  `json:"memory free"`
	Usage     float64 `json:"memory usage"`
}

type Disk struct {
	Total uint64  `json:"disk total"`
	Used  uint64  `json:"disk used"`
	Free  uint64  `json:"disk free"`
	Usage float64 `json:"disk usage"`
}

type Uptime struct {
	runtime string `json:"runtime"`
}

type Network struct {
	PacketsSent int     `json:"packets sent"`
	PacketsLost int     `json:"packets lost"`
	MinRTT      float64 `json:"min rtt"`
	AvgRTT      float64 `json:"avg rtt"`
	MaxRTT      float64 `json:"max rtt"`
}

func main() {
	// Kafka broker addresses
	brokers := []string{"localhost:9092"}

	// Create a new Kafka producer
	producer, err := sarama.NewSyncProducer(brokers, nil)
	if err != nil {
		log.Fatal("Failed to create producer:", err)
	}
	defer producer.Close()

	// Create a new Kafka topic for streaming data
	topic := "server-001-health"

	// Produce streaming data
	errChan := make(chan error)
	logdata := Logdata{}
	tasknum := 5

	for i := 0; ; i++ {
		go logdata.cpuMonitor(errChan)
		go logdata.memoryMonitor(errChan)
		go logdata.diskMonitor(errChan)
		go logdata.uptimeMonitor(errChan)
		go logdata.networkMonitor(errChan)

		for i := 0; i < tasknum; i++ {
			select {
			case msg := <-errChan:
				if msg != nil {
					fmt.Println(msg)
				}
			}
		}

		message, err := json.Marshal(logdata)
		if err != nil {
			fmt.Println("Error in marshal log data")
		}

		// Create a new Kafka message
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.StringEncoder(message),
		}

		// Send the message to the Kafka topic
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			log.Println("Failed to send message:", err)
		} else {
			log.Printf("Message sent successfully. Partition: %d, Offset: %d\n", partition, offset)
		}

		// Sleep for a while before sending the next message
		time.Sleep(time.Second * 1)
	}
}

func (ld *Logdata) cpuMonitor(ec chan error) {
	// Get CPU usage percentages for all cores
	percentages, err := cpu.Percent(time.Second, false)
	if err != nil {
		ec <- err
		return
	}

	// Use mutex to synchronize the lookup access
	ld.mutex.Lock()
	defer ld.mutex.Unlock()

	// CPU usage for each core
	for _, percentage := range percentages {
		ld.Resources.CPU.Usage = percentage
	}
	ec <- nil
}

func (ld *Logdata) memoryMonitor(ec chan error) {
	// Get memory usage
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		ec <- err
		return
	}

	// Use mutex to synchronize the lookup access
	ld.mutex.Lock()
	defer ld.mutex.Unlock()

	// Memory usage information
	ld.Resources.Memory.Total = memInfo.Total
	ld.Resources.Memory.Available = memInfo.Available
	ld.Resources.Memory.Used = memInfo.Used
	ld.Resources.Memory.Free = memInfo.Free
	ld.Resources.Memory.Usage = memInfo.UsedPercent
	ec <- nil
}

func (ld *Logdata) diskMonitor(ec chan error) {
	// Get disk usage
	usageStat, err := disk.Usage("/")
	if err != nil {
		ec <- err
		return
	}

	// Use mutex to synchronize the lookup access
	ld.mutex.Lock()
	defer ld.mutex.Unlock()

	// Disk usage information
	ld.Resources.Disk.Total = usageStat.Total
	ld.Resources.Disk.Used = usageStat.Used
	ld.Resources.Disk.Free = usageStat.Free
	ld.Resources.Disk.Usage = usageStat.UsedPercent
	ec <- nil
}

func (ld *Logdata) uptimeMonitor(ec chan error) {
	// Run the "uptime" command to get the system uptime
	output, err := exec.Command("uptime", "-p").Output()
	if err != nil {
		ec <- err
		return
	}

	// Use mutex to synchronize the lookup access
	ld.mutex.Lock()
	defer ld.mutex.Unlock()

	// Extract the uptime string from the output
	uptime := strings.TrimSpace(string(output))
	ld.Resources.Uptime.runtime = uptime
	ec <- nil
}

func (ld *Logdata) networkMonitor(ec chan error) {
	cmd := exec.Command("ping", "-c", "5", "google.com")
	output, err := cmd.CombinedOutput()
	if err != nil {
		ec <- err
		return
	}

	pingOutput := string(output)
	lines := strings.Split(pingOutput, "\n")

	var sentPackets int
	var lostPackets int
	var minRTT float64
	var avgRTT float64
	var maxRTT float64

	for _, line := range lines {
		if strings.Contains(line, "packets transmitted") {
			fields := strings.Fields(line)
			sentPackets = parseInt(fields[0])
			lostPackets = parseInt(fields[5])
		} else if strings.Contains(line, "rtt min/avg/max/mdev") {
			fields := strings.Fields(line)
			rttValues := strings.Split(fields[3], "/")
			minRTT = parseFloat(rttValues[0])
			avgRTT = parseFloat(rttValues[1])
			maxRTT = parseFloat(rttValues[2])
		}
	}

	// Use mutex to synchronize the lookup access
	ld.mutex.Lock()
	defer ld.mutex.Unlock()

	// Network Latency
	ld.Network.PacketsSent = sentPackets
	ld.Network.PacketsLost = lostPackets
	ld.Network.MinRTT = minRTT
	ld.Network.MaxRTT = maxRTT
	ld.Network.AvgRTT = avgRTT
	ec <- nil
}

func parseInt(value string) int {
	var result int
	_, err := fmt.Sscanf(value, "%d", &result)
	if err != nil {
		fmt.Println("Error parsing integer:", err)
	}
	return result
}

func parseFloat(value string) float64 {
	var result float64
	_, err := fmt.Sscanf(value, "%f", &result)
	if err != nil {
		fmt.Println("Error parsing float:", err)
	}
	return result
}
