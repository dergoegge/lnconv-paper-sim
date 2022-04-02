package lnsim

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
)

type GossipAlgorithm struct {
	Name          string  `json:"name"`
	StaggerTimer  uint64  `json:"stagger_timer"`
	MinBatchSize  int     `json:"min_batch_size"`
	ReconcilTimer uint64  `json:"reconcil_timer"`
	ReconcilQ     float64 `json:"reconcil_q"`
	TrickleTimer  uint64  `json:"trickle_timer"`
	ShouldTrickle bool    `json:"should_trickle"`
	SpanningTree  bool    `json:"spanning_tree"`
	SendInvs      bool    `json:"send_invs"`
	// Should updates be send one by one or in a group as one message?
	SendBatchUpdates bool `json:"send_batch_updates"`
}

func LndGossip() GossipAlgorithm {
	return GossipAlgorithm{
		Name:         "lnd",
		MinBatchSize: 10,
		StaggerTimer: 90000,
		TrickleTimer: 5000,
		SendInvs:     false,

		ReconcilTimer: 0,
		ReconcilQ:     0.25,

		// This is not technically true for lnd but it helps with simulation
		// performance and does not change the results.
		SendBatchUpdates: true,
	}
}

type Config struct {
	NumberOfMessages uint   `json:"num_messages"`
	MessageInterval  uint64 `json:"message_broadcast_interval"`

	NumberOfPayments uint   `json:"num_payments"`
	PaymentInterval  uint64 `json:"payment_interval"`

	PercentageBlackholes int `json:"percent_blackholes"`

	Algorithm GossipAlgorithm `json:"algo"`

	SyncerConnections uint `json:"syncer_conns"`

	NoKeepalives bool `json:"no_keepalives"`

	GraphFile   string `json:"graph"`
	MessageFile string `json:"messages"`
	StartTime   uint64 `json:"start_time"`
	EndTime     uint64 `json:"end_time"`

	CpuProfile string `json:"cpu_profile"`
	MemProfile string `json:"mem_profile"`

	OutputDir string `json:"output"`

	AllowExcessiveMemUsage bool `json:"allow_excessive_mem_usage"`

	ID string `json:"id"`

	BlackholeTest bool `json:"test_blackholes"`
}

var GlobalConfig Config

func (c *Config) Default() {
	*c = Config{
		NumberOfMessages:       10,
		MessageInterval:        0,
		NumberOfPayments:       0,
		PaymentInterval:        0,
		PercentageBlackholes:   0,
		Algorithm:              LndGossip(),
		SyncerConnections:      3,
		GraphFile:              "",
		MessageFile:            "",
		CpuProfile:             "",
		MemProfile:             "",
		AllowExcessiveMemUsage: false,
		NoKeepalives:           false,
	}
}

func (c Config) Name() string {
	return c.Algorithm.Name + "_" +
		strconv.Itoa(int(c.NumberOfMessages)) + "m_" +
		strconv.Itoa(int(c.NumberOfPayments)) + "p"
}

func (c Config) ShortName() string {
	return c.Algorithm.Name
}

func InitConfig(configPath string) {
	jsonFile, err := os.Open(configPath)
	if err != nil {
		fmt.Println("Could not open config file", configPath)
		panic(err)
	}

	defer jsonFile.Close()

	bytes, _ := ioutil.ReadAll(jsonFile)

	GlobalConfig.Default()
	json.Unmarshal(bytes, &GlobalConfig)
	GlobalConfig.GraphFile, _ = filepath.Abs(GlobalConfig.GraphFile)
	GlobalConfig.OutputDir, _ = filepath.Abs(path.Join("experiments", GlobalConfig.Name()))
	if GlobalConfig.MessageFile != "" {
		GlobalConfig.MessageFile, _ = filepath.Abs(GlobalConfig.MessageFile)
	}
	GlobalConfig.Algorithm.ShouldTrickle = GlobalConfig.Algorithm.TrickleTimer > 0
	GlobalConfig.ID = GlobalConfig.Name()

	if !GlobalConfig.AllowExcessiveMemUsage && GlobalConfig.NumberOfMessages > 1000 {
		panic(fmt.Errorf("InitConfig: a large number of messages (>1000) can cause excessive memory usage.\n" +
			"configure with \"allow_excessive_mem_usage\": true to override this check."))
	}

	_ = os.MkdirAll(GlobalConfig.OutputDir, 0777)

	completeConfigFile, err := os.Create(path.Join(GlobalConfig.OutputDir, "config.json"))
	if err != nil {
		panic(err)
	}
	defer completeConfigFile.Close()

	completeConfigFile.WriteString(GlobalConfig.String())
}

func (c Config) String() string {
	bytes, _ := json.MarshalIndent(c, "", "    ")
	return string(bytes)
}
