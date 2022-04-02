package main

import (
	"flag"
	"log"
	"math/rand"
	"os"

	"git.tu-berlin.de/dergoegge/bachelor_thesis/lnsim"
)

func main() {
	log.SetOutput(os.Stdout)
	rand.Seed(0)
	configFile := flag.String("config", "", "path to simulation config")
	flag.Parse()

	lnsim.InitConfig(*configFile)
	log.Println("config:", lnsim.GlobalConfig)
	if lnsim.GlobalConfig.BlackholeTest {
		for i := 0; i < 100; i++ {
			rand.Seed(int64(i))
			lnsim.New()
		}
		return
	}

	sim := lnsim.New()
	sim.Start()
}
