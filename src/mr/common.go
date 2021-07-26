package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = false

func DebugPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format, v...)
	}
}

func reduceName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func mergeName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}
