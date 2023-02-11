package components

import (
	"sync"
)

var (
	mc   *MonitorContainer
	lock sync.Mutex
)
