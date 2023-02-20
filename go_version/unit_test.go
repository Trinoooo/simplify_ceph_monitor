package go_version

import (
	"ceph/monitor/cephadm"
	"ceph/monitor/consts"
	monitor "ceph/monitor/mon"
	"ceph/monitor/others"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

// TestCephadm
// 测试cephadm功能
// * 添加mon节点
// * 移除mon节点
func TestCephadm(t *testing.T) {
	admin := cephadm.NewCephadm()

	// 测试添加mon节点
	monAlpha := monitor.NewMonitor("alpha", admin)
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
	monBeta := monitor.NewMonitor("beta", admin)
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
	monGamma := monitor.NewMonitor("gamma", admin)
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())

	// 测试移除mon节点
	monAlpha.Shutdown()
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
	monBeta.Shutdown()
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
	monGamma.Shutdown()
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
}

// TestMonitorConsensus
// 测试状态监控节点共识选主
func TestElection(t *testing.T) {
	admin, monAlpha, monBeta, monGamma := bootSystem()

	// 停顿3秒
	time.Sleep(3 * time.Second)
	// 移除alpha
	monAlpha.Shutdown()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	time.Sleep(1 * time.Second)

	// 移除beta
	monBeta.Shutdown()
	monGamma.ReportConsensusState()
	time.Sleep(1 * time.Second)

	// 移除gamma
	monGamma.Shutdown()
	t.Logf("current monitorIds: %v", admin.GetMonitorIds())
	time.Sleep(1 * time.Second)
}

// TestLogSynchronization
// 测试状态监控节点共识日志复制
func TestLogReplication(t *testing.T) {
	admin, monAlpha, monBeta, monGamma := bootSystem()

	// osd-alpha加入系统
	osdAlpha := others.NewOtherNode("osd-alpha", admin, consts.NODE_TYPE_OSD, &monitor.OSDTopology{
		Weight: 10,
		Path:   []string{"rack.001", "host.001", "osd.001"},
		IP:     "192.168.0.1",
		Port:   3306,
	})
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	// osd-beta加入系统
	osdBeta := others.NewOtherNode("osd-beta", admin, consts.NODE_TYPE_OSD, &monitor.OSDTopology{
		Weight: 5,
		Path:   []string{"rack.001", "host.002", "osd.002"},
		IP:     "192.168.0.2",
		Port:   3306,
	})
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	// osd-gamma加入系统
	osdGamma := others.NewOtherNode("osd-gamma", admin, consts.NODE_TYPE_OSD, &monitor.OSDTopology{
		Weight: 10,
		Path:   []string{"rack.002", "host.001", "osd.001"},
		IP:     "192.168.1.1",
		Port:   3306,
	})
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	// 测试集群状态查询接口
	log.Printf("[EXTERNAL]:%s", monAlpha.ReportClusterState())

	// osd-alpha离开系统
	osdAlpha.Shutdown()
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	// osd-beta离开系统
	osdBeta.Shutdown()
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	// osd-gamma离开系统
	osdGamma.Shutdown()
	time.Sleep(1 * time.Second)
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	fmt.Println(monAlpha.GetClusterMap())

	shutdownSystem([]*monitor.Monitor{monAlpha, monBeta, monGamma})
}

func bootSystem() (*cephadm.Cephadm, *monitor.Monitor, *monitor.Monitor, *monitor.Monitor) {
	admin := cephadm.NewCephadm()
	// 启动alpha
	monAlpha := monitor.NewMonitor("alpha", admin)
	monAlpha.Ready()
	monAlpha.ReportConsensusState()
	time.Sleep(1 * time.Second)

	// 启动beta
	monBeta := monitor.NewMonitor("beta", admin)
	monBeta.Ready()
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	time.Sleep(1 * time.Second)

	// 启动gamma
	monGamma := monitor.NewMonitor("gamma", admin)
	monGamma.Ready()
	monAlpha.ReportConsensusState()
	monBeta.ReportConsensusState()
	monGamma.ReportConsensusState()
	time.Sleep(1 * time.Second)

	return admin, monAlpha, monBeta, monGamma
}

func shutdownSystem(monitors []*monitor.Monitor) {
	wg := sync.WaitGroup{}
	for _, mon := range monitors {
		wg.Add(1)
		go func(monitor *monitor.Monitor) {
			monitor.Shutdown()
			wg.Done()
		}(mon)
	}

	wg.Wait()
}
