package common

import set "github.com/deckarep/golang-set"

// Bucket cluster_map中的非叶子节点
type Bucket struct {
	Weight    int64                  // 权值
	Children  map[string]interface{} // 子节点
	Container set.Set                // 包含哪些osd，元素是id
}

// Device cluster_map中的叶子节点
type Device struct {
	Id     string // osd标识
	IP     string // ip地址
	Port   uint16 // 端口号
	Weight int64  // 权值
}
