package common

// Bucket cluster_map中的非叶子节点
type Bucket struct {
	Weight   int64                  // 权值
	Children map[string]interface{} // 子节点
}

// Device cluster_map中的叶子节点
type Device struct {
	IP     string // ip地址
	Port   uint16 // 端口号
	Weight int64  // 权值
}
