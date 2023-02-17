# 极简CEPH分布式文件系统监控节点实现
<div>
<a target="_blank" href="https://trinoooo.github.io/">
<img src="https://img.shields.io/badge/author-trino-yellowgreen"/>
</a>
<img alt="GitHub go.mod Go version (subdirectory of monorepo)" src="https://img.shields.io/github/go-mod/go-version/Trinoooo/simplify_ceph_monitor?filename=go_version%2Fgo.mod">
</div>

## 简介
这是极简版的ceph-monitor代码实现，其中舍弃了很多官方版本中十分优雅的特性，这是因为我想通过抽象出最核心部分加深对ceph整体的理解。
正如上文所说，我舍弃了很多官方版本中包含的特性，但是保留了最核心的逻辑。
- [x] 监控节点集群的关系维护
- [x] 分布式共识算法实现（Raft）
- [x] ceph分布式系统中各集群状态维护
- [x] 监控节点集群对外状态查询
- [ ] 监控节点集群整体测试方案
## 快速开始
请通过代码中的测试框架查看运行结果
- TODO
## 引用
go语言版本raft算法的实现很大程度上参考了eliben大佬的raft项目
- [仓库地址](https://github.com/eliben/raft/tree/master)
- [大佬个人博客](https://eli.thegreenplace.net/2020/implementing-raft-part-0-introduction/)
## 其他
> 北海，要多想

[![Star History Chart](https://api.star-history.com/svg?repos=Trinoooo/simplify_ceph_monitor&type=Date)](https://star-history.com/#Trinoooo/simplify_ceph_monitor&Date)
