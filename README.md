# url-counter

## 问题
100GB的url，统计 Top 100 url及其出现次数。
内存限制：1GB

## 解决思路

### Spark伪代码
```scala
sc.readText("/path/to/input")
  .map(url => (url, 1))
  .reduceByKey((lcnt, rcnt) => lcnt + rcnt)
  .sortBy((url, cnt) => cnt)
  .zipWithIndex
  .filter(_._2 < 1000)
  .save("/path/to/output")
```

### 通过编程实现

* 对 url hash 拆分为互不重叠的小文件
* 各个小文件分别统计
* 合并选出Top 1000的url

## 问题 && 优化

* 拆分过程可能存在数据倾斜
* 分别统计的具体方法
  * hash-map 计数
  * trie-tree 计数
  * 快排后，遍历计数

## 测试数据生成

```bash
wget http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
uzip top-1m.csv.zip
```
