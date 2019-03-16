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

1. 对 url hash 拆分为互不重叠的小文件
2. 各个小文件分别统计
3. 合并选出Top 1000的url

## 问题 && 优化

### 分别统计的具体方法
  * hash-map 计数
  * trie-tree 计数
  * 排序后，遍历计数

#### 复杂度分析
* hash-map,O(n)
* trie-tree,O(n * avg_len) `avg_len` 为 URL 的平均长度
* 排序, O(nlogn). 在文件较小的情况下用快排；文件比较大(见数据倾斜)采用外排,还有IO代价

### 数据倾斜 (skew)
#### 影响分析
数据倾斜导致 第1步 拆分得到的小文件大小不均, 从而导致第2步 统计过程, 如果利用多核并发执行, 会有少量任务所需时间比其它任务长。  

* 如果第2步采用 `hash-map`/`trie-tree` 统计，数据倾斜对 **内存限制** 这一条件没有影响，因为大量key重复出现才会导致数据倾斜。而统计过程中顺序读文件，插入 hash-map/trie-tree 所需的内存不会比其它小分片大
* 如果第2步采用排序的方式，快排就不合适，需要改为使用基于磁盘的归并排序。

#### 数据倾斜的解决思路
* 第2步为 `hash-map`/`trie-tree`
在第1步中, 对 `skew`的部分URL, 加随机前缀后再进行一次 hash 拆分。拆分过程增加了磁盘IO, 但可以利用多核提高计算并行度。注意拆分后合并阶段，因为同一条url可能被分到不同的文件中，需要先合并统计结果。
* 第2步为排序
采用基于磁盘的归并排序

## 测试数据生成 && 运行

```bash
wget http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
uzip top-1m.csv.zip
python2 conv.py --filename top-1m.csv --out top-1m.url
python2 solve.py --filename top-1m.url --split 2 --ntop 100
```
