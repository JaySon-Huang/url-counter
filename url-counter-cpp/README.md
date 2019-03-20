### 环境需求

```
cmake 2.8+
gcc 4.8+
```

### 编译运行
```
mkdir cmake-build-release
cd cmake-build-release/
cmake -DCMAKE_BUILD_TYPE=Release -G "CodeBlocks - Unix Makefiles" ..
make

./url_counter ../../top-1m.url 2 100
```
