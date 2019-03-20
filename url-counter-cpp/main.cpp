
#include <stdio.h>
#include <random>
#include <unordered_map>
#include <stack>

#include <string>
#include <cstdint>
#include <cassert>
#include <cmath>
#include <vector>
#include <fstream>
#include <queue>

using std::string;
using std::vector;


#include "fmt/format.h"
#include "hash/murmurhash.h"
#include "utils.h"


std::string part(
        const std::string &filename,
        size_t splited_mb = 512,
        bool preserve_ori_file = true,
        bool is_skew_add_prefix = false,
        size_t skew_splited_factor = 10) {
    assert(skew_splited_factor <= 26);
    // 计算拆分为多少份
    size_t split_bytes = splited_mb * (1024 * 1024);
    size_t file_size_to_split = GetFileSize(filename);
    size_t num_to_split = int(ceil(1.0 * file_size_to_split / split_bytes));
    fmt::print(stderr,
            "`{}`({}) going to split into {} parts, expect {}mb",
            filename, file_size_to_split, num_to_split, splited_mb);

    // 生成拆分的文件及小文件
    std::string dirname = fmt::format("{}-parted", filename);
    if (DirExists(dirname)) {
        DeleteDir(dirname, true, true);
    }
    CreateDir(dirname);

    // url 利用 murmurhash3 拆分为互不重叠的小文件
    std::vector<std::string> outfiles_name;
    for (size_t i = 0; i < num_to_split; ++i) {
        outfiles_name.emplace_back(fmt::format(fmt::format("{}/part-{:05d}", dirname, i)));
    }
    std::vector<FILE *> outfiles;
    for (auto &fn : outfiles_name) {
        FILE *fp = fopen(fn.c_str(), "wb");
        outfiles.emplace_back(fp);
    }
    std::vector<size_t> outfiles_size(outfiles.size(), 0);

    std::ifstream infile;
    infile.open(filename, std::ios::in);
    if (!infile.good()) {
        assert(false);
    }
    std::string line;
    while (std::getline(infile, line)) {
        const size_t line_len = line.size();
        if (line.back() == '\n') {
            line.resize(line.size() - 1);
        }
        uint64_t h = 0;
        if (is_skew_add_prefix) {
            std::string prefix;
            prefix.push_back('a' + (random() % skew_splited_factor));
            prefix += line;
            uint64_t hash = MurmurHash64A(prefix.c_str(), prefix.size(), 0);
            //if (line == "amazon.com") {fmt::print("line: {} hash: {}\n", prefix, hash);}
            h = hash % num_to_split;
        } else {
            uint64_t hash = MurmurHash64A(line.c_str(), line.size(), 0);
            //if (line == "amazon.com") {fmt::print("line: {} hash: {}\n", line, hash);}
            h = hash % num_to_split;
        }
        FILE *fp = outfiles[h];
        fwrite(line.c_str(), line.size(), 1, fp);
        fwrite("\n", 1, 1, fp);
        outfiles_size[h] += line_len;
    }
    // 关闭文件句柄
    for (auto *fp : outfiles) { fclose(fp); }

    // 检查 outfiles_size 过滤出比预期大的文件
    std::vector<std::string> skew_files;
    for (size_t i = 0; i < outfiles_name.size(); ++i) {
        if (outfiles_size[i] > split_bytes) {
            skew_files.emplace_back(outfiles_name[i]);
        }
    }

    if (!skew_files.empty()) {
        fmt::print(stderr, "skew files:");
        for (const auto &fn : skew_files) {
            fmt::print(stderr, "{},", fn);
        }
        fmt::print(stderr, "\n");
        // 对数据倾斜的部分进一步拆分
        for (const auto &fn : skew_files) {
            part(fn, splited_mb, false, true);
        }
    }
    if (!preserve_ori_file) {
        DeleteFile(filename);
    }
    return dirname;
}

bool get_leave_files(const std::string &path, std::vector<std::string> *file_lst) {
    std::vector<std::string> cur_file_lst;
    bool result = GetChildren(path, &cur_file_lst);
    assert(result);
    for (const auto &fn: cur_file_lst) {
        std::string filepath = fmt::format("{}/{}", path, fn);
        if (DirExists(filepath)) { // 是目录, 递归获取子目录下的所有文件
            get_leave_files(filepath, file_lst);
        } else {
            file_lst->emplace_back(filepath);
        }
    }
    return true;
}

class CountItem {
public:
    std::string url;
    size_t cnt;
    CountItem(std::string url_="", size_t cnt_=0): url(url_), cnt(cnt_) {
    }

    bool operator<(const CountItem &rhs) const {
        return cnt > rhs.cnt;  // 默认为大顶堆, 重定义小于使其作为小顶堆
    }
};

void count(const std::string &filename, size_t topN) {
    std::vector<std::string> filenames;
    if (DirExists(filename)) {
        // skew key 进一步拆分的文件夹, 同一个 url 可能分布在该文件夹下的不同文件, 需要合并
        get_leave_files(filename, &filenames);
    } else {
        // hash 取模拆分的文件, URL互不重叠
        filenames.emplace_back(filename);
    }

    std::unordered_map<std::string, size_t> counter;

    // TODO 多线程并发
    for (const auto &fn: filenames) {
        std::ifstream infile;
        infile.open(fn, std::ios::in);
        assert(infile.good());
        size_t lineno = 0;
        std::string line;
        while (std::getline(infile, line)) {
            if (lineno % 50000 == 0) {
                fmt::print(stderr, "processing {} line\n", lineno);
            }
            if (line.back() == '\n') { line.resize(line.size() - 1);}
            // 更新计数
            size_t prev_cnt = 0;
            auto iter = counter.find(line);
            if (iter != counter.end()) {
                prev_cnt = iter->second;
            }
            counter[line] = prev_cnt + 1;

            lineno++;
        }
    }

    // 使用对过滤出此分片中 TopN 的结果, 可以减少冗余结果带来的 IO 代价
    std::priority_queue<CountItem> q;
    for (const auto &kv : counter) {
        assert(q.size() <= topN);
        if (q.size() == topN) {
            // 弹出最小的元素, 对比, 压较大的元素入堆
            CountItem cur_min = q.top(); q.pop();
            if (cur_min.cnt < kv.second) {
                q.push(CountItem(kv.first, kv.second));
            } else {
                q.push(cur_min);
            }
        } else {
            // 未满, 入堆
            q.push(CountItem(kv.first, kv.second));
        }
    }
    fmt::print("file: {}, qsize: {}\n", filename, q.size());
    std::string cnt_filename = filename + "_cnt";
    FILE *fp = fopen(cnt_filename.c_str(), "wb");
    assert(fp != nullptr);
    while (!q.empty()) {
        CountItem cur_min = q.top(); q.pop();
        fprintf(fp, "%s,%zu\n", cur_min.url.c_str(), cur_min.cnt);
    }
    fclose(fp);
}

std::priority_queue<CountItem>
reduce_counters(const std::string &dirname, size_t topN=1000) {
    std::priority_queue<CountItem> q;
    std::vector<std::string> children;
    GetChildren(dirname, &children);
    for (const auto &_fn : children) {
        std::string fn = fmt::format("{}/{}", dirname, _fn);
        if (!str_ends_with(fn, "_cnt")) { continue; }
        std::ifstream infile;
        infile.open(fn, std::ios::in);
        assert(infile.good());
        std::string line;
        while (std::getline(infile, line)) {
            if (line.back() == '\n') {
                line.resize(line.size() - 1);
            }
            std::vector<std::string> parts;
            split(line, ',', parts);
            assert(parts.size() == 2);
            const std::string &url = parts[0];
            const std::string &cnt_s = parts[1];
            size_t cnt = atoll(cnt_s.c_str());
            assert(q.size() <= topN);
            if (q.size() == topN) {
                // 弹最小的元素, 对比, 压较大的元素入堆
                auto cur_min = q.top(); q.pop();
                if (cur_min.cnt < cnt) {
                    q.push(CountItem(url, cnt));
                } else {
                    q.push(cur_min);
                }
            } else {
                // 未满, 入堆
                q.push(CountItem(url, cnt));
            }
        }
        fmt::print(stderr, "after counting {}, qsize: {}\n", fn, q.size());
    }
    return q;
}

int main(int argc, char **argv) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s filename split-size(mb) ntop\n", argv[0]);
        return EXIT_FAILURE;
    }
    const std::string filename = argv[1];
    size_t split = atoi(argv[2]);
    int ntop = atoi(argv[3]);

    // main
    std::string parted_dir = part(filename, split);

    std::vector<std::string> children;
    GetChildren(parted_dir, &children);
    for (const auto &_fn : children) {
        std::string fn = fmt::format("{}/{}", parted_dir, _fn);
        count(fn, ntop);
    }

    auto q = reduce_counters(parted_dir, ntop);

    // print results
    // 小顶堆, 通过 stack 使其降序输出
    fmt::print("qsize: {}\n", q.size());
    std::stack<CountItem> s_items;
    while (!q.empty()) {
        CountItem item = q.top();
        q.pop();
        s_items.push(item);
    }
    fmt::print("rank\tcount\turl\n");
    fmt::print("====================\n");

    size_t idx = 0;
    while (!s_items.empty()) {
        CountItem item = s_items.top();s_items.pop();
        fmt::print("{}\t{}\t{}\n", idx, item.cnt, item.url);
        idx++;
    }

    return EXIT_SUCCESS;
}
