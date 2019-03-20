#ifndef UTILS_UTILS_H
#define UTILS_UTILS_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

// utils

size_t GetFileSize(const std::string &fn) {
    struct stat sbuf;
    size_t size = 0;
    if (stat(fn.c_str(), &sbuf) != 0) {
        fprintf(stderr, "GetFileSize: errno: %d\n", errno);
        assert(false);
    } else {
        size = sbuf.st_size;
    }
    return size;
}

bool DirExists(const std::string &dname) {
    struct stat statbuf;
    if (stat(dname.c_str(), &statbuf) == 0) {
        return S_ISDIR(statbuf.st_mode);
    }
    return false;
}

bool GetChildren(const std::string &dir,
                 std::vector<std::string> *result) {
    result->clear();

    DIR *d = opendir(dir.c_str());
    if (d == nullptr) {
        switch (errno) {
            case EACCES:
            case ENOENT:
            case ENOTDIR:
                // file not found
                return false;
            default:
                // other error
                return false;
        }
    }
    struct dirent *entry;
    while ((entry = readdir(d)) != nullptr) {
        // exclude '.' '..' from result
        if (strncmp(entry->d_name, ".", PATH_MAX) != 0 && strncmp(entry->d_name, "..", PATH_MAX) != 0) {
            result->push_back(std::string(entry->d_name));
        }
    }
    closedir(d);
    return true;
}

bool DeleteFile(const std::string &fname) {
    bool result = true;
    if (unlink(fname.c_str()) != 0) {
        fprintf(stderr, "while unlink() file: %s, %d", fname.c_str(), errno);
        result = false;
    }
    return result;
};

bool DeleteDir(const std::string &name, bool recursive, bool verbose) {
    bool result = true;
    if (recursive) {
        struct stat buf;
        std::vector<std::string> files;
        result = GetChildren(name, &files);
        if (!result) {
            fprintf(stderr, "err getting children of: %s\n", name.c_str());
            return result;
        }
        for (const auto &file: files) {
            const std::string filename = fmt::format("{}/{}", name, file);
            if (lstat(filename.c_str(), &buf) < 0) {
                fprintf(stderr, "Fail to get stat of %s\n", name.c_str());
                return false;
            }
            if (S_ISDIR(buf.st_mode)) {
                if (verbose) fprintf(stderr, "entering dir: %s\n", filename.c_str());
                result = DeleteDir(filename, recursive, verbose);
                if (verbose) fprintf(stderr, "removing dir: %s\n", filename.c_str());
            } else {
                if (verbose) fprintf(stderr, "removing file: %s\n", filename.c_str());
                result = DeleteFile(filename);
            }
            if (!result) { return result; }
        }
        int iret = remove(name.c_str());
        if (iret == 0) { return true; }
        else { return false; }
    } else {
        if (rmdir(name.c_str()) != 0) {
            result = false;
        }
    }
    return result;
}

bool CreateDir(const std::string &name) {
    bool result;
    if (mkdir(name.c_str(), 0755) != 0) {
        result = false;//result = IOError("While mkdir", name, errno);
    }
    return result;
}


bool str_ends_with(const std::string &cur, const std::string &suffix) {
    return ((cur.size() >= suffix.size()) &&
            (memcmp(cur.data() + cur.size() - suffix.size(), suffix.data(), suffix.size()) == 0));
}

int split(const std::string &str, const char pattern, std::vector<std::string> &result, const int max_split=-1) {
    result.clear();

    assert(max_split == -1 || max_split >= 0);  // max_split 不应为-1以外的负值

    size_t numPattern = 0;

    size_t begPos = 0,
            endPos = 0;
    while ((endPos = str.find(pattern, begPos)) != std::string::npos) {
        if (max_split != -1 && numPattern == static_cast<size_t>(max_split)) {
            // printf("[Max] Push `%s`\n", str.substr(begPos).c_str());
            result.push_back(str.substr(begPos));
            return 0;
        }
        //printf("(%zu, %zu) `%s`\n",
        //         begPos, endPos,
        //         str.substr(begPos, endPos - begPos).c_str());
#if __cplusplus < 201103L
        result.push_back(str.substr(begPos, endPos - begPos));
#else
        result.emplace_back(str.substr(begPos, endPos - begPos));
#endif
        begPos = endPos + 1;
        numPattern += 1;
    }
#if __cplusplus < 201103L
    result.push_back(str.substr(begPos));
#else
    result.emplace_back(str.substr(begPos));
#endif
    return 0;
}

#endif //UTILS_UTILS_H
