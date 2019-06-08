use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::hash::Hasher;
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;

use rand::prelude::*;

use fasthash::{murmur3::Hasher32, FastHasher};

fn part(filename: &str, splited_mb: u64,
        _preserve_ori_file: bool,
        is_skew_add_prefix: bool,
        skew_splited_factor: u8) -> Result<String, io::Error> {
    assert!(skew_splited_factor <= 26);

    let split_bytes = splited_mb * (1024 * 1024);
    let file_size_to_split = {
        let meta = fs::metadata(filename)?;
        meta.len()
    };
    let num_to_split = (file_size_to_split as f32 / split_bytes as f32).ceil() as u64;
    eprintln!("`{}`({}) going to split into {} parts, expect {}mb",
              filename, file_size_to_split, num_to_split, splited_mb);

    let dirname = format!("{}-parted", filename);
    if Path::new(&dirname).is_dir() {
        fs::remove_dir_all(&dirname)?;
    }
    fs::create_dir(&dirname)?;

    let mut outfiles_name: Vec<String> = vec!();
    for i in 0..num_to_split {
        outfiles_name.push(format!("{}/part-{:05}", dirname, i));
    }
    let outfiles_name = outfiles_name;
    let mut outfiles: Vec<fs::File> = vec!();
    for filename in outfiles_name {
        outfiles.push(fs::File::create(filename)?);
    }
    let outfiles = outfiles;

    let mut outfiles_size = vec![0; outfiles.len()];

    let mut rng = rand::thread_rng();
    let infile = BufReader::new(File::open(filename)?);
    for line in infile.lines() {
        let line = match line {
            Ok(line) => line.trim().to_string(),
            Err(_e) => continue
        };
        let h: usize = if is_skew_add_prefix {
            let mut hasher = Hasher32::new();
            let mut prefix = String::new();
            prefix.push(rng.gen_range(0, skew_splited_factor) as char);
            hasher.write(prefix.as_bytes());
            hasher.write(line.as_bytes());
            (hasher.finish() % num_to_split) as usize
        } else {
            let mut hasher = Hasher32::new();
            hasher.write(line.as_bytes());
            (hasher.finish() % num_to_split) as usize
        };
        let mut fp = &outfiles[h];
        fp.write_fmt(format_args!("{}\n", line));
        outfiles_size[h] += line.len();
    }

    return std::result::Result::Ok(dirname);
}

#[derive(Clone, Eq, PartialEq)]
struct CountItem {
    url: String,
    cnt: u64,
}

impl Ord for CountItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cnt.cmp(&self.cnt)
    }
}

impl PartialOrd for CountItem {
    fn partial_cmp(&self, other: &CountItem) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

fn count(filename: &str, ntop: u64) -> Result<(), io::Error> {
    let filenames = if fs::metadata(filename)?.is_dir() {
        vec![]
    } else {
        vec![filename]
    };

    let mut counter = HashMap::new();
    for filename in filenames {
        let infile = BufReader::new(File::open(filename)?);
        for (_lineno, line) in infile.lines().enumerate() {
            if let Ok(line) = line {
                let count = counter.entry(line).or_insert(0);
                *count += 1;
            }
        }
    }

    let mut heap: BinaryHeap<CountItem> = BinaryHeap::new();
    for (k, v) in counter {
        assert!(heap.len() <= ntop as usize);
        if heap.len() == ntop as usize {
            let cur_min = heap.peek().unwrap();
            if cur_min.cnt < v {
                heap.pop();
                heap.push(CountItem{ url: k, cnt: v});
            }
        } else {
            heap.push(CountItem{ url:k, cnt:v});
        }
    }

    println!("file: {}, qsize: {}", filename, heap.len());

    let filename: String = filename.to_owned() + "_cnt";
    let mut outfile = fs::File::create(filename)?;
    while !heap.is_empty() {
        let cur_min = heap.pop().unwrap();
        outfile.write_fmt(
            format_args!("{},{}\n", cur_min.url, cur_min.cnt));
    }

    Ok(())
}

fn reduce_counters(dirname: &str, _ntop: u64)
    -> Result< BinaryHeap<CountItem>, io::Error> {
    let mut heap : BinaryHeap<CountItem> = BinaryHeap::new();
    let paths = fs::read_dir(dirname)?;
    for path in paths {
        //let path = path?.path().to_str()?;
        let path = match path {
            Ok(entry) => entry.path().to_str().unwrap().to_owned() ,
            Err(e) => return Err(e)
        };
        if !path.ends_with("_cnt") { continue; }
        let infile = BufReader::new(File::open(path)?);
        for line in infile.lines() {
            let line = match line {
                Ok(line) => line.trim().to_string(),
                Err(_e) => continue
            };

        }
    }

    Ok(heap)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        eprintln!("Usage: {} filename split-size(mb) ntop", args.get(0).unwrap());
        std::process::exit(1);
    }
    let filename = &args[1];
    let split = args[2].parse::<u64>().unwrap();
    let ntop = args[3].parse::<u64>().unwrap();

    let parted_dir = part(
        filename, split,
        true, false, 10
    ).unwrap();

    let paths = fs::read_dir(parted_dir).unwrap();
    for path in paths {
        let path = path.unwrap().path();
        println!("counting in {}", path.display());
        count(path.to_str().unwrap(), ntop);
    }


}
