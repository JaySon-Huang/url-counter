use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::hash::Hasher;

use rand::prelude::*;

use fasthash::{murmur3::Hasher32, FastHasher};
use fasthash::murmur3::Hash128_x64;

fn part(filename: &str, splited_mb: u64,
        preserve_ori_file: bool,
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
    if fs::metadata(&dirname)?.is_dir() {
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
            let prefix = rng.gen_range(0, skew_splited_factor);
            hasher.write(prefix);
            hasher.write(line.as_bytes());
            (hasher.finish() % num_to_split) as usize
        } else {
            let mut hasher = Hasher32::new();
            hasher.write(line.as_bytes());
            (hasher.finish() % num_to_split) as usize
        };
        let mut fp = &outfiles[h];
        fp.write(line.as_bytes())?;
        outfiles_size[h] += line.len();
    }

    return std::result::Result::Ok(dirname);
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

}
