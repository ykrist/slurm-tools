#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
    vec,
};

use indexmap::IndexSet;
use labrat::{Deserialize, MailType, Serialize, SlurmResources};
use once_cell::sync::Lazy;
use posix_cli_utils::*;
use slurm_tools::{config_directory, read_json, write_json, IndexMap};
use tempfile::NamedTempFile;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Sbatch {
    Fake,
    Real,
}

impl Sbatch {
    fn binary(&self) -> &'static str {
        match self {
            Sbatch::Fake => "sbatch-fake",
            Sbatch::Real => "sbatch",
        }
    }
}

fn sbatch() -> Sbatch {
    static SBATCH: Lazy<Sbatch> =
        Lazy::new(|| match std::process::Command::new("sbatch").spawn() {
            Ok(_) => Sbatch::Real,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    eprintln!("sbatch was not found on this system, falling back to sbatch-fake.");
                    Sbatch::Fake
                } else {
                    panic!("{}", e)
                }
            }
        });
    *SBATCH
}

fn create_tmp_file() -> Result<NamedTempFile> {
    let d = format!("/run/user/{}", unsafe { libc::geteuid() });
    NamedTempFile::new_in(d)
        .or_else(|_| NamedTempFile::new())
        .context("failed to create temp file")
}

fn read_lines_from_file(path: impl AsRef<Path>, dest: &mut Vec<String>) -> Result<()> {
    let reader = std::fs::File::open(&path)
        .context_read(&path)
        .map(BufReader::new)?;

    for l in reader.lines() {
        dest.push(l.context_read(&path)?);
    }
    Ok(())
}

type Database = HashMap<String, Job>;

fn load_db() -> Result<Database> {
    let p = db_file()?;
    if p.exists() {
        read_json(p)
    } else {
        Ok(Default::default())
    }
}

fn write_db(data: &Database) -> Result<()> {
    write_json(data, db_file()?)
}

fn query_pending_jobs() -> Result<HashSet<String>> {
    if sbatch() == Sbatch::Fake {
        return Ok(Default::default());
    }

    let out = std::process::Command::new("squeue")
        .args(&["-o", "%i", "--array", "--array-unique", "--all"])
        .output()
        .context("failed to spawn squeue")?;

    if out.status.success() {
        let out = String::from_utf8(out.stdout)?;
        Ok(out.lines().skip(1).map(|s| s.to_string()).collect())
    } else {
        bail!(
            "squeue failed with status: {}\n{}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        )
    }
}

fn db_file() -> Result<PathBuf> {
    let mut p = config_directory()?;
    p.push("slurm_jobs.json");
    Ok(p)
}

fn pending_logs_dir() -> Result<PathBuf> {
    let mut p = config_directory()?;
    p.push("pending-logs");
    std::fs::create_dir_all(&p)
        .with_context(|| format!("failed to create slurm-tools config subdirectory: {:?}", &p))?;
    Ok(p)
}

#[derive(Parser, Debug, Clone)]
#[clap(trailing_var_arg(true))]
enum ClArgs {
    Submit {
        #[clap(flatten)]
        options: Options,
        #[clap(required(true), parse(from_str))]
        command: Vec<ArgumentToken>,
    },
    Push,
}

#[derive(Clone, Debug, Args)]
#[clap(trailing_var_arg(true))]
pub struct Options {
    /// Don't submit, just test using sbatch's --test-only flag
    #[clap(short = 'd')]
    dry_run: bool,
    /// Print the generated sbatch scripts and command line flags.
    #[clap(short = 'v')]
    show_script: bool,
    /// Specify an argument list to use as the array index.  Argument list must only contain non-negative integers.
    #[clap(short = 'i')]
    index: Option<usize>,
}

pub use parse::*;
mod parse; 

mod submit;
use submit::*;

mod push;

fn main() -> Result<()> {
    match ClArgs::parse() {
        ClArgs::Push => push::main(),
        ClArgs::Submit { options, command } => submit::main(options, command),
    }
}
