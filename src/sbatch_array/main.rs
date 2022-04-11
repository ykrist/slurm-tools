#![allow(unused)]

use std::{
    collections::{HashMap, HashSet},
    fs::DirEntry,
    io::{BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::Stdio,
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
    static SBATCH: Lazy<Sbatch> = Lazy::new(|| {
        let s = std::process::Command::new("sbatch")
            .arg("-V")
            .stderr(Stdio::null())
            .stdout(Stdio::null())
            .stdin(Stdio::null())
            .status();

        match s {
            Ok(_) => Sbatch::Real,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    eprintln!("sbatch was not found on this system, falling back to sbatch-fake.");
                    Sbatch::Fake
                } else {
                    panic!("{}", e)
                }
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
#[clap(trailing_var_arg(true), infer_subcommands(true))]
enum ClArgs {
    /// Submit a new set of jobs
    Submit {
        #[clap(flatten)]
        options: Options,
        /// Target command to run. It should be a shell command,
        /// followed by arguments lists, each preceded by SEP,
        /// which is one of: :::, :::+, ::::+ or ::::. To read an
        /// argument list from files instead, use :::: or ::::+. The `+`
        /// variants will 'zip' the following argument list with
        /// the previous argument list. These may not be used as
        /// the first SEP. See GNU parallel documentation for
        /// details on how to construct argument lists. The target
        /// command must accept a `--p-slurminfo R W` switch, and when
        /// given this option, should read command line arguments
        /// from the R file and write the necessary Slurm information the
        /// the W file befor exiting with 0 return code.
        #[clap(required(true), parse(from_str))]
        command: Vec<ArgumentToken>,
    },
    /// Push (move) log files from completed jobs to their destinations
    Push,
    /// List the Slurm IDs of all jobs currently managed by `sbatch-array`
    List,
    /// Dump the database in JSON form to STDOUT.
    Dump,
    /// Delete the database and any logs, useful for cleaning up corrupted state.
    Clear {
        /// Skip confirmation prompt
        #[clap(long)]
        force: bool,
    },
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

    /// Job profile to use.  `default` files jobs with `--slurm-profile default` passed to the target.
    /// `test` files two jobs: the first passes `--slurm-profile test` to the binary and the second,
    /// which is only run if the first job fails, passes `--slurm-profile trace`.  This is mainly
    /// intended for debugging expensive jobs.
    #[clap(short, arg_enum, default_value_t=Profile::Default)]
    profile: Profile,
}

pub use parse::*;
mod parse;

mod submit;
use submit::*;

mod push;

fn delete_db_and_logs() -> Result<()> {
    let db = db_file()?;
    if db.exists() {
        std::fs::remove_file(db)?;
    }
    for f in std::fs::read_dir(pending_logs_dir()?)? {
        let f = f?;
        if f.file_type()?.is_file() {
            std::fs::remove_file(&f.path())?;
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    match ClArgs::parse() {
        ClArgs::Push => push::main(),
        ClArgs::Submit { options, command } => submit::main(options, command),
        ClArgs::Dump => {
            let file = db_file()?;
            let data = std::fs::read(&file).context_read(&file)?;
            std::io::stdout().lock().write_all(&data);
            Ok(())
        }
        ClArgs::List => {
            let mut ids: Vec<_> = load_db()?.into_keys().collect();
            ids.sort();
            for i in ids {
                println!("{}", i);
            }
            Ok(())
        }
        ClArgs::Clear { force } => {
            if !force {
                let mut buf = String::new();
                eprint!("WARNING: This will delete all managed jobs and their output.  Continue [y/n]? ");
                loop {
                    buf.clear();
                    std::io::stdin().read_line(&mut buf)?;
                    buf.make_ascii_lowercase();

                    match buf.trim() {
                        "y" | "yes" => break,
                        "n" | "no" => return Ok(()),
                        _ => eprint!("answer [y]es or [n]o: "),
                    }
                }
            }
            delete_db_and_logs()
        }
    }
}
