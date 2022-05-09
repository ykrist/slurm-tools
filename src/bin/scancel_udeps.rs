use posix_cli_utils::*;
use std::process::*;

/// Cancels all Slurm jobs the current user has which are in the DependencyNeverSatisfied state
#[derive(Parser, Clone, Debug)]
struct Args {}

fn make_parse_error(s: &str) -> anyhow::Error {
    anyhow!("failed to parse squeue output line: {:?}", s)
}

fn main() -> Result<()> {
    reset_sigpipe();
    Args::parse();
    let user = whoami::username_os();
    let squeue = Command::new("squeue")
        .args(&["-h", "-r", "-O", "jobid:50,reason:80", "-u"])
        .arg(&user)
        .stderr(Stdio::inherit())
        .output()?;

    if !squeue.status.success() {
        bail!("squeue failed with exit code {}", squeue.status)
    }

    let output = std::str::from_utf8(&squeue.stdout)?;
    let mut cancel = vec![];
    for line in output.lines() {
        let mut l = line.trim().split_whitespace();
        let job_id = l.next().ok_or_else(|| make_parse_error(line))?;
        let state = l.next().ok_or_else(|| make_parse_error(line))?;
        if l.next().is_some() {
            return Err(make_parse_error(line));
        }
        if state == "DependencyNeverSatisfied" {
            cancel.push(job_id);
        }
    }
    if cancel.is_empty() {
        println!("no jobs cancelled");
    } else {
        let status = Command::new("scancel")
            .arg("-u")
            .arg(&user)
            .args(&cancel)
            .status()?;
        if !status.success() {
            bail!("scancel failed with exit code {}", status)
        }
        println!("cancelled {} jobs", cancel.len());
    }

    Ok(())
}
