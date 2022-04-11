use posix_cli_utils::*;
use std::process::*;
use std::os::unix::process::CommandExt;

#[derive(Parser, Clone, Debug)]
struct ClArgs {
    #[clap(short, long, default_value="1")]
    cpus: String,
    #[clap(short='m', long="mem", default_value="8G")]
    memory: String,
    #[clap(short, long, default_value="0:10:00")]
    time: String,
    #[clap(short='C', long)]
    constraint: Option<String>,
}

fn main() -> Result<()> {
    let args = ClArgs::parse();
    let mut cmd = Command::new("srun");
    cmd.arg("--time");
    cmd.arg(&args.time);
    cmd.arg("-c");
    cmd.arg(&args.cpus);
    cmd.arg("-N");
    cmd.arg("1");
    cmd.arg("--mem");
    cmd.arg(&args.memory);

    if let Some(ref c) = args.constraint {
        cmd.arg("--constraint");
        cmd.arg(c);
    
    }
    cmd.arg("--pty");
    if let Ok(shell) = std::env::var("SHELL") {
        cmd.arg(shell);
    } else {
        cmd.arg("/bin/bash");
    }
    
    eprint!("running: srun");
    for c in cmd.get_args() {
        eprint!(" {}", c.to_str().unwrap());
    }
    eprintln!();
    
    Err(cmd.exec().into())
}