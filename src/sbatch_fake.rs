use std::time::SystemTime;

use posix_cli_utils::*;

#[derive(Parser)]
#[clap(trailing_var_arg(true))]
struct Args {
    #[clap(long, short = 'A')]
    account: Option<String>,

    #[clap(long)]
    acctg_freq: Option<String>,

    #[clap(long, short = 'a')]
    array: Option<String>,

    #[clap(long)]
    batch: Option<String>,

    #[clap(long)]
    bb: Option<String>,

    #[clap(long)]
    bbf: Option<String>,

    #[clap(long, short = 'b')]
    begin: Option<String>,

    #[clap(long, short = 'D')]
    chdir: Option<String>,

    #[clap(long)]
    cluster_constraint: Option<String>,

    #[clap(long, short = 'M')]
    clusters: Option<String>,

    #[clap(long)]
    comment: Option<String>,

    #[clap(long, short = 'C')]
    constraint: Option<String>,

    #[clap(long)]
    container: Option<String>,

    #[clap(long)]
    contiguous: bool,

    #[clap(long, short = 'S')]
    core_spec: Option<String>,

    #[clap(long)]
    cores_per_socket: Option<String>,

    #[clap(long)]
    cpu_freq: Option<String>,

    #[clap(long)]
    cpus_per_gpu: Option<String>,

    #[clap(long, short = 'c')]
    cpus_per_task: Option<String>,

    #[clap(long)]
    deadline: Option<String>,

    #[clap(long)]
    delay_boot: Option<String>,

    #[clap(long, short = 'd')]
    dependency: Option<String>,

    #[clap(long, short = 'm')]
    distribution: Option<String>,

    #[clap(long, short = 'e')]
    error: Option<String>,

    #[clap(long, short = 'x')]
    exclude: Option<String>,

    #[clap(long)]
    exclusive: Option<String>,

    #[clap(long)]
    export: Option<String>,

    #[clap(long)]
    export_file: Option<String>,

    #[clap(long, short = 'B')]
    extra_node_info: Option<String>,

    #[clap(long)]
    get_user_env: Option<String>,

    #[clap(long)]
    gid: Option<String>,

    #[clap(long)]
    gpu_bind: Option<String>,

    #[clap(long)]
    gpu_freq: Option<String>,

    #[clap(long, short = 'G')]
    gpus: Option<String>,

    #[clap(long)]
    gpus_per_node: Option<String>,

    #[clap(long)]
    gpus_per_socket: Option<String>,

    #[clap(long)]
    gpus_per_task: Option<String>,

    #[clap(long)]
    gres: Option<String>,

    #[clap(long)]
    gres_flags: Option<String>,

    #[clap(long, short = 'h')]
    help: bool,

    #[clap(long)]
    hint: Option<String>,

    #[clap(long, short = 'H')]
    hold: bool,

    #[clap(long)]
    ignore_pbs: bool,

    #[clap(long, short = 'i')]
    input: Option<String>,

    #[clap(long, short = 'J')]
    job_name: Option<String>,

    #[clap(long)]
    kill_on_invalid_dep: Option<String>,

    #[clap(long, short = 'L')]
    licenses: Option<String>,

    #[clap(long)]
    mail_type: Option<String>,

    #[clap(long)]
    mail_user: Option<String>,

    #[clap(long)]
    mcs_label: Option<String>,

    #[clap(long)]
    mem: Option<String>,

    #[clap(long)]
    mem_bind: Option<String>,

    #[clap(long)]
    mem_per_cpu: Option<String>,

    #[clap(long)]
    mem_per_gpu: Option<String>,

    #[clap(long)]
    mincpus: Option<String>,

    #[clap(long)]
    network: Option<String>,

    #[clap(long)]
    nice: Option<String>,

    #[clap(long, short = 'k')]
    no_kill: bool,

    #[clap(long)]
    no_requeue: bool,

    #[clap(long, short = 'F')]
    nodefile: Option<String>,

    #[clap(long, short = 'w')]
    nodelist: Option<String>,

    #[clap(long, short = 'N')]
    nodes: Option<String>,

    #[clap(long, short = 'n')]
    ntasks: Option<String>,

    #[clap(long)]
    ntasks_per_core: Option<String>,

    #[clap(long)]
    ntasks_per_gpu: Option<String>,

    #[clap(long)]
    ntasks_per_node: Option<String>,

    #[clap(long)]
    ntasks_per_socket: Option<String>,

    #[clap(long)]
    open_mode: Option<String>,

    #[clap(long, short = 'o')]
    output: Option<String>,

    #[clap(long, short = 'O')]
    overcommit: bool,

    #[clap(long, short = 's')]
    oversubscribe: bool,

    #[clap(long)]
    parsable: bool,

    #[clap(long, short = 'p')]
    partition: Option<String>,

    #[clap(long)]
    power: Option<String>,

    #[clap(long)]
    priority: Option<String>,

    #[clap(long)]
    profile: Option<String>,

    #[clap(long)]
    propagate: Option<String>,

    #[clap(long, short = 'q')]
    qos: Option<String>,

    #[clap(long, short = 'Q')]
    quiet: bool,

    #[clap(long)]
    reboot: bool,

    #[clap(long)]
    requeue: bool,

    #[clap(long)]
    reservation: Option<String>,

    #[clap(long)]
    signal: Option<String>,

    #[clap(long)]
    sockets_per_node: Option<String>,

    #[clap(long)]
    spread_job: bool,

    #[clap(long)]
    switches: Option<String>,

    #[clap(long)]
    test_only: bool,

    #[clap(long)]
    thread_spec: Option<String>,

    #[clap(long)]
    threads_per_core: Option<String>,

    #[clap(long, short = 't')]
    time: Option<String>,

    #[clap(long)]
    time_min: Option<String>,

    #[clap(long)]
    tmp: Option<String>,

    #[clap(long)]
    uid: Option<String>,

    #[clap(long)]
    usage: bool,

    #[clap(long)]
    use_min_nodes: bool,

    #[clap(long, short = 'v')]
    verbose: bool,

    #[clap(long, short = 'V')]
    version: bool,

    #[clap(long, short = 'W')]
    wait: bool,

    #[clap(long)]
    wait_all_nodes: Option<String>,

    #[clap(long)]
    wckey: Option<String>,

    #[clap(long)]
    wrap: Option<String>,

    script: String,

    script_args: Vec<String>,
}

fn get_fake_id() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}
fn main() -> Result<()> {
    let args = Args::parse();
    {
        let script = std::fs::read_to_string(&args.script).context_read(&args.script)?;
        if script.is_empty() {
            bail!("Batch script is empty!")
        }
        if script.chars().all(char::is_whitespace) {
            bail!("Batch script contains only whitespace!")
        }
        if !script.starts_with("#!") {
            bail!("This does not look like a batch script.  The first line must start with followed by the path to an interpreter")
        }
    }

    if args.parsable {
        println!("{};partition-name", get_fake_id());
    }

    Ok(())
}
