use slurm_tools::join_display;
use std::io::Write as IoWrite;
use std::process::Command;
use std::{
    ffi::OsStr,
    fmt::{Display, Write},
};

use super::*;

#[derive(Clone, Debug)]
enum Arg<'a> {
    Str(&'a str),
    String(String),
    Path(&'a Path),
    PathBuf(PathBuf),
}

impl<'a> AsRef<OsStr> for Arg<'a> {
    fn as_ref(&self) -> &OsStr {
        use Arg::*;
        match self {
            Str(s) => s.as_ref(),
            String(s) => s.as_ref(),
            Path(s) => s.as_ref(),
            PathBuf(s) => s.as_ref(),
        }
    }
}

impl<'a> From<&'a str> for Arg<'a> {
    fn from(s: &'a str) -> Self {
        Arg::Str(s)
    }
}

impl<'a> From<&'a Path> for Arg<'a> {
    fn from(s: &'a Path) -> Self {
        Arg::Path(s)
    }
}

impl<'a> From<String> for Arg<'a> {
    fn from(s: String) -> Self {
        Arg::String(s)
    }
}

impl<'a> From<PathBuf> for Arg<'a> {
    fn from(s: PathBuf) -> Self {
        Arg::PathBuf(s)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ArgEnum)]
pub enum Profile {
    Default,
    Test,
}

#[derive(Clone, Copy)]
struct DisplayCmd<'a>(&'a Command);

impl<'a> Display for DisplayCmd<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.get_program().to_string_lossy())?;
        for arg in self.0.get_args() {
            write!(f, " {}", arg.to_string_lossy())?;
        }
        Ok(())
    }
}

fn query_slurm_resources(commands: &[Vec<String>]) -> Result<Vec<SlurmResources>> {
    let sample_run = match commands.get(0) {
        Some(c) => c,
        None => return Ok(vec![]),
    };

    let mut command_file = create_tmp_file()?;
    let slurm_resource_file = create_tmp_file()?;
    serde_json::to_writer(command_file.as_file_mut(), commands)?;
    command_file.as_file_mut().flush()?;

    let mut cmd = std::process::Command::new(&sample_run[0]);
    cmd.args(&sample_run[1..])
        .arg("--p-slurminfo")
        .arg(command_file.path())
        .arg(slurm_resource_file.path());

    let mut child = cmd.spawn()?;
    let child_status = child.wait().unwrap();
    if !child_status.success() {
        bail!(
            "failed to start Slurm info server: command `{}` exited with code {}",
            DisplayCmd(&cmd),
            child_status
        )
    }

    let resources = serde_json::from_reader(slurm_resource_file.reopen()?)
        .context("failed to deserialize Slurm info")?;
    Ok(resources)
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ArraySlurmResources {
    script: String,
    job_name: Option<String>,
    cpus: usize,
    nodes: usize,
    time: String,
    memory: String,
    mail_user: Option<String>,
    mail_type: Option<Vec<MailType>>,
    constraint: Option<String>,
    exclude: Option<String>,
    nodelist: Option<String>,
    combine_stdout_stderr: bool,
}

trait SbatchArgs {
    fn extend_args<'a>(&'a self, args: &mut Vec<Arg<'a>>);
}

macro_rules! impl_sbatch_args {
    ($t:ty $(,$ignore_fields:ident)*) =>
    {
        impl SbatchArgs for $t {
            /// This lets use get compiler errors if we forget to use one of the resources
            #[deny(unused)]
            fn extend_args<'a>(&'a self, sbatch_args: & mut Vec<Arg<'a>>) {
                let Self {
                    $($ignore_fields: _,)*
                    job_name,
                    cpus,
                    nodes,
                    time,
                    memory,
                    mail_user,
                    mail_type,
                    constraint,
                    exclude,
                    nodelist,
                } = self;

                sbatch_args.push("--cpus-per-task".into());
                sbatch_args.push(cpus.to_string().into());

                sbatch_args.push("--nodes".into());
                sbatch_args.push(nodes.to_string().into());

                sbatch_args.push("--time".into());
                sbatch_args.push(time.as_str().into());

                sbatch_args.push("--mem".into());
                sbatch_args.push(memory.as_str().into());

                if let Some(mail_user) = mail_user {
                    sbatch_args.push("--mail-user".into());
                    sbatch_args.push(mail_user.as_str().into());
                }

                if let Some(mail_type) = mail_type {
                    sbatch_args.push("--mail-type".into());
                    sbatch_args.push(join_display(mail_type, ',').into());
                }

                if let Some(constraint) = constraint {
                    sbatch_args.push("--constraint".into());
                    sbatch_args.push(constraint.as_str().into());
                }

                if let Some(exclude) = exclude {
                    sbatch_args.push("--exclude".into());
                    sbatch_args.push(exclude.as_str().into());
                }
                if let Some(nodelist) = nodelist {
                    sbatch_args.push("--nodelist".into());
                    sbatch_args.push(nodelist.as_str().into());
                }

                if let Some(name) = job_name {
                    sbatch_args.push("--job-name".into());
                    sbatch_args.push(name.as_str().into());
                }
            }
        }


    };
}

impl_sbatch_args!(ArraySlurmResources, script, combine_stdout_stderr);
impl_sbatch_args!(SlurmResources, script, log_err, log_out);

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_err: Option<PathBuf>,
    pub log_out: PathBuf,
    #[serde(skip)]
    pub trace_job: Option<TraceJob>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TraceJob {
    pub command: Vec<String>,
    pub resources: SlurmResources,
}

fn split_job(command: Vec<String>, s: SlurmResources) -> (ArraySlurmResources, Job) {
    let combine_stdout_stderr = s.log_err.is_none();
    let a = ArraySlurmResources {
        script: s.script,
        job_name: s.job_name,
        cpus: s.cpus,
        nodes: s.nodes,
        time: s.time,
        memory: s.memory,
        mail_user: s.mail_user,
        mail_type: s.mail_type,
        constraint: s.constraint,
        exclude: s.exclude,
        nodelist: s.nodelist,
        combine_stdout_stderr,
    };
    let m = Job {
        command,
        log_err: s.log_err,
        log_out: s.log_out,
        trace_job: None,
    };
    (a, m)
}

#[derive(Clone, Debug)]
struct ArrayJob {
    resources: ArraySlurmResources,
    members: Vec<Job>,
}

impl ArrayJob {
    fn new(resources: ArraySlurmResources) -> Self {
        ArrayJob {
            resources,
            members: vec![],
        }
    }
    fn add_member(&mut self, s: Job) {
        self.members.push(s);
    }
    fn assign_indices(self) -> ArrayJobWithIndices {
        let members = self.members.into_iter().enumerate().collect();
        ArrayJobWithIndices {
            resources: self.resources,
            members,
        }
    }
}

#[derive(Clone, Debug)]
struct ArrayJobWithIndices {
    resources: ArraySlurmResources,
    members: IndexMap<usize, Job>,
}

#[derive(Clone, Debug)]
struct SbatchCall<'a> {
    sbatch_args: Vec<Arg<'a>>,
    batch_script: String,
}

fn parse_sbatch_output(bytes: &[u8]) -> Result<usize> {
    let s = std::str::from_utf8(bytes).context("failed to get sbatch output")?;
    let make_context = || format!("failed to parse sbatch output: {:?}", s);
    let s = s.split(';').next().unwrap().trim();
    let id = s.parse().with_context(make_context)?;
    Ok(id)
}

impl<'a> SbatchCall<'a> {
    fn exec(&self, o: &Options) -> Result<Option<usize>> {
        let bin = sbatch().binary();
        let mut file = create_tmp_file()?;

        let mut cmd = std::process::Command::new(bin);
        cmd.args(&self.sbatch_args);
        if o.dry_run {
            cmd.arg("--test-only");
        }

        cmd.arg(file.path());

        if o.show_script {
            println!("# {:-^80}", " COMMAND ");
            print!("{}", bin);
            for arg in cmd.get_args() {
                print!(" {}", arg.to_str().unwrap())
            }
            println!();
            println!("# {:-^80}", format!(" SCRIPT ({}) ", file.path().display()));
            print!("{}", self.batch_script);
        }

        if o.dry_run && cmd.get_args().any(|s| s == "--dependency") {
            // --dependency with a bogus job ID will cause sbatch to fail.
            return Ok(None);
        }

        {
            let f = file.as_file_mut();
            f.write_all(self.batch_script.as_bytes())?;
            f.flush()?;
        }

        let output = cmd
            .output()
            .context("failed to find sbatch or sbatch-fake")?;

        if !output.status.success() {
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            bail!("sbatch failed with exit code {}", output.status)
        }

        if o.dry_run {
            Ok(None)
        } else {
            let i = parse_sbatch_output(&output.stdout)?;
            eprintln!("Submitted job {}", i);
            Ok(Some(i))
        }
    }
}

impl ArrayJobWithIndices {
    fn new(resources: ArraySlurmResources) -> Self {
        ArrayJobWithIndices {
            resources,
            members: Default::default(),
        }
    }

    fn new_with_member(resources: ArraySlurmResources, index: usize, member: Job) -> Self {
        let mut a = Self::new(resources);
        a.add_member(index, member).unwrap();
        a
    }

    fn add_member(&mut self, index: usize, s: Job) -> std::result::Result<(), Job> {
        if self.members.contains_key(&index) {
            Err(s)
        } else {
            self.members.insert(index, s);
            Ok(())
        }
    }

    fn job(&self, p: Profile) -> Result<SbatchCall> {
        use std::fmt::Write;

        let mut stdout = pending_logs_dir()?;
        stdout.push("%A_%a.stdout");

        let mut sbatch_args = vec![
            "--parsable".into(),
            "--array".into(),
            fmt_array_indices(self.members.keys().copied()).into(),
        ];
        if !self.resources.combine_stdout_stderr {
            sbatch_args.push("--error".into());
            sbatch_args.push(stdout.with_extension("stderr").into())
        }
        sbatch_args.push("--output".into());
        sbatch_args.push(stdout.into());
        self.resources.extend_args(&mut sbatch_args);

        let mut batch_script = self.resources.script.clone();
        writeln!(&mut batch_script, "\ncase $SLURM_ARRAY_TASK_ID in");
        for (i, m) in &self.members {
            write!(&mut batch_script, "{})\n    ", i);
            for c in &m.command {
                write!(&mut batch_script, " {}", c);
            }
            writeln!(&mut batch_script, "\n;;");
        }
        writeln!(&mut batch_script, "esac");
        Ok(SbatchCall {
            sbatch_args,
            batch_script,
        })
    }
}

impl TraceJob {
    fn job(&self, parent_id: usize, index: usize) -> SbatchCall {
        use std::fmt::Write;

        let mut sbatch_args = vec![
            "--parsable".into(),
            "--dependency".into(),
            format!("afternotok:{}_{}", parent_id, index).into(),
            "--output".into(),
            self.resources.log_out.as_path().into(),
        ];

        if let Some(ref p) = self.resources.log_err {
            sbatch_args.push("--error".into());
            sbatch_args.push(p.as_path().into())
        }
        self.resources.extend_args(&mut sbatch_args);

        let mut batch_script = self.resources.script.clone();
        writeln!(
            &mut batch_script,
            "\nif [[ `sacct -n -X -j {}_{} -o state --parsable2` == 'FAILED' ]] ; then",
            parent_id, index
        );
        write!(&mut batch_script, "   ");
        for c in &self.command {
            write!(&mut batch_script, " {}", c);
        }
        writeln!(&mut batch_script, "\nelse");
        writeln!(
            &mut batch_script,
            "    echo \"Trace job skipped: triggering job was not in FAILED state.\" >&2"
        );
        writeln!(&mut batch_script, "fi");

        SbatchCall {
            batch_script,
            sbatch_args,
        }
    }
}

fn submit_job(
    options: &Options,
    mut array_job: ArrayJobWithIndices,
    db: &mut Database,
) -> Result<()> {
    array_job.members.sort_keys();

    let array_id = array_job.job(options.profile)?.exec(options)?;

    for (i, mut job) in array_job.members {
        if let Some(trace) = job.trace_job.take() {
            trace.job(array_id.unwrap_or(0), i).exec(options)?;
        }

        if !options.dry_run {
            db.insert(format!("{}_{}", array_id.unwrap(), i), job);
        }
    }
    Ok(())
}

fn submit_jobs(
    options: &Options,
    jobs: impl IntoIterator<Item = ArrayJobWithIndices>,
) -> Result<()> {
    let mut db = load_db()?;

    for array_job in jobs {
        if let Err(e) = submit_job(options, array_job, &mut db) {
            write_db(&db)?;
            return Err(e);
        }
    }

    write_db(&db)
}

pub fn group_and_submit_jobs(
    options: &Options,
    jobs: impl IntoIterator<Item = (ArraySlurmResources, Job)>,
    array_indices: Option<Vec<usize>>,
) -> Result<()> {
    if let Some(inds) = array_indices {
        let mut groups: HashMap<ArraySlurmResources, Vec<ArrayJobWithIndices>> = Default::default();

        for ((a, member), i) in jobs.into_iter().zip(inds) {
            if let Some(g) = groups.get_mut(&a) {
                let mut member = Some(member);
                for g in g.iter_mut() {
                    match g.add_member(i, member.take().unwrap()) {
                        Ok(()) => break,
                        Err(m) => member = Some(m),
                    }
                }
                if let Some(m) = member {
                    g.push(ArrayJobWithIndices::new_with_member(a, i, m));
                }
            } else {
                groups.insert(
                    a.clone(),
                    vec![ArrayJobWithIndices::new_with_member(a, i, member)],
                );
            }
        }

        submit_jobs(options, groups.into_values().flatten())
    } else {
        let mut groups: HashMap<ArraySlurmResources, ArrayJob> = Default::default();
        for (a, member) in jobs {
            if let Some(g) = groups.get_mut(&a) {
                g.add_member(member)
            } else {
                let mut job = ArrayJob::new(a.clone());
                job.add_member(member);
                groups.insert(a, job);
            }
        }

        submit_jobs(options, groups.into_values().map(ArrayJob::assign_indices))
    }
}

pub fn main(options: Options, command: Vec<ArgumentToken>) -> Result<()> {
    let (template, args) = parse_command(command)?;
    let array_indices = options
        .index
        .map(|i| {
            args.as_ref()
                .ok_or_else(|| anyhow!("At least one argument list is required with "))?
                .indices(i)
        })
        .transpose()?;
    let mut commands = substitute_args(&template, args);

    if options.profile == Profile::Test {
        let k = commands.len();
        commands.extend_from_within(..);
        for c in &mut commands[..k] {
            c.push("--slurmprofile".to_string());
            c.push("test".to_string());
        }
        for c in &mut commands[k..] {
            c.push("--slurmprofile".to_string());
            c.push("trace".to_string());
        }
        let mut resources = query_slurm_resources(&commands)?;
        let mut trace_jobs = resources
            .split_off(k)
            .into_iter()
            .zip(commands.split_off(k))
            .map(|(resources, command)| TraceJob { resources, command });

        let jobs = commands
            .into_iter()
            .zip(resources)
            .enumerate()
            .map(|(k, (cmd, res))| {
                let (arr, mut j) = split_job(cmd, res);
                j.trace_job = Some(trace_jobs.next().unwrap());
                (arr, j)
            });

        group_and_submit_jobs(&options, jobs, array_indices)?;
    } else {
        let resources = query_slurm_resources(&commands)?;
        let jobs = commands
            .into_iter()
            .zip(resources)
            .map(|(cmd, res)| split_job(cmd, res));
        group_and_submit_jobs(&options, jobs, array_indices)?;
    }

    Ok(())
}

fn fmt_array_indices(vals: impl IntoIterator<Item = usize>) -> String {
    let mut iter = vals.into_iter();
    let mut prev = iter.next().expect("empty iterator");
    let mut group_start = prev;
    let mut s = String::new();

    fn fmt_group(s: &mut String, start: usize, end: usize) {
        use std::fmt::Write;
        match end - start {
            0 => write!(s, "{}", start),
            1 => write!(s, "{},{}", start, end),
            _ => write!(s, "{}-{}", start, end),
        };
    }

    while let Some(n) = iter.next() {
        assert!(prev < n);
        if prev + 1 < n {
            fmt_group(&mut s, group_start, prev);
            s.push(',');
            group_start = n;
        }
        prev = n;
    }
    fmt_group(&mut s, group_start, prev);
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_array_indices() {
        assert_eq!(&fmt_array_indices([0]), "0");
        assert_eq!(&fmt_array_indices([0, 1]), "0,1");
        assert_eq!(&fmt_array_indices([0, 1, 2]), "0-2");
        assert_eq!(&fmt_array_indices([0, 1, 2, 4]), "0-2,4");
        assert_eq!(&fmt_array_indices([0, 1, 2, 4, 5]), "0-2,4,5");
        assert_eq!(&fmt_array_indices([0, 1, 2, 4, 5, 6]), "0-2,4-6");
        assert_eq!(&fmt_array_indices([0, 4, 5, 6]), "0,4-6");
        assert_eq!(&fmt_array_indices([0, 1, 4, 5, 6]), "0,1,4-6");
        assert_eq!(&fmt_array_indices([0, 1, 3, 4, 5, 7, 8, 9]), "0,1,3-5,7-9");
    }
}
