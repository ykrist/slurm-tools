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
use slurm_tools::{write_json, IndexMap};
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
    static SBATCH : Lazy<Sbatch> = Lazy::new(|| 
    match std::process::Command::new("sbatch").spawn() {
        Ok(_) => Sbatch::Real,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                eprintln!("sbatch was not found on this system, falling back to sbatch-fake.");
                Sbatch::Fake
            } else {
                panic!("{}", e)
            }
        }, 
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum ArgumentToken {
    Arg(String),
    Sep { files: bool, zip: bool },
    Index(usize),
}

impl From<ArgumentToken> for String {
    fn from(t: ArgumentToken) -> Self {
        use ArgumentToken::*;
        match t {
            Arg(s) => s,
            Index(i) => format!("{{{}}}", i),
            Sep { files, zip } => {
                let mut s = ":::".to_string();
                if files {
                    s.push(':')
                }
                if zip {
                    s.push('+')
                }
                s
            }
        }
    }
}

impl ArgumentToken {
    fn is_sep(&self) -> bool {
        matches!(self, ArgumentToken::Sep { .. })
    }
}

impl From<&str> for ArgumentToken {
    fn from(s: &str) -> Self {
        use ArgumentToken::*;
        match s {
            ":::" => Sep {
                files: false,
                zip: false,
            },
            ":::+" => Sep {
                files: false,
                zip: true,
            },
            "::::" => Sep {
                files: true,
                zip: false,
            },
            "::::+" => Sep {
                files: true,
                zip: true,
            },
            s => {
                if let Some(s) = s.strip_prefix('{') {
                    if let Some(s) = s.strip_suffix('}') {
                        if let Ok(i) = s.parse() {
                            return Index(i);
                        }
                    }
                }
                Arg(s.to_string())
            }
        }
    }
}

#[derive(Clone, Debug, Parser)]
#[clap(trailing_var_arg(true))]
struct ClArgs {
    #[clap(flatten)]
    options: Options,
    #[clap(required(true), parse(from_str))]
    command: Vec<ArgumentToken>,
}

#[derive(Clone, Debug, Args)]
#[clap(trailing_var_arg(true))]
struct Options {
    #[clap(short = 'd')]
    dry_run: bool,
    #[clap(short = 'v')]
    show_script: bool,
    #[clap(short = 'i')]
    index: Option<usize>,
}

fn get_template(command: &mut Vec<ArgumentToken>) -> Result<Vec<ArgumentToken>> {
    let mut template_end = command.len();
    let mut n_args = 0;
    for (k, c) in command.iter().enumerate().rev() {
        if c.is_sep() {
            template_end = k;
            n_args += 1;
        }
    }
    for c in &command[..template_end] {
        if let &ArgumentToken::Index(i) = c {
            if i >= n_args {
                bail!(
                    "Index given is {} but only {} argument lists given",
                    i,
                    n_args
                );
            }
        }
    }
    let mut tmpl = command.split_off(template_end);
    std::mem::swap(&mut tmpl, command);
    for i in (0..n_args).map(ArgumentToken::Index) {
        if !tmpl.contains(&i) {
            tmpl.push(i)
        }
    }
    Ok(tmpl)
}

fn parse_command(
    mut command: Vec<ArgumentToken>,
) -> Result<(Vec<ArgumentToken>, Option<ArgumentList>)> {
    let template = get_template(&mut command)?;
    if command.is_empty() {
        return Ok((template, None));
    }

    let mut zip_with_prev = vec![];
    let mut arglists = vec![];

    let starts = command
        .iter()
        .enumerate()
        .filter_map(|(k, s)| if s.is_sep() { Some(k) } else { None });
    let ends = starts.clone().skip(1).chain([command.len()]);

    for (i, j) in starts.zip(ends) {
        let mut arglist = vec![];
        let (files, zip) = match &command[i] {
            &ArgumentToken::Sep { files, zip } => (files, zip),
            _ => unreachable!(),
        };
        if zip_with_prev.is_empty() && zip {
            bail!("first argument list cannot be zipped with previous")
        }

        for t in &command[(i + 1)..j] {
            let t = t.clone();
            if files {
                read_lines_from_file(String::from(t), &mut arglist)?;
            } else {
                arglist.push(t.clone().into());
            }
        }
        arglists.push(arglist);
        zip_with_prev.push(zip);
    }
    let arg_iter = ArgumentList::new(&zip_with_prev, arglists);
    Ok((template, Some(arg_iter)))
}

fn substitute_args(templ: &[ArgumentToken], arglist: Option<ArgumentList>) -> Vec<Vec<String>> {
    let arglist = match arglist {
        Some(a) => a,
        None => return vec![templ.iter().cloned().map(String::from).collect()]
    };

    arglist
        .iter()
        .map(|args| {
            let mut v = Vec::with_capacity(templ.len());
            for t in templ {
                match t {
                    ArgumentToken::Sep { .. } => unreachable!(),
                    ArgumentToken::Arg(s) => v.push(s.clone()),
                    ArgumentToken::Index(i) => v.push(args[*i].clone()),
                }
            }
            v
        })
        .collect()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct ZipGroup {
    start: usize,
    end: usize,
    members: usize,
}

#[derive(Clone, Debug)]
struct ArgumentList {
    groups: Vec<ZipGroup>,
    arglists: Vec<Vec<String>>,
}

#[derive(Clone, Debug)]
struct ArgumentListIter<'a> {
    index_buf: Vec<usize>,
    l: &'a ArgumentList
}

#[derive(Clone, Debug)]
struct ArgumentListIndices {
    inner_period: usize,
    outer_period: usize,
    array_indices: Vec<usize>,
}


impl ArgumentList {
    fn new(zip_with_previous: &[bool], mut arglists: Vec<Vec<String>>) -> Self {
        assert_eq!(zip_with_previous.len(), arglists.len());
        assert!(!arglists.is_empty());
        let mut groups = vec![];
        let mut start = 0;
        let mut members = arglists[0].len();
        debug_assert_eq!(zip_with_previous[0], false);
        for (end, (z, args)) in zip_with_previous.iter().zip(&arglists).enumerate().skip(1) {
            if !z {
                groups.push(ZipGroup {
                    start,
                    end,
                    members,
                });
                start = end;
                members = args.len();
            } else {
                members = members.min(args.len());
            };
        }
        groups.push(ZipGroup {
            start,
            end: arglists.len(),
            members,
        });
        for g in &groups {
            for k in g.start..g.end {
                arglists[k].truncate(g.members);
            }
        }
        ArgumentList {
            groups,
            arglists,
        }
    }

    fn iter(&self) -> ArgumentListIter {
        let index_buf = vec![0; self.groups.len()];
        ArgumentListIter { index_buf, l: self }
    }

    fn indices(&self, i: usize) -> Result<Vec<usize>> {
        let array_inds: Result<IndexSet<usize>> = {
            let args = self.arglists.get(i).ok_or_else(
                || anyhow!("Argument with index {} reference but only {} argument lists given", 
                i,
                    self.arglists.len()
                ))?;
            args.iter().map(|s| s.parse().map_err(|_| anyhow!("Index argument list must be non-negative integers."))).collect()
        };
        let mut array_inds = array_inds?;
        array_inds.sort();

        let inner_period = self.groups[(i+1)..].iter().fold(1 , |acc, x| acc * x.members);
        let outer_period = self.groups[..i].iter().fold(1 , |acc, x| acc * x.members);
        let n = array_inds.len() * inner_period * outer_period;

        let mut inds = Vec::with_capacity(n);
        for _ in 0..outer_period {
            for &k in &array_inds {
                for _ in 0..inner_period {
                    inds.push(k)
                }
            }
        }
        Ok(inds)
    }
}

impl<'a> ArgumentListIter<'a> {
    fn inc_indices(&mut self) {
        for k in (0..self.index_buf.len()).rev() {
            self.index_buf[k] += 1;
            if self.index_buf[k] >= self.l.groups[k].members && k > 0 {
                self.index_buf[k] = 0;
            } else {
                return;
            }
        }
    }
}
impl<'a> Iterator for ArgumentListIter<'a> {
    type Item = Vec<String>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index_buf.is_empty() {
            return None;
        }
        if self.index_buf[0] >= self.l.groups[0].members {
            return None;
        }

        let mut output = Vec::with_capacity(self.l.arglists.len());

        for (&k, group) in self.index_buf.iter().zip(&self.l.groups) {
            for i in group.start..group.end {
                output.push(self.l.arglists[i][k].clone())
            }
        }

        debug_assert_eq!(output.len(), self.l.arglists.len());
        self.inc_indices();
        Some(output)
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
        bail!("command exited with code {}", child_status)
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
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArrayJobMember {
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_err: Option<PathBuf>,
    pub log_out: PathBuf,
}

fn split_job(command: Vec<String>, s: SlurmResources) -> (ArraySlurmResources, ArrayJobMember) {
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
    };
    let m = ArrayJobMember {
        command,
        log_err: s.log_err,
        log_out: s.log_out,
    };
    (a, m)
}

#[derive(Clone, Debug)]
struct ArrayJob {
    resources: ArraySlurmResources,
    members: Vec<ArrayJobMember>,
}

impl ArrayJob {
    fn new(resources: ArraySlurmResources) -> Self {
        ArrayJob {
            resources,
            members: vec![],
        }
    }
    fn add_member(&mut self, s: ArrayJobMember) {
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
    members: IndexMap<usize, ArrayJobMember>,
}

#[derive(Clone, Debug)]
struct SubmitJob {
    sbatch_args: Vec<String>,
    batch_script: String,
}

impl SubmitJob {
    fn submit(&self, o: &Options) -> Result<()> {
        let bin = sbatch().binary();
        let mut file = create_tmp_file()?;

        let mut cmd = std::process::Command::new(bin);
        cmd.args(&self.sbatch_args);
        if o.dry_run {
            cmd.arg("--test-only");
        }
        
        cmd.arg(file.path());
        let status = cmd.status().context("failed to find sbatch or sbatch-fake")?;

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
        file.as_file_mut().write_all(self.batch_script.as_bytes())?;

        if !status.success() {
            bail!("submission failed with exit code {}", status)
        }

        Ok(())
    }
}

impl ArrayJobWithIndices {
    fn new(resources: ArraySlurmResources) -> Self {
        ArrayJobWithIndices {
            resources,
            members: Default::default(),
        }
    }

    fn new_with_one(resources: ArraySlurmResources, index: usize, member: ArrayJobMember) -> Self {
        let mut a = Self::new(resources);
        a.add_member(index, member).unwrap();
        a
    }

    fn add_member(
        &mut self,
        index: usize,
        s: ArrayJobMember,
    ) -> std::result::Result<(), ArrayJobMember> {
        if self.members.contains_key(&index) {
            Err(s)
        } else {
            self.members.insert(index, s);
            Ok(())
        }
    }

    fn job(&self) -> SubmitJob {
        use std::fmt::Write;
        let mut batch_script = self.resources.script.clone();

        writeln!(&mut batch_script, "\ncase $SLURM_ARRAY_TASK_ID in");

        for (i, m) in &self.members {
            write!(&mut batch_script, "{})\n    ", i);
            for c in &m.command {
                write!(&mut batch_script, " {}", c);
            }
            writeln!(&mut batch_script, "\n;;");
        }

        SubmitJob { 
            sbatch_args: vec![], 
            batch_script,
        }
    }

    fn trace_jobs(&self, slurm_jobid: &str) -> impl Iterator<Item = SubmitJob> {
        unimplemented!();
        None.into_iter()
    }
}


fn group_and_submit_jobs(
    options: &Options,
    commands: Vec<Vec<String>>,
    resources: Vec<SlurmResources>,
    array_indices: Option<Vec<usize>>,
) -> Result<()> {
    fn submit_jobs(options: &Options, jobs: impl IntoIterator<Item=ArrayJobWithIndices>) -> Result<()> {
        for job in jobs {
            job.job().submit(options)?;
        }
        Ok(())
    }

    let jobs = commands
        .into_iter()
        .zip(resources)
        .map(|(c, r)| split_job(c, r));

    if let Some(inds) = array_indices {
        let mut groups: HashMap<ArraySlurmResources, Vec<ArrayJobWithIndices>> = Default::default();

        for ((a, member), i) in jobs.zip(inds) {
            if let Some(g) = groups.get_mut(&a) {
                let mut member = Some(member);
                for g in g.iter_mut() {
                    match g.add_member(i, member.take().unwrap()) {
                        Ok(()) => break,
                        Err(m) => member = Some(m),
                    }
                }
                if let Some(m) = member {
                    g.push(ArrayJobWithIndices::new_with_one(a, i, m));
                }
            } else {
                groups.insert(a.clone(), vec![ArrayJobWithIndices::new_with_one(a, i, member)]);
            }
        }
        submit_jobs(options, 
            groups.into_values()
            .flatten()
            .map(|mut a| {
                a.members.sort_keys();
                a
            })
        )
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

fn query_pending_jobs() -> Result<HashSet<String>> {
    if sbatch() == Sbatch::Fake { return Ok(Default::default()) }

    let out = std::process::Command::new("squeue")
        .args(&["-o", "%50i", "-r"])
        .output()
        .context("failed to spawn squeue")?;

    if out.status.success() {
        let out = String::from_utf8(out.stdout)?;
        Ok(out.lines().skip(1).map(|s| s.to_string()).collect())
    } else {
        bail!("squeue failed with status: {}\n{}", out.status, String::from_utf8_lossy(&out.stderr))
    }
}

fn main() -> Result<()> {
    let ClArgs { options, command }= ClArgs::parse();
    let (template, args) = parse_command(command)?;
    let array_indices = options.index.map(|i|
        args.as_ref().ok_or_else(|| anyhow!("At least one argument list is required with "))?.indices(i)
    ).transpose()?;
    let commands = substitute_args(&template, args);
    let resources = query_slurm_resources(&commands)?;
    group_and_submit_jobs(&options, commands, resources, array_indices)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_tokens() {
        assert_eq!(ArgumentToken::from("{1}"), ArgumentToken::Index(1));
        assert_eq!(
            ArgumentToken::from("{1"),
            ArgumentToken::Arg("{1".to_string())
        );
        assert_eq!(
            ArgumentToken::from(":::"),
            ArgumentToken::Sep {
                files: false,
                zip: false
            }
        );
        assert_eq!(
            ArgumentToken::from(":::+"),
            ArgumentToken::Sep {
                files: false,
                zip: true
            }
        );
        assert_eq!(
            ArgumentToken::from("::::"),
            ArgumentToken::Sep {
                files: true,
                zip: false
            }
        );
        assert_eq!(
            ArgumentToken::from("::::+"),
            ArgumentToken::Sep {
                files: true,
                zip: true
            }
        );
    }

    macro_rules! svec {
        ($($t:tt)*) => {
            [$($t)*].into_iter().map(String::from).collect::<Vec<_>>()
        };
    }

    #[test]
    fn arg_iter_single() {
        let i = ArgumentList::new(&[false], vec![svec!["a", "b", "c"]]);
        assert_eq!(
            &i.groups,
            &vec![ZipGroup {
                start: 0,
                end: 1,
                members: 3
            }]
        );
        let v: Vec<_> = i.collect();
        assert_eq!(v, vec![svec!["a"], svec!["b"], svec!["c"]]);
    }
    #[test]
    fn arg_iter_multigroup() {
        let i = ArgumentList::new(
            &[false, true, true, false],
            vec![
                svec!["a", "b", "c"],
                svec!["x", "y", "z"],
                svec!["1", "2", "3", "4"],
                svec!["i", "j"],
            ],
        );
        assert_eq!(
            &i.groups,
            &vec![
                ZipGroup {
                    start: 0,
                    end: 3,
                    members: 3
                },
                ZipGroup {
                    start: 3,
                    end: 4,
                    members: 2
                },
            ]
        );
        let v: Vec<_> = i.collect();
        assert_eq!(
            v,
            vec![
                svec!["a", "x", "1", "i"],
                svec!["a", "x", "1", "j"],
                svec!["b", "y", "2", "i"],
                svec!["b", "y", "2", "j"],
                svec!["c", "z", "3", "i"],
                svec!["c", "z", "3", "j"],
            ]
        );
    }

    #[test]
    #[should_panic]
    fn arg_iter_empty() {
        ArgumentList::new(&[], vec![]);
    }

    fn expand_args(s: &str) -> Result<Vec<Vec<String>>> {
        let (template, args) = parse_command(s.split_whitespace().map(From::from).collect())?;
        Ok(substitute_args(&template, args))
    }

    #[test]
    fn generate_args() -> Result<()> {
        assert_eq!(expand_args("echo ::: 1")?, vec![svec!["echo", "1"]]);
        assert_eq!(
            expand_args("echo ::: 1 ::: 2 ::: 3")?,
            vec![svec!["echo", "1", "2", "3"]]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 :::+ 3")?,
            vec![svec!["echo", "1", "3"]]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 ::: 3 :::+ 4")?,
            vec![svec!["echo", "1", "3", "4"], svec!["echo", "2", "3", "4"],]
        );
        assert_eq!(
            expand_args("echo ::: 1 2 ::: 3 :::+ 4")?,
            vec![svec!["echo", "1", "3", "4"], svec!["echo", "2", "3", "4"],]
        );

        assert!(expand_args("echo :::+ 1").is_err());
        Ok(())
    }
}
