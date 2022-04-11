
use slurm_tools::join_display;

use super::*;

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
    combine_stdout_stderr: bool,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct Job {
    pub command: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_err: Option<PathBuf>,
    pub log_out: PathBuf,
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
struct SbatchCall {
    sbatch_args: Vec<String>,
    batch_script: String,
}

fn parse_sbatch_output(bytes: &[u8]) -> Result<usize> {
    let s = std::str::from_utf8(bytes).context("failed to get sbatch output")?;
    let make_context = || format!("failed to parse sbatch output:\n{}", s);
    let s = s.split(';').next().unwrap();
    let id = s.parse().with_context(make_context)?;
    Ok(id)
}

impl SbatchCall {
    fn exec(&self, o: &Options) -> Result<usize> {
        let bin = sbatch().binary();
        let mut file = create_tmp_file()?;

        let mut cmd = std::process::Command::new(bin);
        cmd.args(&self.sbatch_args);
        if o.dry_run {
            cmd.arg("--test-only");
        }

        cmd.arg(file.path());
            
        let output= cmd.output().context("failed to find sbatch or sbatch-fake")?;  

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

        if !output.status.success() {
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            bail!("sbatch failed with exit code {}", output.status)
        }

        parse_sbatch_output(&output.stdout)
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

    fn add_member(
        &mut self,
        index: usize,
        s: Job,
    ) -> std::result::Result<(), Job> {
        if self.members.contains_key(&index) {
            Err(s)
        } else {
            self.members.insert(index, s);
            Ok(())
        }
    }

    fn job(&self) -> Result<SbatchCall> {
        use std::fmt::Write;
        let mut log_path = pending_logs_dir()?;
        log_path.push("%A_%a.stdout");
        let mut sbatch_args = vec!["--parsable".to_string(), "--output".to_string(), log_path.to_str().unwrap().to_string()];
        let mut batch_script = self.resources.script.clone();

        {
            // This lets use get compiler warnings if we forget to use one of the resources
            let ArraySlurmResources { 
                script: _, 
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
                combine_stdout_stderr 
            } = &self.resources;
            


            if !combine_stdout_stderr {
                sbatch_args.push("--error".to_string());
                log_path.set_extension("stderr");
                sbatch_args.push(log_path.to_str().unwrap().to_string());
            }

            sbatch_args.push("--cpus-per-task".to_string());
            sbatch_args.push(cpus.to_string());

            sbatch_args.push("--nodes".to_string());
            sbatch_args.push(nodes.to_string());

            sbatch_args.push("--time".to_string());
            sbatch_args.push(time.clone());

            sbatch_args.push("--mem".to_string());
            sbatch_args.push(memory.clone());

            if let Some(mail_user) = mail_user {
                sbatch_args.push("--mail-user".to_string());
                sbatch_args.push(mail_user.clone());
            }

            if let Some(mail_type) = mail_type {
                sbatch_args.push("--mail-type".to_string());
                sbatch_args.push(join_display(mail_type, ','));
            }

            if let Some(constraint) = constraint {
                sbatch_args.push("--constraint".to_string());
                sbatch_args.push(constraint.clone());
            }

            if let Some(exclude) = exclude {
                sbatch_args.push("--exclude".to_string());
                sbatch_args.push(exclude.clone());
            }
            if let Some(nodelist) = nodelist {
                sbatch_args.push("--nodelist".to_string());
                sbatch_args.push(nodelist.clone());
            }
        }




        writeln!(&mut batch_script, "\ncase $SLURM_ARRAY_TASK_ID in");

        for (i, m) in &self.members {
            write!(&mut batch_script, "{})\n    ", i);
            for c in &m.command {
                write!(&mut batch_script, " {}", c);
            }
            writeln!(&mut batch_script, "\n;;");
        }

        Ok(SbatchCall { sbatch_args, batch_script })
    }

    // pub fn submit(self, options: &Options, db: &mut Database) -> Result<()> {
    //     use std::fmt::Write;

    //     let array_id = self.job().exec(options)?;

    //     let mut job_id = String::new();
    //     for (i, job) in self.members {
    //         job_id.clear();
    //         write!(&mut job_id, "{}_{}", array_id, i);
            
            
    //     }

    //     Ok(())
    // }

    fn trace_jobs(&self, parent_id: usize) -> impl Iterator<Item = SbatchCall> {
        unimplemented!();
        None.into_iter()
    }
}


fn submit_jobs(options: &Options, jobs: impl IntoIterator<Item=ArrayJobWithIndices>) -> Result<()> {
    let mut db = load_db()?;
    
    for mut array_job in jobs {
        array_job.members.sort_keys();

        let array_id = match array_job.job()?.exec(options) {
            Ok(i) => i,
            Err(e) => {
                write_db(&db)?;
                return Err(e)
            } 
        };
        
        for (i, job) in array_job.members {
            let job_id = format!("{}_{}", array_id, i);
            db.insert(job_id, job);    
        }
    }

    write_db(&db)?;
    Ok(())
}


pub fn group_and_submit_jobs(
    options: &Options,
    commands: Vec<Vec<String>>,
    resources: Vec<SlurmResources>,
    array_indices: Option<Vec<usize>>,
) -> Result<()> {

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
                    g.push(ArrayJobWithIndices::new_with_member(a, i, m));
                }
            } else {
                groups.insert(
                    a.clone(),
                    vec![ArrayJobWithIndices::new_with_member(a, i, member)],
                );
            }
        }

        submit_jobs(
            options,
            groups.into_values().flatten()
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
        
        submit_jobs(
            options, 
            groups.into_values().map(ArrayJob::assign_indices)
        )
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
    let commands = substitute_args(&template, args);
    let resources = query_slurm_resources(&commands)?;
    group_and_submit_jobs(&options, commands, resources, array_indices)?;
    Ok(())
}
