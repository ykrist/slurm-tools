use posix_cli_utils::*;
use slurm_tools::{parse_job_state, IndexMap, JobState};
use std::{
    collections::HashMap,
    io::{BufRead, BufReader},
    process::*,
};

type PartitionUsage = IndexMap<String, UserStatistics>;

#[derive(Clone, Copy, Default, Debug)]
struct UserStatistics {
    pending_jobs: u64,
    running_jobs: u64,
    running_cpus: u64,
}

impl UserStatistics {
    fn update(&mut self, l: &SqueueLine) {
        match l.job_state {
            JobState::Pending => {
                self.pending_jobs += 1;
            }
            JobState::Running => {
                self.running_jobs += 1;
                self.running_cpus += l.cpus as u64;
            }
            _ => {}
        }
    }
}

struct SqueueLine<'a> {
    partition: &'a str,
    user: &'a str,
    cpus: u32,
    job_state: JobState,
}

impl<'a> SqueueLine<'a> {
    fn parse(line: &'a str) -> Result<Self> {
        let mut s = line.split_ascii_whitespace();
        let partition = s.next().ok_or_else(|| anyhow!("missing partition field"))?;
        let user = s.next().ok_or_else(|| anyhow!("missing user field"))?;
        let cpus: u32 = s
            .next()
            .ok_or_else(|| anyhow!("missing cpus field"))?
            .parse()?;
        let job_state = s.next().ok_or_else(|| anyhow!("missing cpus field"))?;
        let (job_state, _) = parse_job_state(job_state)?;
        if !s.next().is_none() {
            bail!("unexpected field in line: {}", line)
        }
        Ok(SqueueLine {
            partition,
            user,
            cpus,
            job_state,
        })
    }
}

#[allow(unused)]
#[derive(Debug, Clone)]
struct PartitionInfo {
    cpus_allocated: u64,
    cpus_idle: u64,
    cpus_other: u64,
    cpus_total: u64,
}

fn get_partition_info() -> Result<HashMap<String, PartitionInfo>> {
    let mut si = Command::new("sinfo")
        .args(&["-s", "-o", "%R/%C", "--noheader"])
        .stdout(Stdio::piped())
        .spawn()
        .context("failed to run sinfo")?;
    let mut info = HashMap::default();
    for line in BufReader::new(si.stdout.take().unwrap()).lines() {
        let mut line = line?;

        let mut l = line.split('/');
        if !l.next().is_some() {
            bail!("missing partition: {}", &line)
        }

        let mut parse_cpu_field = || {
            if let Some(i) = l.next() {
                i.parse::<u64>()
                    .with_context(|| format!("invalid CPU field: {}", &line))
            } else {
                bail!("missing CPU field: {}", &line)
            }
        };

        let p = PartitionInfo {
            cpus_allocated: parse_cpu_field()?,
            cpus_idle: parse_cpu_field()?,
            cpus_other: parse_cpu_field()?,
            cpus_total: parse_cpu_field()?,
        };

        line.truncate(line.find('/').unwrap());
        let name = line;

        info.insert(name, p);
    }
    Ok(info)
}

fn get_user_name_map() -> HashMap<String, String> {
    pwd::Passwd::iter()
        .map(|p| (p.name, p.gecos.unwrap_or_default()))
        .collect()
}

fn get_usage_stats(partitions: Option<&str>) -> Result<HashMap<String, PartitionUsage>> {
    let mut sq = Command::new("squeue");
    sq.args(&["-r", "-o", "%P %u %C %T", "--noheader"]);
    if let Some(partitions) = partitions {
        sq.args(&["-p", partitions]);
    }
    let mut sq = sq.stdout(Stdio::piped()).spawn().context("failed to run squeue")?;

    let mut stats = HashMap::new();
    for line in BufReader::new(sq.stdout.take().unwrap()).lines() {
        let line = line?;
        let l = SqueueLine::parse(&line)?;

        if !stats.contains_key(l.partition) {
            stats.insert(l.partition.to_string(), IndexMap::default());
        }

        let users: &mut IndexMap<String, UserStatistics> = stats.get_mut(l.partition).unwrap();

        if let Some(u) = users.get_mut(l.user) {
            u.update(&l);
        } else {
            let mut u = UserStatistics::default();
            u.update(&l);
            users.insert(l.user.to_string(), u);
        }
    }
    let sq = sq.wait()?;
    if !sq.success() {
        bail!("squeue failed with exit code {}", sq)
    }
    for users in stats.values_mut() {
        users.sort_unstable_by(|_, u1, _, u2| u2.running_cpus.cmp(&u1.running_cpus))
    }

    Ok(stats)
}

#[derive(Parser, Clone, Debug)]
struct Options {
    /// Which partition to print statistics for.  Default is all partitions.
    #[clap(short, long)]
    partition: Option<String>,

    /// Limit the number of users shown.  By default there is not limit.
    #[clap(short = 'n')]
    num_users: Option<usize>,
}

fn pretty_print_usage<'a, U>(names: &HashMap<String, String>, usage: U)
where
    U: IntoIterator<Item = (&'a String, &'a UserStatistics)> + 'a,
{
    use comfy_table::*;

    let mut t = Table::new();
    t.load_preset(presets::UTF8_BORDERS_ONLY);

    t.set_header::<[&str; 5]>([
        "User",
        "Name",
        "Jobs Running",
        "Jobs Pending",
        "Occupied CPUS",
    ]);
    for (user, u) in usage {
        let row: [Cell; 5] = [
            user.into(),
            names[user].as_str().into(),
            u.running_jobs.into(),
            u.pending_jobs.into(),
            u.running_cpus.into(),
        ];
        t.add_row(row);
    }
    println!("{}", &t)
}

fn main() -> Result<()> {
    let o = Options::parse();
    let names = get_user_name_map();
    let stats = get_usage_stats(o.partition.as_deref())?;
    let partition_info = get_partition_info()?;

    let mut first = true;
    for (pname, usage) in stats {
        if first {
            first = false;
        } else {
            println!("");
        }
        print!("Partition: {}", pname);
        if let Some(info) = partition_info.get(&pname) {
            println!(" ({} CPUs)", info.cpus_total)
        } else {
            println!("")
        }

        if let Some(n) = o.num_users {
            pretty_print_usage(&names, usage.iter().take(n));
        } else {
            pretty_print_usage(&names, usage.iter());
        }
    }

    Ok(())
}
