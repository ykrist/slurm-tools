#![allow(unused)]
use std::{
    borrow::BorrowMut,
    io::{stdout, BufRead, Read, Write},
};

use clap::{ArgEnum, Args, Parser};
use format_num::format_num;
use regex::Regex;
use serde::{Deserialize, Serialize};
use slurm_tools::*;

type ResourceAmount = f64;

#[derive(Clone, Copy, Debug)]
struct ExceedsLimits {
    limit: ResourceAmount,
    value: ResourceAmount,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct Job {
    #[serde(flatten)]
    job_id: JobId,

    #[serde(rename = "State")]
    job_state: JobState,

    #[serde(flatten)]
    fields: IndexMap<String, JsonValue>,
}

#[derive(Copy, Clone, Debug)]
pub enum PartitionResource {
    Memory,
    Time,
}

#[derive(Clone, Debug)]
pub struct Partition {
    resource: PartitionResource,
    buckets: Vec<f64>,
    options: CommonOptions,
}

impl Partition {
    fn needs_increase(&self, s: JobState, usage: f64, limit: f64) -> bool {
        if usage >= limit {
            return true
        }
        match self.resource {
            PartitionResource::Memory => matches!(s, JobState::OutOfMemory),
            PartitionResource::Time => matches!(s, JobState::Timeout),
        }
    }

    fn estimate_usage(&self, s: JobState, current: f64, limit: f64) -> f64 {
        let u = current * (1.0 + self.options.margin);
        if s == JobState::Completed {
            u
        } else {
            limit.max(u)
        }
    }
}

impl PartitionResource {
    const fn output_field(self) -> &'static str {
        use PartitionResource::*;
        match self {
            Memory => "NewReqMem",
            Time => "NewTimelimit",
        }
    }
    const fn limit_field(self) -> &'static str {
        use PartitionResource::*;
        match self {
            Memory => fieldname::REQ_MEM,
            Time => fieldname::TIMELIMIT,
        }
    }

    const fn value_field(self) -> &'static str {
        use PartitionResource::*;
        match self {
            Memory => fieldname::MAX_RSS,
            Time => fieldname::ELAPSED,
        }
    }
}

fn require_field<'a>(job: &'a Job, field: &str) -> Result<&'a JsonValue> {
    job.fields
        .get(field)
        .ok_or_else(|| anyhow!("missing field: {}", field))
}

fn require_float_field(job: &Job, field: &str) -> Result<ResourceAmount> {
    require_field(job, field)?
        .as_f64()
        .ok_or_else(|| anyhow!("expected numeric field value: {}", field))
}

fn find_bucket(val: f64, buckets: &[f64]) -> Option<usize> {
    buckets.iter().position(|&b| val <= b)
}

fn get_new_limit(p: &Partition, job: &Job) -> Result<StdResult<ResourceAmount, ExceedsLimits>> {
    let limit = require_float_field(job, p.resource.limit_field())?;
    let current = require_float_field(job, p.resource.value_field())?;
    
    let usage = p.estimate_usage(job.job_state, current, limit);
    let mut bucket = p.buckets.iter().position(|&b| usage <= b).unwrap_or(p.buckets.len());

    if p.needs_increase(job.job_state, usage, limit) {
        bucket += 1;
    }

    let bucket = p.buckets
        .get(bucket)
        .copied()
        .ok_or(ExceedsLimits {
            limit,
            value: current,
        });

    Ok(bucket)
}

fn run(input: impl Read, mut output: impl Write, p: &Partition) -> Result<()> {
    let mut idx = 0;
    for job in serde_json::Deserializer::from_reader(input).into_iter::<Job>() {
        let mut job = job?;

        match get_new_limit(&p, &job)? {
            Ok(l) => {
                job.fields.insert(p.options.output_field.clone(), l.into());
            }
            Err(e) => {
                if !p.options.ignore_exceeding {
                    bail!(
                        "job at index {} (line {}) exceeds all limits: MAX_LIMIT = {} < {} * {}. \n The limit given to the job was {} = {}.",
                        idx,
                        idx + 1,
                        format_num!(",.0", *p.buckets.last().unwrap()),
                        format_num!(",.0", e.value),
                        1.0 + p.options.margin,
                        p.resource.limit_field(),
                        format_num!(",.0", e.limit),
                    )
                }
            }
        }
        idx += 1;
        serde_json::to_writer(&mut output, &job)?;
        output.write_all(b"\n")?;
    }
    Ok(())
}



#[derive(Clone, Debug, Parser)]
struct ClArgs {
    /// Path to log file, defaults to STDIN.
    #[clap(short = 'f')]
    filename: Option<String>,

    #[clap(subcommand)]
    resource: ResourceType,
}

#[derive(Args, Clone, Debug)]
struct CommonOptions {
    #[clap(short = 'm', default_value_t = 0.15)]
    margin: f64,

    /// Name of the JSON field in which the output value will be placed.
    // Defaults: [memory = "NewReqMem", time = "NewTimelimit"]
    #[clap(short = 'o', default_value = "output")]
    output_field: String,

    /// Ignore cases where the largest limit would be exceeded.  
    #[clap(short = 'i')]
    ignore_exceeding: bool,
}

fn parse_memory_amount(s: &str) -> ParseResult<f64> {
    parse_si_suffix(s).map(|m| m as f64)
}

fn parse_time_amount(s: &str) -> ParseResult<f64> {
    lazy_static::lazy_static! {
        static ref FORMAT: Regex =
            Regex::new(r"^((((?P<days>\d+)\-)?(?P<hours>\d+):)?(?P<mins>\d+):)?(?P<secs>\d+)$")
                .unwrap();
    }

    if let Ok(v) = s.parse() {
        return Ok(v);
    }

    let make_error = || ParseError::Duration(s.to_string());

    let captures = FORMAT.captures(s).ok_or_else(make_error)?;

    let parse_int = |s: &str| {
        let s = s.trim_start_matches('0');
        if s.is_empty() {
            Ok(0)
        } else {
            s.parse::<u64>().map_err(|_| make_error())
        }
    };

    let parse_field = |n| parse_int(captures.name(n).unwrap().as_str());

    let parse_optional_field = |n| {
        captures
            .name(n)
            .map(|s| parse_int(s.as_str()))
            .unwrap_or(Ok(0))
    };

    let days = parse_optional_field("days")?;
    let hrs = parse_optional_field("hours")?;
    let min = parse_optional_field("mins")?;
    let mut secs = parse_field("secs")?;

    let time = ((days * 24 + hrs) * 60 + min) * 60 + secs;
    Ok(time as f64)
}

#[derive(Clone, Debug, Subcommand)]
enum ResourceType {
    /// Assign new memory limits (in bytes)
    Memory {
        /// Memory limit. SI powers-of-2 suffixes are supported (K, M, G, T, P).
        #[clap(parse(try_from_str=parse_memory_amount), min_values(1), default_values(&["2G","4G", "8G", "16G", "32G", "64G", "128G", "240G"]))]
        buckets: Vec<f64>,

        #[clap(flatten)]
        options: CommonOptions,
    },

    /// Assign new time limits (in seconds)
    Time {
        /// Time limit values.  Acceptable formats are SEC, MIN:SEC, HRS:MIN:SEC and DAY-HRS:MIN:SEC.
        #[clap(parse(try_from_str=parse_time_amount), required(true), min_values(1))]
        buckets: Vec<f64>,

        #[clap(flatten)]
        options: CommonOptions,
    },
}

impl ResourceType {
    fn into_partition(self) -> Partition {
        match self {
            ResourceType::Memory { buckets, options } => Partition {
                buckets,
                options,
                resource: PartitionResource::Memory,
            },
            ResourceType::Time { buckets, options } => Partition {
                buckets,
                options,
                resource: PartitionResource::Time,
            },
        }
    }
}

fn main() -> Result<()> {
    reset_sigpipe();
    let mut args = ClArgs::parse();
    let out = stdout();
    let output = out.lock();

    match Input::default_stdin(args.filename.as_ref())? {
        Input::File(input) => run(input, output, &args.resource.into_partition()),
        Input::Stdin(stdin) => run(stdin.lock(), output, &args.resource.into_partition()),
    }
    // Ok(())
}
