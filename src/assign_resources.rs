#![allow(unused)]
use std::io::{stdout, BufRead, Write};

use clap::{ArgEnum, Args, Parser};
use serde::{Deserialize, Serialize};
use slurm_tools::prelude::*;

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

#[derive(Copy, Clone, Debug, ArgEnum)]
pub enum PartitionResource {
    Memory,
    Time,
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
    let adjusted = current * (1. + p.margin);

    let search_for = if job.job_state == JobState::Completed { adjusted } else { limit };

    let mut bucket = p
        .buckets
        .iter()
        .position(|&b| search_for <= b)
        .unwrap_or(p.buckets.len());

    if adjusted >= limit {
        bucket += 1;
    }

    Ok(p.buckets.get(bucket).copied().ok_or(ExceedsLimits {
        limit,
        value: current,
    }))
}

fn run(input: impl BufRead, mut output: impl Write, options: &Partition) -> Result<()> {
    let output_field = options
        .output_field
        .as_ref()
        .map(String::as_str)
        .unwrap_or_else(|| options.resource.output_field());

    let mut idx = 0;
    for job in serde_json::Deserializer::from_reader(input).into_iter::<Job>() {
        let mut job = job?;

        match get_new_limit(&options, &job)? {
            Ok(l) => { job.fields.insert(output_field.to_string(), l.into()); },
            Err(e) => {
                if !options.ignore_exceeding {
                    bail!(
                        "job {} at index {} (line {}) exceeds limits: {} * {} > {}",
                        serde_json::to_string(&job.job_id).unwrap(),
                        idx,
                        idx + 1,
                        e.value,
                        1.0 + options.margin,
                        e.limit,
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

#[derive(Args, Clone, Debug)]
pub struct Partition {
    #[clap(arg_enum)]
    resource: PartitionResource,
    buckets: Vec<f64>,
    #[clap(short = 'm', default_value_t = 0.05)]
    margin: f64,
    #[clap(short = 'o')]
    output_field: Option<String>,

    /// Ignore cases where the largest limit would be exceeded.  
    #[clap(short = 'i')]
    ignore_exceeding: bool,
}

#[derive(Clone, Debug, Parser)]
struct ClArgs {
    /// Path to log file, defaults to STDIN.
    #[clap(short = 'f')]
    filename: Option<String>,

    #[clap(flatten)]
    partition: Partition,
}



fn main() -> Result<()> {
    reset_sigpipe();
    let args = ClArgs::parse();
    let out = stdout();
    let output = out.lock();

    match open_file_or_stdin(args.filename.as_ref())? {
        Input::File(input) => run(input, output, &args.partition),
        Input::Stdin(stdin) => run(stdin.lock(), output, &args.partition),
    }
}
