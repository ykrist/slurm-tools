use std::io::{stdout, Read, Write};

use clap::{Args, Parser};
use format_num::format_num;
use regex::Regex;
use serde::{Deserialize, Serialize};
use slurm_tools::*;

type ResourceAmount = f64;

#[derive(Clone, Copy, Debug, PartialOrd)]
pub struct OrderF64(pub f64);

impl PartialEq for OrderF64 {
    fn eq(&self, other: &Self) -> bool {
        self.0.to_bits() == other.0.to_bits()
    }
}

impl Eq for OrderF64 {}

impl Ord for OrderF64 {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).expect("nan or inf")
    }
}

#[derive(Clone, Copy, Debug)]
struct ExceedsLimits {
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

impl PartitionResource {
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

fn get_new_limit(
    p: &Partition,
    j: JobState,
    current: f64,
    limit: f64,
) -> Result<StdResult<ResourceAmount, ExceedsLimits>> {
    let current_with_margin = (1. + p.options.margin) * current;
    let get_bucket = |val| {
        p.buckets
            .iter()
            .position(|&b| val <= b)
            .unwrap_or(p.buckets.len())
    };

    let (bucket, usage) = match (j, current_with_margin < limit) {
        (JobState::Completed, true) => {
            // Case 1: Job completed, simple lookup
            (get_bucket(current_with_margin), current_with_margin)
        }
        (_, false) => {
            // Case 2: Resource we're currently looking at wasn't enough
            // Job may or may not have failed but we should respect the margin.
            let mut k = get_bucket(current_with_margin);
            let (k_limit, _) = p
                .buckets
                .iter()
                .enumerate()
                .min_by_key(|(_, b)| OrderF64((limit - *b).abs()))
                .unwrap();
            debug_assert!(k_limit <= k);
            if k == k_limit && k < p.buckets.len() - 1 {
                k += 1;
            }
            (k, current_with_margin)
        }
        (_, true) => {
            // Case 3: Job failed for some other reason.  Try to maintain the current limit,
            // but don't decrease it (much).
            let k = p
                .buckets
                .iter()
                .position(|&b| limit * 0.95 <= b && current_with_margin <= b)
                .unwrap_or(p.buckets.len());
            (k, limit)
        }
    };

    let bucket = p
        .buckets
        .get(bucket)
        .copied()
        .ok_or(ExceedsLimits { value: usage });

    Ok(bucket)
}

fn run(input: impl Read, mut output: impl Write, p: &Partition) -> Result<()> {
    let mut idx = 0;
    for job in serde_json::Deserializer::from_reader(input).into_iter::<Job>() {
        let mut job = job?;

        let current = require_float_field(&job, p.resource.value_field())?;
        let limit = require_float_field(&job, p.resource.limit_field())?;

        match get_new_limit(&p, job.job_state, current, limit)? {
            Ok(l) => {
                job.fields.insert(p.options.output_field.clone(), l.into());
            }
            Err(e) => {
                if !p.options.ignore_exceeding {
                    bail!(
                        "job at index {} (line {}) exceeds all limits: MAX_LIMIT = {} < {} = AMOUNT_WITH_MARGIN.\n\
                        The limit given to the job was {} = {}.",
                        idx,
                        idx + 1,
                        format_num!(",.0", *p.buckets.last().unwrap()),
                        format_num!(",.0", e.value),
                        p.resource.limit_field(),
                        format_num!(",.0", limit),
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

fn parse_memory_amount(s: &str) -> Result<f64> {
    let m = parse_si_suffix(s)?;
    if m == 0 {
        bail!("must greater than 0");
    }
    Ok(m as f64)
}

fn parse_time_amount(s: &str) -> Result<f64> {
    lazy_static::lazy_static! {
        static ref FORMAT: Regex =
            Regex::new(r"^((((?P<days>\d+)\-)?(?P<hours>\d+):)?(?P<mins>\d+):)?(?P<secs>\d+)$")
                .unwrap();
    }

    if let Ok(v) = s.parse::<u64>() {
        return Ok(v as f64);
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
    let secs = parse_field("secs")?;

    let time = ((days * 24 + hrs) * 60 + min) * 60 + secs;
    if time == 0 {
        bail!("must be greater than 0");
    }
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
        #[inline]
        fn sort_buckets(mut buckets: Vec<f64>) -> Vec<f64> {
            buckets.sort_unstable_by_key(|&x| OrderF64(x));
            buckets
        }

        match self {
            ResourceType::Memory { buckets, options } => Partition {
                buckets: sort_buckets(buckets),
                options,
                resource: PartitionResource::Memory,
            },
            ResourceType::Time { buckets, options } => Partition {
                buckets: sort_buckets(buckets),
                options,
                resource: PartitionResource::Time,
            },
        }
    }
}

fn main() -> Result<()> {
    reset_sigpipe();
    let args = ClArgs::parse();
    let out = stdout();
    let output = out.lock();

    match Input::default_stdin(args.filename.as_ref())? {
        Input::File(input) => run(input, output, &args.resource.into_partition()),
        Input::Stdin(stdin) => run(stdin.lock(), output, &args.resource.into_partition()),
    }
    // Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_bucket(buckets: &[f64], s: JobState, current: f64, limit: f64) -> Option<f64> {
        let p = Partition {
            resource: PartitionResource::Time, // doesn't matter,
            buckets: buckets.to_vec(),
            options: CommonOptions {
                margin: 0.1,
                output_field: "test".into(),
                ignore_exceeding: false,
            },
        };
        get_new_limit(&p, s, current, limit).unwrap().ok()
    }

    #[test]
    fn new_limit_logic() {
        use JobState::*;
        let buckets = &[1., 5., 10., 20.];
        assert_eq!(get_bucket(buckets, Completed, 4.0, 5.0), Some(5.0));
        assert_eq!(get_bucket(buckets, Completed, 4.0, 10.0), Some(5.0));
        assert_eq!(get_bucket(buckets, Failed, 4.0, 10.0), Some(10.0));
        assert_eq!(get_bucket(buckets, Failed, 4.0, 10.01), Some(10.0));
        assert_eq!(get_bucket(buckets, Failed, 4.0, 11.0), Some(20.0));
        assert_eq!(get_bucket(buckets, Failed, 4.5, 4.0), Some(10.0));
        assert_eq!(get_bucket(buckets, Failed, 5.5, 5.0), Some(10.0));
        assert_eq!(get_bucket(buckets, Failed, 6.5, 5.5), Some(10.0));
        assert_eq!(get_bucket(buckets, Completed, 9.95, 10.0), Some(20.0));
        assert_eq!(get_bucket(buckets, Failed, 9.5, 9.0), Some(20.0));
        assert_eq!(get_bucket(buckets, Completed, 9.5, 9.0), Some(20.0));
        assert_eq!(get_bucket(buckets, Completed, 10.5, 11.0), Some(20.0));
        assert_eq!(get_bucket(buckets, Failed, 9.0, 10.1), Some(10.0));
        assert_eq!(get_bucket(buckets, Completed, 9.0, 10.1), Some(10.0));
    }
}
