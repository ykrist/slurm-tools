use clap::{Args, Parser};
use regex::Regex;
use slurm_tools::resource_limit::*;
use slurm_tools::*;
use std::io::stdout;

#[derive(Clone, Debug, Parser)]
struct ClArgs {
    /// Path to log file, defaults to STDIN.
    #[clap(short = 'f')]
    filename: Option<String>,

    #[clap(subcommand)]
    resource: ResourceType,
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

#[derive(Args, Clone, Debug)]
struct CommonOptions {
    #[clap(flatten)]
    margin: Margin,
    /// Name of the JSON field in which the output value will be placed.
    // Defaults: [memory = "NewReqMem", time = "NewTimelimit"]
    #[clap(short = 'o', default_value = "output")]
    output_field: String,

    /// Ignore cases where the largest limit would be exceeded.  
    #[clap(short = 'i')]
    ignore_exceeding: bool,
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
    fn validate(self) -> Result<(Levels, CommonOptions)> {
        let (levels, options) = match self {
            ResourceType::Memory { buckets, options } => (
                Levels::new(ResourceKind::Memory, buckets, options.margin),
                options,
            ),
            ResourceType::Time { buckets, options } => (
                Levels::new(ResourceKind::Time, buckets, options.margin),
                options,
            ),
        };
        levels.margin.validate()?;
        Ok((levels, options))
    }
}

fn main() -> Result<()> {
    reset_sigpipe();
    let args = ClArgs::parse();
    let out = stdout();
    let output = out.lock();
    let (levels, opt) = args.resource.validate()?;

    match Input::default_stdin(args.filename.as_ref())? {
        Input::File(input) => run(
            input,
            output,
            &levels,
            &opt.output_field,
            opt.ignore_exceeding,
        ),
        Input::Stdin(stdin) => run(
            stdin.lock(),
            output,
            &levels,
            &opt.output_field,
            opt.ignore_exceeding,
        ),
    }
}
