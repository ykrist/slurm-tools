use std::{
    fmt::Display,
    io::Write,
    path::{Path, PathBuf},
};

use regex::Regex;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use std::collections::{HashMap, HashSet};

pub use indexmap::IndexMap;
pub use serde_json::Value as JsonValue;

pub type Map<K, V> = HashMap<K, V>;
pub type Set<K, V> = HashSet<K, V>;

pub use anyhow::{anyhow, bail, Result};
pub use std::result::Result as StdResult;

pub use posix_cli_utils::*;

pub mod fieldname {
    pub const JOB_ID: &'static str = "JobID";
    pub const STATE: &'static str = "State";
    pub const REQ_MEM: &'static str = "ReqMem";
    pub const TIMELIMIT: &'static str = "Timelimit";
    pub const ELAPSED: &'static str = "Elapsed";
    pub const MAX_RSS: &'static str = "MaxRSS";

    /// Custom fields
    pub const CANCELLED_BY: &'static str = "CancelledBy";

    pub static DROP_FIELDS: phf::Set<&'static str> = phf::phf_set! {
        "TimelimitRaw",
        "ElapsedRaw",
        "ResvCPURAW",
        "CPUTimeRAW",
    };

    pub const BYTESIZE_FIELDS: &'static [&'static str] = &[
        REQ_MEM,
        "AveDiskRead",
        "AveDiskWrite",
        "AveRSS",
        "AveVMSize",
        MAX_RSS,
        "MaxDiskRead",
        "MaxDiskReadTask",
        "MaxDiskWrite",
        "MaxDiskWriteTask",
        "MaxRSSTask",
        "MaxVMSize",
        "MaxVMSizeTask",
        "AvePages",
        "MaxPages",
    ];

    pub const UINT_FIELDS: &'static [&'static str] = &[
        "UID",
        "GID",
        "NCPUS",
        "NNodes",
        "AllocCPUS",
        "AllocNodes",
        "ReqCPUS",
        "NTasks",
    ];

    pub const DURATION_FIELDS: &'static [&'static str] = &[
        TIMELIMIT,
        ELAPSED,
        "Reserved",
        "ResvCPU",
        "CPUTime",
        "MinCPU",
        "Suspended",
        "TotalCPU",
        "UserCPU",
        "SystemCPU",
    ];
}

#[derive(Clone, Copy, Deserialize, Serialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JobState {
    BootFail,
    Cancelled,
    Completed,
    Failed,
    NodeFail,
    OutOfMemory,
    Pending,
    Preempted,
    Running,
    Requeued,
    Resizing,
    Revoked,
    Suspended,
    Timeout,
}

pub fn parse_job_state(s: &str) -> ParseResult<(JobState, Option<u64>)> {
    fn simple_case(s: &str) -> Option<JobState> {
        use JobState::*;
        let s = match s {
            "BOOT_FAIL" => BootFail,
            "CANCELLED" => Cancelled,
            "COMPLETED" => Completed,
            "FAILED" => Failed,
            "NODE_FAIL" => NodeFail,
            "OUT_OF_MEMORY" => OutOfMemory,
            "PENDING" => Pending,
            "PREEMPTED" => Preempted,
            "RUNNING" => Running,
            "REQUEUED" => Requeued,
            "RESIZING" => Resizing,
            "REVOKED" => Revoked,
            "SUSPENDED" => Suspended,
            "TIMEOUT" => Timeout,
            _ => return None,
        };
        Some(s)
    }
    if let Some(s) = simple_case(s) {
        return Ok((s, None));
    }

    if let Some(uid) = s.strip_prefix("CANCELLED by ") {
        if let Ok(uid) = uid.parse() {
            return Ok((JobState::Cancelled, Some(uid)));
        }
    }
    Err(ParseError::JobState(s.to_string()))
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum JobId {
    Array {
        #[serde(rename = "JobID")]
        job_id: u64,
        #[serde(rename = "ArrayTaskID")]
        array_index: u64,
    },

    Normal {
        #[serde(rename = "JobID")]
        job_id: u64,
    },
}

impl JobId {
    #[cfg(test)]
    pub(crate) fn dummy() -> Self {
        JobId::Normal { job_id: 0 }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    IncorrectNumberOfFields,
    UnsupportedJobId(String),
    MissingJobId,
    Bytesize(String),
    Uint(String),
    Duration(String),
    JobState(String),
}

pub type ParseResult<T> = StdResult<T, ParseError>;

impl Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        use ParseError::*;
        match self {
            IncorrectNumberOfFields => write!(
                f,
                "row has incorrect number of fields (try using a different delimiter)"
            ),
            Bytesize(s) => write!(f, "unable to parse bytesize ({})", s),
            Duration(s) => write!(f, "unable to parse duration ({})", s),
            JobState(s) => write!(f, "unable to parse job state ({})", s),
            Uint(s) => write!(f, "unable to parse unsigned int ({})", s),
            UnsupportedJobId(s) => write!(f, "JobIDs of this type ({}) is not yet supported", s),
            MissingJobId => write!(f, "Header must have a JobID field"),
        }
    }
}

impl std::error::Error for ParseError {}

pub fn parse_uint(s: &str) -> ParseResult<u64> {
    s.parse().map_err(|_| ParseError::Uint(s.to_string()))
}

pub fn parse_si_suffix(s: &str) -> ParseResult<u64> {
    let mut power = 0;
    let mut base = s;
    for (suffix, p) in [('K', 10), ('M', 20), ('G', 30), ('T', 40), ('P', 50)] {
        if let Some(b) = s.strip_suffix(|c: char| c.eq_ignore_ascii_case(&suffix)) {
            base = b;
            power = p;
            break;
        }
    }
    let base: f64 = base
        .parse()
        .map_err(|_| ParseError::Bytesize(s.to_string()))?;
    Ok((base * f64::powi(2.0, power)).round() as u64)
}

pub fn parse_duration(s: &str) -> ParseResult<u64> {
    lazy_static::lazy_static! {
        static ref LONG_FORMAT: Regex =
            Regex::new(r"^((?P<days>\d+)\-)?(?P<hours>\d\d?):(?P<mins>\d\d?):(?P<secs>\d\d?)$")
                .unwrap();
        static ref SHORT_FORMAT: Regex =
            Regex::new(r"^(?P<mins>\d\d?):(?P<secs>\d\d?).(?P<millis>\d\d\d)$").unwrap();
    }
    let make_error = || ParseError::Duration(s.to_string());

    let captures = LONG_FORMAT
        .captures(s)
        .or_else(|| SHORT_FORMAT.captures(s))
        .ok_or_else(make_error)?;

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
    let min = parse_field("mins")?;
    let mut secs = parse_field("secs")?;
    let millis = parse_optional_field("millis")?;
    if millis >= 500 {
        secs += 1
    };

    Ok(((days * 24 + hrs) * 60 + min) * 60 + secs)
}

pub fn write_json<T, P>(val: T, path: P) -> Result<()>
where
    T: Serialize,
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let mut f = std::fs::File::create(path).context_write(path)?;
    serde_json::to_writer(&mut f, &val).context("serialization failed")?;
    f.write_all(b"\n")?;
    Ok(())
}

pub fn read_json<T, P>(path: P) -> Result<T>
where
    T: DeserializeOwned,
    P: AsRef<Path>,
{
    let path = path.as_ref();
    let f = std::fs::File::open(path).context_read(path)?;
    serde_json::from_reader(f).context("deserialization failed")
}

pub fn join_display<I, S>(values: I, sep: S) -> String
where
    I: IntoIterator,
    I::Item: Display,
    S: Display + Clone,
{
    use std::fmt::Write;
    let mut values = values.into_iter();
    let mut s = match values.next() {
        Some(v) => v.to_string(),
        None => return String::new(),
    };
    for v in values {
        write!(&mut s, "{}{}", &sep, &v).unwrap();
    }
    s
}

pub fn config_directory() -> Result<PathBuf> {
    let mut p =
        dirs::config_dir().ok_or_else(|| anyhow!("unable to determine user config directory"))?;
    p.push("slurm-tools");
    std::fs::create_dir_all(&p)
        .with_context(|| format!("failed to create slurm-tools config subdirectory: {:?}", &p))?;
    Ok(p)
}

/// A Slurm job record
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Job {
    /// The ID assigned by Slurm to the job
    #[serde(flatten, skip_serializing)]
    pub id: JobId,

    /// The final state of the job
    #[serde(rename = "State")]
    pub state: JobState,

    #[serde(flatten)]
    fields: IndexMap<String, JsonValue>,
}

impl Job {
    pub fn get_field<'a>(&'a self, field: &str) -> Option<&'a JsonValue> {
        self.fields.get(field)
    }
}

pub mod resource_limit;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duration_parsing() {
        const M: u64 = 60;
        const H: u64 = 60 * M;
        const D: u64 = 24 * H;

        assert_eq!(parse_duration("00:00:00"), Ok(0));
        assert_eq!(parse_duration("00:00:01"), Ok(1));
        assert_eq!(parse_duration("1-00:00:01"), Ok(1 * D + 1));
        assert_eq!(
            parse_duration("10-12:45:10"),
            Ok(10 * D + 12 * H + 45 * M + 10)
        );
        assert_eq!(parse_duration("0:68:0"), Ok(68 * M));
        assert_eq!(parse_duration("00:0.123"), Ok(0));
        assert_eq!(parse_duration("00:00.900"), Ok(1));

        assert!(parse_duration("5:0").is_err());
        assert!(parse_duration("00:00:00.001").is_err());
        assert!(parse_duration("00:00.01").is_err());
        assert!(parse_duration("-00:00:00").is_err());
    }
}
