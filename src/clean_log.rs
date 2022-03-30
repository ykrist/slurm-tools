#![allow(unused)]
use anyhow::Context;
use clap::{Args, Parser};
use indexmap::map::Entry;
use lazy_static::lazy_static;
use regex::Regex;
use serde::{Deserialize, Serialize};

use slurm_tools::{prelude::*, JobState};
use std::{
    fmt::Display,
    fs::File,
    io::{stdin, stdout, BufRead, BufReader, Write},
    num::ParseIntError,
    str::FromStr,
};

pub const BATCH_MERGE_FIELDS: &'static [(&'static str, MergeAction)] = &[];

#[derive(Debug, Clone, PartialEq, Eq)]
enum ParseError {
    IncorrectNumberOfFields,
    UnsupportedJobId(String),
    MissingJobId,
    Bytesize(String),
    Uint(String),
    Duration(String),
    JobState(String),
}

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

type ParseResult<T> = StdResult<T, ParseError>;

#[derive(Copy, Clone, Debug)]
struct Metadata {
    is_batch_step: bool,
}

#[derive(Clone, Copy, Debug)]
pub enum MergeAction {
    Overwrite,
    Sum,
    Max,
    Min,
    AssertSame,
}

impl MergeAction {
    fn numeric_update<F1, F2>(a: &mut JsonValue, b: JsonValue, intop: F1, floatop: F2)
    where
        F1: FnOnce(i64, i64) -> i64,
        F2: FnOnce(f64, f64) -> f64,
    {
        const ERR: &'static str = "expected numeric field";

        *a = match (a.as_i64(), b.as_i64()) {
            (Some(x), Some(y)) => intop(x, y).into(),
            _ => floatop(a.as_f64().expect(ERR), b.as_f64().expect(ERR)).into(),
        };
    }

    fn merge_value(self, original: &mut JsonValue, new: JsonValue) {
        use MergeAction::*;
        match self {
            Overwrite => *original = new,
            AssertSame => assert_eq!(original, &new),
            Sum => {
                MergeAction::numeric_update(original, new, std::ops::Add::add, std::ops::Add::add)
            }
            Max => MergeAction::numeric_update(original, new, i64::max, f64::max),
            Min => MergeAction::numeric_update(original, new, i64::min, f64::min),
        }
    }
}

#[derive(Default, Clone, Debug)]
struct Jobs<'a> {
    jobs: IndexMap<JobId, (Metadata, JobRow<'a>)>,
}

impl<'a> Jobs<'a> {
    fn merge_batch_job(into: &mut JobRow<'a>, mut src: JobRow<'a>) {
        for (&field, val) in &mut src.fields {
            into.fields
                .entry(field)
                .or_insert(std::mem::replace(val, JsonValue::Null));
        }

        for (field, merge) in BATCH_MERGE_FIELDS {
            if let Some(a) = into.fields.get_mut(field) {
                match src.fields.remove(field) {
                    None | Some(JsonValue::Null) => continue,
                    Some(b) => merge.merge_value(a, b),
                }
            }
        }
    }

    fn merge(&mut self, mut meta: Metadata, mut row: JobRow<'a>) {
        match self.jobs.entry(row.job_id) {
            Entry::Occupied(mut e) => {
                let (m, r) = e.get_mut();
                if m.is_batch_step {
                    std::mem::swap(m, &mut meta);
                    std::mem::swap(r, &mut row);
                }
                debug_assert!(!m.is_batch_step);
                Jobs::merge_batch_job(r, row);
            }
            Entry::Vacant(e) => {
                e.insert((meta, row));
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct JobRow<'a> {
    #[serde(flatten)]
    job_id: JobId,
    #[serde(flatten, borrow)]
    fields: IndexMap<&'a str, JsonValue>,
}

fn parse_job_id(s: &str) -> ParseResult<JobId> {
    if let Ok(job_id) = s.parse::<u64>() {
        return Ok(JobId::Normal { job_id });
    }

    if s.contains('_') {
        let mut s = s.split('_');
        let job_id = s.next().map(|s| s.parse::<u64>());
        let index = s.next().map(|s| s.parse::<u64>());

        match (job_id, index, s.next()) {
            (Some(Ok(job_id)), Some(Ok(array_index)), None) => {
                return Ok(JobId::Array {
                    job_id,
                    array_index,
                })
            }
            _ => {}
        }
    }

    Err(ParseError::UnsupportedJobId(s.to_string()))
}

fn parse_job_state(s: &str) -> ParseResult<(JobState, Option<u64>)> {
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

fn parse_header<'a>(options: &ParseOptions, line: &'a str) -> ParseResult<Vec<&'a str>> {
    let mut has_job_id_field = false;

    let header = line
        .split(&options.delimiter)
        .map(|s| s.trim())
        .inspect(|&f| {
            has_job_id_field |= f == fieldname::JOB_ID;
        })
        .collect();

    if has_job_id_field {
        Ok(header)
    } else {
        Err(ParseError::MissingJobId)
    }
}

fn parse_uint(s: &str) -> ParseResult<u64> {
    s.parse().map_err(|_| ParseError::Uint(s.to_string()))
}

fn parse_bytesize(s: &str) -> ParseResult<u64> {
    let s = s.strip_suffix("n").unwrap_or(s);

    let mut power = 0;
    let mut base = s;
    for (suffix, p) in [('K', 10), ('M', 20), ('G', 30), ('T', 40), ('P', 50)] {
        if let Some(b) = s.strip_suffix(suffix) {
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

fn parse_fields<'a, F, T, I>(r: &mut JobRow, fields: I, mut parser: F) -> ParseResult<()>
where
    I: IntoIterator<Item = &'a &'a str>,
    F: FnMut(&str) -> ParseResult<T>,
    T: Into<JsonValue>,
{
    for &field in fields {
        if let Some(value) = r.fields.get_mut(field) {
            let v = parser(value.as_str().expect("field is parsed twice"))?;
            *value = v.into();
        }
    }
    Ok(())
}

fn parse_body<'a>(
    options: &ParseOptions,
    header: &'a [&'a str],
    line: &str,
) -> StdResult<(Metadata, JobRow<'a>), ParseError> {
    let mut fields =
        IndexMap::<&str, JsonValue>::with_capacity_and_hasher(header.len(), Default::default());

    let mut meta = Metadata {
        is_batch_step: false,
    };

    let mut count = 0;
    let mut values = line.split(&options.delimiter).map(|s| s.trim());

    for &field in header {
        let val = values.next().ok_or(ParseError::IncorrectNumberOfFields)?;
        if !(val.is_empty() || fieldname::DROP_FIELDS.contains(field)) {
            fields.insert(field, val.into());
        }
    }
    if values.next().is_some() {
        return Err(ParseError::IncorrectNumberOfFields);
    }

    let job_id = {
        let j = match fields.remove(fieldname::JOB_ID).unwrap() {
            JsonValue::String(s) => s,
            _ => unreachable!(),
        };
        let job_id = match j.strip_suffix(".batch") {
            None => j.as_str(),
            Some(s) => {
                meta.is_batch_step = true;
                s
            }
        };
        parse_job_id(job_id)?
    };

    if let Some(val) = fields.get_mut(fieldname::STATE) {
        let (state, cancelled_by) = parse_job_state(val.as_str().unwrap())?;
        *val = serde_json::to_value(state).unwrap();
        if let Some(uid) = cancelled_by {
            fields.insert(fieldname::CANCELLED_BY, uid.into());
        }
    }

    let mut row = JobRow { job_id, fields };
    parse_fields(&mut row, fieldname::BYTESIZE_FIELDS, parse_bytesize)?;
    parse_fields(&mut row, fieldname::DURATION_FIELDS, parse_duration)?;
    parse_fields(&mut row, fieldname::UINT_FIELDS, parse_uint)?;
    Ok((meta, row))
}

#[derive(Parser, Clone)]
struct ClArgs {
    /// Path to log file, defaults to STDIN.
    filename: Option<String>,
    #[clap(flatten)]
    options: ParseOptions,
}

#[derive(Debug, Clone, Args)]
struct ParseOptions {
    #[clap(short = 'd', long = "delimiter", default_value = "|")]
    delimiter: String,
}

fn parse(mut input: impl BufRead, mut output: impl Write, options: &ParseOptions) -> Result<()> {
    let mut header_buf = String::new();
    input
        .read_line(&mut header_buf)
        .context("failed to read header")?;
    let header = parse_header(options, &header_buf)?;

    let mut line_buf = String::new();
    let mut line_no = 2;
    let mut jobs = Jobs::default();

    while input.read_line(&mut line_buf)? != 0 {
        match parse_body(options, &header, &line_buf) {
            Ok((meta, row)) => {
                jobs.merge(meta, row);
            }
            Err(e) => eprintln!("error (ignoring input on line {}): {}", line_no, e),
        }
        line_no += 1;
        line_buf.clear();
    }
    for (_, mut row) in jobs.jobs.into_values() {
        row.fields.sort_unstable_keys();
        serde_json::to_writer(&mut output, &row)?;
        output.write_all(b"\n")?;
    }
    Ok(())
}

fn parse_duration(s: &str) -> ParseResult<u64> {
    lazy_static! {
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

fn main() -> Result<()> {
    reset_sigpipe();
    let args = ClArgs::parse();
    let stdout = stdout();
    let stdout_lock = stdout.lock();

    match open_file_or_stdin(args.filename.as_ref())? {
        Input::File(input) => parse(input, stdout_lock, &args.options),
        Input::Stdin(input) => parse(input.lock(), stdout_lock, &args.options),
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_duration;

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
