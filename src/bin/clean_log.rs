use anyhow::Context;
use clap::{Args, Parser};
use indexmap::map::Entry;
use serde::{Deserialize, Serialize};

use slurm_tools::*;
use std::io::{stdout, BufRead, Write};

pub const BATCH_MERGE_FIELDS: &'static [(&'static str, MergeAction)] = &[];

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

#[inline]
fn parse_bytesize(s: &str) -> ParseResult<u64> {
    parse_si_suffix(s.strip_suffix("n").unwrap_or(s))
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

fn main() -> Result<()> {
    reset_sigpipe();
    let args = ClArgs::parse();
    let stdout = stdout();
    let stdout_lock = stdout.lock();

    match Input::default_stdin(args.filename.as_ref())?.buffered() {
        Input::File(input) => parse(input, stdout_lock, &args.options),
        Input::Stdin(input) => parse(input.lock(), stdout_lock, &args.options),
    }
}
