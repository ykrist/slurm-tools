use super::*;
use std::io;

/// The type of a resource value (eg time, memory amount)
pub type ResourceAmount = f64;

/// Type of Slurm resource
#[derive(Copy, Clone, Debug)]
pub enum ResourceKind {
    Memory,
    Time,
}

impl ResourceKind {
    pub const fn limit_field(self) -> &'static str {
        use ResourceKind::*;
        match self {
            Memory => fieldname::REQ_MEM,
            Time => fieldname::TIMELIMIT,
        }
    }

    pub const fn value_field(self) -> &'static str {
        use ResourceKind::*;
        match self {
            Memory => fieldname::MAX_RSS,
            Time => fieldname::ELAPSED,
        }
    }
}

/// A set of resource levels, or buckets
#[derive(Clone, Debug)]
pub struct Levels {
    resource: ResourceKind,
    buckets: Vec<f64>,
    pub margin: Margin,
}

impl Levels {
    pub fn new(resource: ResourceKind, mut buckets: Vec<f64>, margin: Margin) -> Self {
        buckets.sort_unstable_by_key(|&x| OrderF64(x));

        Levels {
            resource,
            buckets,
            margin,
        }
    }
}

/// A margin, used with Levels to determine which bucket a job's resource usage belongs to.
#[derive(Args, Clone, Copy, Debug)]
pub struct Margin {
    /// Relative margin to use (probing amount will be AMOUNT * (1 + MARGIN))
    #[clap(short = 'm', default_value_t = 0.15)]
    margin: f64,
    /// Minimum absolute margin to use (probing amount will be at least AMOUNT + MIN)
    #[clap(long = "mmin", default_value_t = 0.)]
    min: f64,
    /// Maximum absolute margin to use (probing amount will be at most AMOUNT + MAX)
    #[clap(long = "mmax", default_value_t=f64::INFINITY)]
    max: f64,
}

impl Margin {
    pub fn new(rel_margin: f64, min: f64, max: f64) -> Result<Self> {
        let m = Margin {
            margin: rel_margin,
            min,
            max,
        };
        m.validate()?;
        Ok(m)
    }

    pub fn validate(&self) -> Result<()> {
        if self.margin.is_sign_negative() || self.margin.is_infinite() | self.margin.is_nan() {
            bail!("margin must be a non-negative real number: {}", self.margin)
        }

        fn validate_minmax(val: f64, which: &str) -> Result<()> {
            if val.is_nan() || val.is_sign_negative() {
                bail!(
                    "{} margin must be a non-negative real number: {}",
                    which,
                    val
                )
            }
            Ok(())
        }

        validate_minmax(self.min, "min")?;
        validate_minmax(self.max, "max")?;

        if self.min > self.max {
            bail!(
                "min margin cannot be greater than max margin: {} > {}",
                self.min,
                self.max
            )
        }

        Ok(())
    }

    pub fn apply_to(&self, val: impl Into<f64>) -> f64 {
        let val = val.into();
        (val * (1. + self.margin))
            .max(val + self.min)
            .min(val + self.max)
    }
}

/// Library error type for determining new resources
#[derive(Debug, Clone)]
pub enum AssignResourceError {
    /// Value (with margin applied) exceeds all levels
    ExceedsLimits {
        /// Value with margin
        value: f64,
        /// The max level
        limit: f64,
    },
    /// A required field was missing from the job record
    MissingField(&'static str),
    /// A field was unexpectedly non-numeric
    ExpectedNumberField(&'static str),
}

impl Display for AssignResourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AssignResourceError::ExceedsLimits { value, limit } => {
                write!(f, "Maximum limit exceeded: {} > {}", value, limit)
            }
            AssignResourceError::MissingField(n) => write!(f, "Missing field: {}", n),
            AssignResourceError::ExpectedNumberField(n) => {
                write!(f, "Expected numeric field value for `{}`", n)
            }
        }
    }
}

impl std::error::Error for AssignResourceError {}

/// Helper struct to use [`Ord`] with [`f64`].
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

/// Estimate a job's resource requirements for a particular reource kind (see [`Levels`])
pub fn get_new_limit(job: &Job, p: &Levels) -> StdResult<ResourceAmount, AssignResourceError> {
    let j = job.state;
    let current = require_float_field(&job, p.resource.value_field())?;
    let limit = require_float_field(&job, p.resource.limit_field())?;

    let current_with_margin = p.margin.apply_to(current);
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

    p.buckets
        .get(bucket)
        .copied()
        .ok_or(AssignResourceError::ExceedsLimits {
            value: usage,
            limit: *p.buckets.last().unwrap(),
        })
}

fn require_field<'a>(
    job: &'a Job,
    field: &'static str,
) -> StdResult<&'a JsonValue, AssignResourceError> {
    job.fields
        .get(field)
        .ok_or(AssignResourceError::MissingField(field))
}

fn require_float_field(
    job: &Job,
    field: &'static str,
) -> StdResult<ResourceAmount, AssignResourceError> {
    require_field(job, field)?
        .as_f64()
        .ok_or(AssignResourceError::ExpectedNumberField(field))
}

/// Main function for the `sacct-assign-resources` binary
pub fn run(
    input: impl io::Read,
    mut output: impl io::Write,
    p: &Levels,
    output_field: &str,
    ignore_exceeding: bool,
) -> Result<()> {
    use format_num::format_num;
    let mut idx = 0;
    for job in serde_json::Deserializer::from_reader(input).into_iter::<Job>() {
        let mut job = job?;

        match get_new_limit(&job, p) {
            Ok(l) => {
                job.fields.insert(output_field.to_string(), l.into());
            }
            Err(AssignResourceError::ExceedsLimits { value, limit }) => {
                if !ignore_exceeding {
                    bail!(
                        "job at index {} (line {}) exceeds all limits: MAX_LIMIT = {} < {} = AMOUNT_WITH_MARGIN.\n\
                        The limit given to the job was {} = {}.",
                        idx,
                        idx + 1,
                        format_num!(",.0", *p.buckets.last().unwrap()),
                        format_num!(",.0", value),
                        p.resource.limit_field(),
                        format_num!(",.0", limit),
                    )
                }
            }
            Err(e) => return Err(e.into()),
        }
        idx += 1;
        serde_json::to_writer(&mut output, &job)?;
        output.write_all(b"\n")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_bucket(buckets: &[f64], s: JobState, current: f64, limit: f64) -> Option<f64> {
        let mut job = Job {
            id: JobId::dummy(),
            state: s,
            fields: Default::default(),
        };
        job.fields
            .insert(ResourceKind::Time.value_field().to_string(), current.into());
        job.fields
            .insert(ResourceKind::Time.limit_field().to_string(), limit.into());

        let p = Levels {
            resource: ResourceKind::Time, // doesn't matter,
            buckets: buckets.to_vec(),
            margin: Margin::new(0.1, 0., f64::INFINITY).unwrap(),
        };
        get_new_limit(&job, &p).ok()
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
