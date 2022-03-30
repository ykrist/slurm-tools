use std::{
    fs::File,
    io::{BufReader, Stdin},
    path::Path,
};

use anyhow::Context;
use serde::{Deserialize, Serialize};

pub mod prelude {
    use std::collections::{HashMap, HashSet};

    pub use indexmap::IndexMap;
    pub use serde_json::Value as JsonValue;

    pub type Map<K, V> = HashMap<K, V>;
    pub type Set<K, V> = HashSet<K, V>;

    pub use anyhow::{anyhow, bail, Result};
    pub use std::result::Result as StdResult;

    #[cfg(unix)]
    pub fn reset_sigpipe() {
        unsafe {
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        }
    }

    #[cfg(not(unix))]
    pub fn reset_sigpipe() {
        // no-op
    }

    pub use super::{fieldname, open_file_or_stdin, Input, JobId, JobState};
}

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

pub enum Input {
    File(BufReader<File>),
    Stdin(Stdin),
}

pub fn open_file_or_stdin<P: AsRef<Path>>(path: Option<P>) -> anyhow::Result<Input> {
    if let Some(path) = path {
        let path = path.as_ref();
        File::open(path)
            .map(BufReader::new)
            .map(Input::File)
            .with_context(|| format!("unable to read {}", path.display()))
    } else {
        Ok(Input::Stdin(std::io::stdin()))
    }
}
