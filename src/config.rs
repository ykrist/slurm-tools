use dirs;
use std::path::PathBuf;
use crate::*;

pub fn config_directory() -> Result<PathBuf> {
    let mut p  =dirs::config_dir()
        .ok_or_else(|| anyhow!("unable to determine user config directory"))?;
    p.push("slurm-tools");
    std::fs::create_dir_all(&p)
        .with_context(|| format!("failed to create slurm-tools config subdirectory: {:?}", &p))?;
        p.push("slurm-tools");
    Ok(p)
}