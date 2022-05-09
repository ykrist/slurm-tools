use super::*;

enum PushLogFailure {
    CreateDir(anyhow::Error),
    MoveFiles(anyhow::Error),
}

fn push_log(job_id: &str, job: &Job) -> std::result::Result<(), PushLogFailure> {
    fn create_parent_dir(p: &Path) -> std::result::Result<(), PushLogFailure> {
        let p = p.parent().unwrap();
        std::fs::create_dir_all(p)
            .context_create_dir(p)
            .map_err(PushLogFailure::CreateDir)
    }

    create_parent_dir(&job.log_out)?;
    if let Some(ref p) = job.log_err {
        create_parent_dir(&p)?;
    }

    let move_files = || -> Result<()> {
        let mut path = pending_logs_dir()?;
        path.push(job_id);
        path.set_extension("stdout");
        std::fs::rename(&path, &job.log_out).context_move_file(&path, &job.log_out)?;
        if let Some(ref dest) = job.log_err {
            path.set_extension("stderr");
            std::fs::rename(&path, dest).context_move_file(&path, dest)?;
        }
        Ok(())
    };

    move_files().map_err(PushLogFailure::MoveFiles)
}

pub fn main() -> Result<()> {
    let mut db = load_db()?;

    let queue: Vec<_> = {
        let pending = query_pending_jobs()?;
        db.keys()
            .filter(|&k| !pending.contains(k))
            .cloned()
            .collect()
    };

    for job_id in queue {
        let job = db.remove(&job_id).unwrap();
        match push_log(&job_id, &job) {
            Ok(()) => {}
            Err(PushLogFailure::CreateDir(e)) => {
                db.insert(job_id, job);
                eprintln!("{:?}", e);
            }
            Err(PushLogFailure::MoveFiles(e)) => {
                eprintln!("{:?}", e);
            }
        }
    }

    write_db(&db)?;

    Ok(())
}
