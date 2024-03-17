use anyhow::Result;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;
use tokio::select;
use rulink::{Database};
use tokio::signal;

#[tokio::main]
async fn main() -> Result<()> {
    let mut rl = DefaultEditor::new()?;
    let db = &mut Database::new();

    loop {
        let read_sql = read_sql(&mut rl);
        match read_sql {
            Ok(sql) => {
                if sql.to_lowercase() == "exit;" {
                    println!("Bye....");
                    break
                }
                if !sql.trim().is_empty() {
                    run_sql(db, sql).await;
                }
            }
            Err(ReadlineError::Interrupted) => {
                if let Some(job_id) = db.get_last_running_job_id() {
                    println!("Interrupted");
                    db.stop_job(job_id.as_str()).await;
                    db.reset_last_running_job_id();
                }
            }
            Err(ReadlineError::Eof) => {
                println!("Exited");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}

/// Read line by line from STDIN until a line ending with `;`.
///
/// Note that `;` in string literals will also be treated as a terminator
/// as long as it is at the end of a line.
fn read_sql(rl: &mut DefaultEditor) -> Result<String, ReadlineError> {
    let mut sql = String::new();
    loop {
        let prompt = if sql.is_empty() { "> " } else { "? " };
        let line = rl.readline(prompt)?;
        if line.is_empty() {
            continue;
        }

        // internal commands starts with "\"
        if line.starts_with('\\') && sql.is_empty() {
            return Ok(line);
        }

        sql.push_str(line.as_str());
        if line.ends_with(';') {
            return Ok(sql);
        } else {
            sql.push('\n');
        }
    }
}


async fn run_sql(db: &mut Database, sql: String) {
    let result = db.run(sql.as_str());
    select! {
         _ = signal::ctrl_c() => {
            if let Some(job_id) = db.get_last_running_job_id() {
                println!("Kill job {}.", job_id);
                db.stop_job(job_id.as_str()).await;
                db.reset_last_running_job_id();
            } else {
                 println!("Interrupted");
            }
        }
        ret = result => {
            match ret {
                Ok(result) => {
                    if let Some(job_id) = result.job_id {
                         db.set_last_running_job_id(job_id);
                    }
                    println!("{}", result.result_chunk);
                }
                Err(err) => println!("{}", err),
            }
        }
    }
}