use chrono::Local;
use env_logger::{Builder, Formatter};
use log::{Level, LevelFilter, Record};
use std::env;
use std::io::Write;

/// Configures the logging system given a numeric verbosity.
pub fn from_verbosity(verbosity: usize, blacklist: Option<Vec<&'static str>>) {
    configure(
        match verbosity {
            0 => LevelFilter::Warn,
            1 => LevelFilter::Info,
            2 => LevelFilter::Debug,
            _ => LevelFilter::Trace,
        },
        blacklist,
    )
}

/// Configures the logging system given a `LogLevelFilter`.
pub fn configure(level: LevelFilter, blacklist: Option<Vec<&'static str>>) {
    let format = |buf: &mut Formatter, record: &Record| {
        let timestamp = Local::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let (level, color) = match record.level() {
            Level::Error => ("ERROR", 1),
            Level::Warn => ("WARN ", 3),
            Level::Info => ("INFO ", 7),
            Level::Debug => ("DEBUG", 4),
            Level::Trace => ("TRACE", 5),
        };
        writeln!(
            buf,
            "\x1B[1;3{}m[{} {} {}:{}]\x1B[0m {}",
            color,
            level,
            timestamp,
            record.file().unwrap_or("unknown"),
            record.line().unwrap_or(0),
            record.args()
        )
    };

    let mut builder = Builder::new();

    builder.format(format).filter(None, level);

    for module in blacklist.unwrap_or_default() {
        builder.filter(Some(module), LevelFilter::Warn);
    }

    if env::var("RUST_LOG").is_ok() {
        builder.parse(&env::var("RUST_LOG").unwrap());
    }

    builder.init();
}
