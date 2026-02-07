use std::collections::HashMap;
use std::path::Path;

/// MySQL connection configuration parsed from CLI args or .my.cnf.
#[derive(Debug, Clone)]
pub struct MysqlConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
    pub database: Option<String>,
    pub socket: Option<String>,
}

impl Default for MysqlConfig {
    fn default() -> Self {
        MysqlConfig {
            host: "localhost".to_string(),
            port: 3306,
            user: "root".to_string(),
            password: None,
            database: None,
            socket: None,
        }
    }
}

impl MysqlConfig {
    /// Build a mysql_async connection URL from the config.
    pub fn connection_url(&self) -> String {
        let mut url = format!("mysql://{}@{}:{}", self.user, self.host, self.port);
        if let Some(ref db) = self.database {
            url.push('/');
            url.push_str(db);
        }
        url
    }

    /// Build an opts builder from config.
    pub fn to_opts(&self) -> mysql_async::OptsBuilder {
        let mut builder = mysql_async::OptsBuilder::default()
            .ip_or_hostname(&self.host)
            .tcp_port(self.port)
            .user(Some(&self.user));

        if let Some(ref pw) = self.password {
            builder = builder.pass(Some(pw));
        }
        if let Some(ref db) = self.database {
            builder = builder.db_name(Some(db));
        }
        if let Some(ref sock) = self.socket {
            builder = builder.socket(Some(sock));
        }

        builder
    }
}

/// Parse a MySQL defaults file (`.my.cnf` format) for `[client]` section credentials.
pub fn parse_defaults_file(path: &Path) -> Option<MysqlConfig> {
    let content = std::fs::read_to_string(path).ok()?;
    let mut config = MysqlConfig::default();
    let mut in_client = false;

    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_client = line.eq_ignore_ascii_case("[client]");
            continue;
        }
        if !in_client || line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once('=') {
            let key = key.trim().to_lowercase();
            let value = value.trim().trim_matches('"').trim_matches('\'');
            match key.as_str() {
                "host" => config.host = value.to_string(),
                "port" => {
                    if let Ok(p) = value.parse() {
                        config.port = p;
                    }
                }
                "user" => config.user = value.to_string(),
                "password" => config.password = Some(value.to_string()),
                "socket" => config.socket = Some(value.to_string()),
                "database" => config.database = Some(value.to_string()),
                _ => {}
            }
        }
    }

    Some(config)
}

/// Find the default .my.cnf file.
pub fn find_defaults_file() -> Option<std::path::PathBuf> {
    // Check $HOME/.my.cnf
    if let Some(home) = std::env::var_os("HOME") {
        let path = Path::new(&home).join(".my.cnf");
        if path.exists() {
            return Some(path);
        }
    }
    // Check /etc/my.cnf
    let etc = Path::new("/etc/my.cnf");
    if etc.exists() {
        return Some(etc.to_path_buf());
    }
    None
}

/// Query result row as a HashMap of column name -> string value.
pub type Row = HashMap<String, String>;
