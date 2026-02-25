//! Prometheus exposition format helpers.
//!
//! Provides lightweight formatting functions for emitting metrics in
//! [Prometheus text exposition format](https://prometheus.io/docs/instrumenting/exposition_formats/)
//! without requiring an external crate.

use std::fmt::Write as FmtWrite;

/// Escape a Prometheus label value.
///
/// Per the specification, label values are enclosed in double quotes and the
/// characters `\`, `"`, and newline must be escaped as `\\`, `\"`, and `\n`
/// respectively.
pub fn escape_label_value(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            _ => out.push(ch),
        }
    }
    out
}

/// Format a single Prometheus gauge line with labels.
///
/// Produces a string like:
/// ```text
/// metric_name{label1="val1",label2="val2"} 0.85
/// ```
///
/// Label values are escaped according to the Prometheus specification.
/// The value is formatted with full precision (no trailing-zero trimming
/// beyond what Rust's `{}` formatter does for f64).
pub fn format_gauge(name: &str, labels: &[(&str, &str)], value: f64) -> String {
    let mut line = String::with_capacity(128);
    line.push_str(name);
    if !labels.is_empty() {
        line.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            let _ = write!(line, "{}=\"{}\"", k, escape_label_value(v));
        }
        line.push('}');
    }
    let _ = write!(line, " {}", value);
    line
}

/// Format a single Prometheus gauge line with an integer value.
pub fn format_gauge_int(name: &str, labels: &[(&str, &str)], value: u64) -> String {
    let mut line = String::with_capacity(128);
    line.push_str(name);
    if !labels.is_empty() {
        line.push('{');
        for (i, (k, v)) in labels.iter().enumerate() {
            if i > 0 {
                line.push(',');
            }
            let _ = write!(line, "{}=\"{}\"", k, escape_label_value(v));
        }
        line.push('}');
    }
    let _ = write!(line, " {}", value);
    line
}

/// Write a `# HELP` annotation line.
pub fn help_line(name: &str, help: &str) -> String {
    format!("# HELP {} {}", name, help)
}

/// Write a `# TYPE` annotation line.
pub fn type_line(name: &str, metric_type: &str) -> String {
    format!("# TYPE {} {}", name, metric_type)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_escape_label_value_plain() {
        assert_eq!(escape_label_value("orders.ibd"), "orders.ibd");
    }

    #[test]
    fn test_escape_label_value_backslash() {
        assert_eq!(escape_label_value("a\\b"), "a\\\\b");
    }

    #[test]
    fn test_escape_label_value_quote() {
        assert_eq!(escape_label_value("a\"b"), "a\\\"b");
    }

    #[test]
    fn test_escape_label_value_newline() {
        assert_eq!(escape_label_value("a\nb"), "a\\nb");
    }

    #[test]
    fn test_escape_label_value_combined() {
        assert_eq!(escape_label_value("a\\\"b\nc"), "a\\\\\\\"b\\nc");
    }

    #[test]
    fn test_format_gauge_no_labels() {
        let line = format_gauge("up", &[], 1.0);
        assert_eq!(line, "up 1");
    }

    #[test]
    fn test_format_gauge_single_label() {
        let line = format_gauge("innodb_fill_factor", &[("file", "orders.ibd")], 0.85);
        assert_eq!(line, "innodb_fill_factor{file=\"orders.ibd\"} 0.85");
    }

    #[test]
    fn test_format_gauge_multiple_labels() {
        let line = format_gauge(
            "innodb_fill_factor",
            &[("file", "orders.ibd"), ("index", "PRIMARY")],
            0.72,
        );
        assert_eq!(
            line,
            "innodb_fill_factor{file=\"orders.ibd\",index=\"PRIMARY\"} 0.72"
        );
    }

    #[test]
    fn test_format_gauge_escapes_labels() {
        let line = format_gauge("metric", &[("path", "a\"b\\c\nd")], 42.0);
        assert_eq!(line, "metric{path=\"a\\\"b\\\\c\\nd\"} 42");
    }

    #[test]
    fn test_format_gauge_int() {
        let line = format_gauge_int("innodb_pages", &[("file", "t.ibd"), ("type", "INDEX")], 150);
        assert_eq!(line, "innodb_pages{file=\"t.ibd\",type=\"INDEX\"} 150");
    }

    #[test]
    fn test_help_line() {
        assert_eq!(
            help_line("innodb_fill_factor", "Average B+Tree fill factor"),
            "# HELP innodb_fill_factor Average B+Tree fill factor"
        );
    }

    #[test]
    fn test_type_line() {
        assert_eq!(
            type_line("innodb_fill_factor", "gauge"),
            "# TYPE innodb_fill_factor gauge"
        );
    }

    #[test]
    fn test_format_gauge_zero() {
        let line = format_gauge("metric", &[], 0.0);
        assert_eq!(line, "metric 0");
    }

    #[test]
    fn test_format_gauge_int_zero() {
        let line = format_gauge_int("metric", &[], 0);
        assert_eq!(line, "metric 0");
    }
}
