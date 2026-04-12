//! Naive splitting of a `.sql` seed file into statements for MySQL-family drivers that execute
//! one statement at a time.
//!
//! Splits on `;` after trimming. Does not handle `;` inside string literals — keep seed files to
//! simple DML/DDL without embedded semicolons in strings.

/// Split a SQL script into non-empty statements (trimmed, `;`-delimited).
pub(crate) fn statements_from_sql_script(script: &str) -> impl Iterator<Item = &str> {
    script
        .split(';')
        .map(str::trim)
        .filter(|statement| !statement.is_empty() && !statement.starts_with("--"))
}
