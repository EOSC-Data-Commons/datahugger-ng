use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

/// Build a `GlobSet` from a list of pattern strings.
///
/// Each pattern is normalised so that:
/// - Backslashes are replaced with forward slashes (Windows compat).
/// - Bare names without a path separator are prefixed with `**/`
///   so they match at any depth (like rsync / rclone).
/// - Bare `**` and patterns already starting with `**/` are left as-is.
///
/// Matching is always case-sensitive so that remote API paths behave
/// consistently across platforms.
fn build_globset<T: AsRef<str>>(patterns: &[T]) -> Result<GlobSet, globset::Error> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        let pattern = pattern.as_ref().replace('\\', "/");

        let normalized = if pattern == "**" || pattern.starts_with("**/") || pattern.contains('/') {
            pattern
        } else {
            format!("**/{pattern}")
        };

        let glob = GlobBuilder::new(&normalized)
            .case_insensitive(false)
            .build()?;
        builder.add(glob);
    }
    builder.build()
}

/// A compiled pair of include/exclude glob sets for filtering file entries
/// by relative path.
///
/// - An empty `includes` set matches everything.
/// - A file passes the filter when it matches the include rules (or there
///   are none) **and** does not match any exclude rule.
#[derive(Clone, Debug)]
pub struct FileFilter {
    includes: GlobSet,
    excludes: GlobSet,
}

impl Default for FileFilter {
    /// Creates a filter that accepts all files (no include or exclude rules).
    fn default() -> Self {
        FileFilter {
            includes: GlobSetBuilder::new().build().unwrap(),
            excludes: GlobSetBuilder::new().build().unwrap(),
        }
    }
}

impl FileFilter {
    /// Creates a filter from include and exclude pattern lists.
    ///
    /// # Errors
    /// Returns an error if any pattern is invalid.
    pub fn new<I, E>(includes: &[I], excludes: &[E]) -> Result<Self, globset::Error>
    where
        I: AsRef<str>,
        E: AsRef<str>,
    {
        Ok(FileFilter {
            includes: build_globset(includes)?,
            excludes: build_globset(excludes)?,
        })
    }

    /// Returns `true` if the given relative path passes the filter.
    ///
    /// A path passes when it is included (or no include rules are set)
    /// **and** it is not excluded.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        let included = self.includes.is_empty() || self.includes.is_match(path);
        included && !self.excludes.is_match(path)
    }

    /// Returns `true` if this filter accepts all files
    /// (no include and no exclude rules).
    #[must_use]
    pub fn is_accept_all(&self) -> bool {
        self.includes.is_empty() && self.excludes.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── include-only ────────────────────────────────────────────

    #[test]
    fn default_matches_everything() {
        let f = FileFilter::default();
        assert!(f.matches("anything.csv"));
        assert!(f.matches("dir/file.txt"));
        assert!(f.is_accept_all());
    }

    #[test]
    fn single_extension_pattern() {
        let f = FileFilter::new(&["*.csv"], &[] as &[&str]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("dir/data.csv"));
        assert!(!f.matches("data.tsv"));
    }

    #[test]
    fn multiple_patterns_or_semantics() {
        let f = FileFilter::new(&["*.csv", "*.tsv"], &[] as &[&str]).unwrap();
        assert!(f.matches("a.csv"));
        assert!(f.matches("b.tsv"));
        assert!(!f.matches("c.json"));
    }

    #[test]
    fn exact_filename() {
        let f = FileFilter::new(&["specific_file.h5"], &[] as &[&str]).unwrap();
        assert!(f.matches("specific_file.h5"));
        assert!(f.matches("dir/specific_file.h5"));
        assert!(!f.matches("other_file.h5"));
    }

    #[test]
    fn subdir_glob() {
        let f = FileFilter::new(&["subdir/*"], &[] as &[&str]).unwrap();
        assert!(f.matches("subdir/file.csv"));
        assert!(!f.matches("other/file.csv"));
    }

    #[test]
    fn empty_patterns_is_accept_all() {
        let f = FileFilter::new(&[] as &[&str], &[] as &[&str]).unwrap();
        assert!(f.is_accept_all());
        assert!(f.matches("anything"));
    }

    #[test]
    fn invalid_pattern_returns_error() {
        let result = FileFilter::new(&["[invalid"], &[] as &[&str]);
        assert!(result.is_err());
    }

    #[test]
    fn pattern_with_explicit_double_star_prefix() {
        let f = FileFilter::new(&["**/data.csv"], &[] as &[&str]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("sub/data.csv"));
        assert!(f.matches("a/b/c/data.csv"));
        assert!(!f.matches("data.tsv"));
    }

    #[test]
    fn pattern_with_path_separator_skips_normalization() {
        let f = FileFilter::new(&["data/results/*.csv"], &[] as &[&str]).unwrap();
        assert!(f.matches("data/results/output.csv"));
        assert!(!f.matches("other/results/output.csv"));
        assert!(!f.matches("data/results/output.tsv"));
    }

    #[test]
    fn deeply_nested_file_matches_extension_glob() {
        let f = FileFilter::new(&["*.h5"], &[] as &[&str]).unwrap();
        assert!(f.matches("a/b/c/d/e/model.h5"));
        assert!(!f.matches("a/b/c/d/e/model.csv"));
    }

    #[test]
    fn filter_is_not_accept_all_when_patterns_set() {
        let f = FileFilter::new(&["*.csv"], &[] as &[&str]).unwrap();
        assert!(!f.is_accept_all());
    }

    #[test]
    fn clone_produces_independent_filter() {
        let f = FileFilter::new(&["*.csv"], &[] as &[&str]).unwrap();
        let f2 = f.clone();
        assert!(f.matches("data.csv"));
        assert!(f2.matches("data.csv"));
        assert!(!f2.matches("data.tsv"));
    }

    #[test]
    fn case_sensitive_matching() {
        let f = FileFilter::new(&["*.CSV"], &[] as &[&str]).unwrap();
        assert!(f.matches("data.CSV"));
        assert!(!f.matches("data.csv"));
    }

    #[test]
    fn pattern_matching_full_path_with_leading_slash() {
        let f = FileFilter::new(&["*.csv"], &[] as &[&str]).unwrap();
        let _ = f.matches("/root/data.csv");
    }

    #[test]
    fn mixed_pattern_types() {
        let f = FileFilter::new(&["*.csv", "README.md", "docs/*.pdf"], &[] as &[&str]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("sub/README.md"));
        assert!(f.matches("docs/paper.pdf"));
        assert!(!f.matches("data.json"));
        assert!(!f.matches("src/main.rs"));
    }

    #[test]
    fn windows_backslash_normalized_to_forward_slash() {
        let f = FileFilter::new(&["subdir\\*.csv"], &[] as &[&str]).unwrap();
        assert!(f.matches("subdir/data.csv"));
        assert!(!f.matches("other/data.csv"));
    }

    #[test]
    fn bare_double_star_matches_everything() {
        let f = FileFilter::new(&["**"], &[] as &[&str]).unwrap();
        assert!(f.matches("README.md"));
        assert!(f.matches("sub/data.csv"));
        assert!(f.matches("a/b/c/deep.txt"));
    }

    // ── exclude-only ────────────────────────────────────────────

    #[test]
    fn exclude_rejects_matching_files() {
        let f = FileFilter::new(&[] as &[&str], &["*.log"]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(!f.matches("debug.log"));
        assert!(!f.matches("sub/error.log"));
    }

    #[test]
    fn exclude_subdir() {
        let f = FileFilter::new(&[] as &[&str], &["raw/*"]).unwrap();
        assert!(f.matches("clean/data.csv"));
        assert!(!f.matches("raw/data.csv"));
    }

    // ── include + exclude combined ──────────────────────────────

    #[test]
    fn include_and_exclude_combined() {
        let f = FileFilter::new(&["*.csv"], &["test_*"]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(!f.matches("test_data.csv"));
        assert!(!f.matches("data.json"));
    }

    #[test]
    fn exclude_overrides_include() {
        let f = FileFilter::new(&["*.csv", "*.tsv"], &["*.tsv"]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(!f.matches("data.tsv"));
    }

    #[test]
    fn is_not_accept_all_with_excludes_only() {
        let f = FileFilter::new(&[] as &[&str], &["*.log"]).unwrap();
        assert!(!f.is_accept_all());
    }

    // ── accepts String and &str ─────────────────────────────────

    #[test]
    fn accepts_owned_strings() {
        let inc = vec!["*.csv".to_string()];
        let exc = vec!["raw/*".to_string()];
        let f = FileFilter::new(&inc, &exc).unwrap();
        assert!(f.matches("data.csv"));
        assert!(!f.matches("raw/data.csv"));
    }
}
