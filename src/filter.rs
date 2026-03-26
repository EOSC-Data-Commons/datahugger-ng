use globset::{GlobBuilder, GlobSet, GlobSetBuilder};

/// A compiled set of glob patterns for filtering file entries by relative path.
///
/// When no patterns are provided (via `accept_all()`), all files pass the filter.
/// When one or more patterns are provided, a file passes if it matches ANY pattern.
#[derive(Clone, Debug)]
pub struct FileFilter {
    inner: Option<GlobSet>,
}

impl FileFilter {
    /// Creates a filter that accepts all files (no filtering).
    #[must_use]
    pub fn accept_all() -> Self {
        FileFilter { inner: None }
    }

    /// Creates a filter from a list of glob pattern strings.
    ///
    /// # Errors
    /// Returns an error if any pattern is invalid.
    pub fn from_patterns(patterns: &[String]) -> Result<Self, globset::Error> {
        if patterns.is_empty() {
            return Ok(Self::accept_all());
        }
        let mut builder = GlobSetBuilder::new();
        for pattern in patterns {
            // Normalize Windows backslashes to forward slashes so patterns like
            // "subdir\*.csv" match CrawlPaths which always use '/'.
            let pattern = pattern.replace('\\', "/");

            // Patterns without a path separator match at any depth (like rsync/rclone).
            // globset only auto-prefixes `**/` for patterns with wildcards, so we
            // normalize literal filenames (e.g. "file.h5" → "**/file.h5") ourselves.
            // Bare "**" is left as-is since it already matches everything.
            let normalized = if pattern == "**"
                || pattern.starts_with("**/")
                || pattern.contains('/')
            {
                pattern
            } else {
                format!("**/{pattern}")
            };

            // Explicitly enforce case-sensitive matching on all platforms so that
            // remote API paths (which have fixed casing) behave consistently.
            let glob = GlobBuilder::new(&normalized)
                .case_insensitive(false)
                .build()?;
            builder.add(glob);
        }
        Ok(FileFilter {
            inner: Some(builder.build()?),
        })
    }

    /// Returns true if the given relative path matches the filter.
    ///
    /// If no patterns are set, always returns true.
    #[must_use]
    pub fn matches(&self, path: &str) -> bool {
        match &self.inner {
            None => true,
            Some(globset) => globset.is_match(path),
        }
    }

    /// Returns true if this filter accepts all files (no patterns set).
    #[must_use]
    pub fn is_accept_all(&self) -> bool {
        self.inner.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_all_matches_everything() {
        let f = FileFilter::accept_all();
        assert!(f.matches("anything.csv"));
        assert!(f.matches("dir/file.txt"));
        assert!(f.is_accept_all());
    }

    #[test]
    fn single_extension_pattern() {
        let f = FileFilter::from_patterns(&["*.csv".to_string()]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("dir/data.csv"));
        assert!(!f.matches("data.tsv"));
    }

    #[test]
    fn multiple_patterns_or_semantics() {
        let f = FileFilter::from_patterns(&["*.csv".to_string(), "*.tsv".to_string()]).unwrap();
        assert!(f.matches("a.csv"));
        assert!(f.matches("b.tsv"));
        assert!(!f.matches("c.json"));
    }

    #[test]
    fn exact_filename() {
        let f = FileFilter::from_patterns(&["specific_file.h5".to_string()]).unwrap();
        assert!(f.matches("specific_file.h5"));
        assert!(f.matches("dir/specific_file.h5"));
        assert!(!f.matches("other_file.h5"));
    }

    #[test]
    fn subdir_glob() {
        let f = FileFilter::from_patterns(&["subdir/*".to_string()]).unwrap();
        assert!(f.matches("subdir/file.csv"));
        assert!(!f.matches("other/file.csv"));
    }

    #[test]
    fn empty_patterns_is_accept_all() {
        let f = FileFilter::from_patterns(&[]).unwrap();
        assert!(f.is_accept_all());
        assert!(f.matches("anything"));
    }

    #[test]
    fn invalid_pattern_returns_error() {
        let result = FileFilter::from_patterns(&["[invalid".to_string()]);
        assert!(result.is_err());
    }

    #[test]
    fn pattern_with_explicit_double_star_prefix() {
        // Patterns already starting with **/ should not get double-prefixed.
        let f = FileFilter::from_patterns(&["**/data.csv".to_string()]).unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("sub/data.csv"));
        assert!(f.matches("a/b/c/data.csv"));
        assert!(!f.matches("data.tsv"));
    }

    #[test]
    fn pattern_with_path_separator_skips_normalization() {
        // Patterns containing '/' are used as-is (no **/ prefix).
        let f = FileFilter::from_patterns(&["data/results/*.csv".to_string()]).unwrap();
        assert!(f.matches("data/results/output.csv"));
        assert!(!f.matches("other/results/output.csv"));
        assert!(!f.matches("data/results/output.tsv"));
    }

    #[test]
    fn deeply_nested_file_matches_extension_glob() {
        let f = FileFilter::from_patterns(&["*.h5".to_string()]).unwrap();
        assert!(f.matches("a/b/c/d/e/model.h5"));
        assert!(!f.matches("a/b/c/d/e/model.csv"));
    }

    #[test]
    fn filter_is_not_accept_all_when_patterns_set() {
        let f = FileFilter::from_patterns(&["*.csv".to_string()]).unwrap();
        assert!(!f.is_accept_all());
    }

    #[test]
    fn clone_produces_independent_filter() {
        let f = FileFilter::from_patterns(&["*.csv".to_string()]).unwrap();
        let f2 = f.clone();
        // Both should work independently.
        assert!(f.matches("data.csv"));
        assert!(f2.matches("data.csv"));
        assert!(!f2.matches("data.tsv"));
    }

    #[test]
    fn case_sensitive_matching() {
        // We explicitly enforce case-sensitive matching on all platforms
        // so remote API paths behave consistently.
        let f = FileFilter::from_patterns(&["*.CSV".to_string()]).unwrap();
        assert!(f.matches("data.CSV"));
        assert!(!f.matches("data.csv"));
    }

    #[test]
    fn pattern_matching_full_path_with_leading_slash() {
        // Relative paths should not start with '/' in practice,
        // but verify the filter handles it without panic.
        let f = FileFilter::from_patterns(&["*.csv".to_string()]).unwrap();
        // globset may or may not match leading-slash paths — just ensure no panic.
        let _ = f.matches("/root/data.csv");
    }

    #[test]
    fn mixed_pattern_types() {
        // Combine extension glob, exact filename, and path glob.
        let f = FileFilter::from_patterns(&[
            "*.csv".to_string(),
            "README.md".to_string(),
            "docs/*.pdf".to_string(),
        ])
        .unwrap();
        assert!(f.matches("data.csv"));
        assert!(f.matches("sub/README.md"));
        assert!(f.matches("docs/paper.pdf"));
        assert!(!f.matches("data.json"));
        assert!(!f.matches("src/main.rs"));
    }

    #[test]
    fn windows_backslash_normalized_to_forward_slash() {
        let f = FileFilter::from_patterns(&["subdir\\*.csv".to_string()]).unwrap();
        assert!(f.matches("subdir/data.csv"));
        assert!(!f.matches("other/data.csv"));
    }

    #[test]
    fn bare_double_star_matches_everything() {
        // "**" should match root-level and nested files without being mangled to "**/**".
        let f = FileFilter::from_patterns(&["**".to_string()]).unwrap();
        assert!(f.matches("README.md"));
        assert!(f.matches("sub/data.csv"));
        assert!(f.matches("a/b/c/deep.txt"));
    }
}
