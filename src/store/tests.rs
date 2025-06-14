use super::*;
use std::fs;
use tempfile::TempDir;

#[test]
fn test_store_creation() {
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().join("test_store");
    let store = Store::new(store_path.to_string_lossy().to_string());

    assert!(store_path.exists());
    assert_eq!(store.base_path(), store_path.to_string_lossy());
}

#[test]
fn test_oci_style_commit_and_tagging() {
    let temp_dir = TempDir::new().unwrap();
    let source_path = temp_dir.path().join("source");
    let store_path = temp_dir.path().join("test_store");

    // Create test directory
    fs::create_dir_all(&source_path).unwrap();
    fs::write(source_path.join("file1.txt"), "content1").unwrap();
    fs::write(source_path.join("file2.txt"), "content2").unwrap();

    let store = Store::new(store_path.to_string_lossy().to_string());

    // Import creates a commit
    let commit_id = store
        .commit_directory_bare("myapp", &source_path.to_string_lossy(), None, false)
        .unwrap();

    assert!(!commit_id.is_empty());

    // Verify the directory structure:
    // store/
    //   commits/<commit_id>/
    //     metadata.toml
    //     commit.cfs
    //   refs/myapp/
    //     tags/
    let commits_dir = store_path.join("commits").join(&commit_id);
    assert!(commits_dir.exists());
    assert!(commits_dir.join("metadata.toml").exists());

    let refs_dir = store_path.join("refs").join("myapp");
    assert!(refs_dir.exists());

    // Tag the commit
    store.tag_commit("myapp", &commit_id, "v1.0").unwrap();
    store.tag_commit("myapp", &commit_id, "latest").unwrap();

    // Verify tags exist as symlinks
    let tags_dir = refs_dir.join("tags");
    assert!(tags_dir.exists());
    let v1_tag = tags_dir.join("v1.0");
    let latest_tag = tags_dir.join("latest");
    assert!(v1_tag.exists());
    assert!(latest_tag.exists());

    // Verify they are symlinks
    assert!(v1_tag.symlink_metadata().unwrap().file_type().is_symlink());
    assert!(
        latest_tag
            .symlink_metadata()
            .unwrap()
            .file_type()
            .is_symlink()
    );

    // Resolve tags
    let resolved_v1 = store.resolve_tag("myapp", "v1.0").unwrap();
    let resolved_latest = store.resolve_tag("myapp", "latest").unwrap();
    assert_eq!(resolved_v1, commit_id);
    assert_eq!(resolved_latest, commit_id);

    // List tags
    let tags = store.list_tags("myapp").unwrap();
    assert_eq!(tags.len(), 2);
    assert!(tags.contains(&"v1.0".to_owned()));
    assert!(tags.contains(&"latest".to_owned()));
}

#[test]
fn test_worktree_paths() {
    let temp_dir = TempDir::new().unwrap();
    let store_path = temp_dir.path().join("test_store");
    let store = Store::new(store_path.to_string_lossy().to_string());

    // Test worktree path methods
    let worktrees_path = store.worktrees_path("myapp");
    let main_worktree_path = store.worktree_path("myapp", "main");
    let upperdir_path = store.worktree_upperdir("myapp", "main");
    let workdir_path = store.worktree_workdir("myapp", "main");
    let meta_path = store.worktree_meta_path("myapp", "main");

    // Verify path structure
    assert!(worktrees_path.contains("refs/myapp/worktrees"));
    assert!(main_worktree_path.contains("refs/myapp/worktrees/main"));
    assert!(upperdir_path.contains("refs/myapp/worktrees/main/upperdir"));
    assert!(workdir_path.contains("refs/myapp/worktrees/main/workdir"));
    assert!(meta_path.contains("refs/myapp/worktrees/main/meta.toml"));

    // Verify parent directories are created by the path methods
    assert!(Path::new(&worktrees_path).exists());
    assert!(Path::new(&main_worktree_path).exists());

    // Note: upperdir and workdir themselves aren't created by path methods,
    // only their parent worktree directory is created
    assert!(Path::new(&main_worktree_path).exists());
}
