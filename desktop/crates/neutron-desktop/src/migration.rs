//! Explicit offline migration. The caller must stop all users of the legacy store.
use std::path::{Path, PathBuf};

/// Copy a chosen legacy directory to an unused app-owned destination, preserving
/// both the original and a backup. Refuses conflicts and symlinks. Never runs
/// implicitly: legacy stores may contain another application's data.
pub fn migrate_legacy_storage(source: &Path, destination: &Path) -> std::io::Result<PathBuf> {
    use std::io::{Error, ErrorKind};
    fn inventory(path: &Path) -> std::io::Result<()> {
        let meta = std::fs::symlink_metadata(path)?;
        if meta.file_type().is_symlink() || !(meta.is_dir() || meta.is_file()) {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "Migration refuses links and special files",
            ));
        }
        if meta.is_dir() {
            for entry in std::fs::read_dir(path)? {
                inventory(&entry?.path())?;
            }
        }
        Ok(())
    }
    fn copy_tree(source: &Path, dest: &Path) -> std::io::Result<()> {
        inventory(source)?;
        if source.is_dir() {
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            builder.create(dest)?;
            for entry in std::fs::read_dir(source)? {
                let entry = entry?;
                copy_tree(&entry.path(), &dest.join(entry.file_name()))?;
            }
            sync_dir(dest)?;
        } else {
            use std::io::Write;
            let mut input = std::fs::File::open(source)?;
            let mut options = std::fs::OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut output = options.open(dest)?;
            std::io::copy(&mut input, &mut output)?;
            output.flush()?;
            output.sync_all()?;
        }
        Ok(())
    }
    // Check the caller-supplied namespace BEFORE canonicalization can hide links.
    fn no_links(path: &Path) -> std::io::Result<()> {
        let absolute = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        let mut prefix = PathBuf::new();
        for part in absolute.components() {
            if matches!(part, std::path::Component::ParentDir) {
                return Err(Error::new(
                    ErrorKind::InvalidInput,
                    "Parent components refused",
                ));
            }
            prefix.push(part.as_os_str());
            match std::fs::symlink_metadata(&prefix) {
                Ok(meta) if meta.file_type().is_symlink() => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        "Migration path contains a link",
                    ));
                }
                Ok(_) => (),
                Err(e) if e.kind() == ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    fn sync_dir(path: &Path) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            std::fs::File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Err(Error::new(
                ErrorKind::Unsupported,
                "Durable offline migration requires directory synchronization",
            ))
        }
    }
    no_links(source)?;
    no_links(destination)?;
    let source = source.canonicalize()?;
    if !source.is_dir() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Legacy source must be a directory",
        ));
    }
    let parent = destination
        .parent()
        .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing destination parent"))?;
    // Caller must exclusively own this destination parent and all newly created
    // ancestors. This API is offline, never a shared-directory merge operation.
    fn private_parent(path: &Path) -> std::io::Result<()> {
        if path.exists() {
            return Ok(());
        }
        let parent = path
            .parent()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing ancestor"))?;
        private_parent(parent)?;
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder.create(path)?;
        sync_dir(parent)
    }
    private_parent(parent)?;
    no_links(parent)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        unsafe extern "C" {
            fn getuid() -> u32;
        }
        let metadata = std::fs::metadata(parent)?;
        if metadata.uid() != unsafe { getuid() } || metadata.mode() & 0o077 != 0 {
            return Err(Error::new(
                ErrorKind::PermissionDenied,
                "Destination parent must be owner-only and owned by the caller",
            ));
        }
    }
    sync_dir(parent)?;
    let destination = parent.canonicalize()?.join(
        destination
            .file_name()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "Missing destination name"))?,
    );
    let backup = destination.with_extension("legacy-backup");
    if destination.starts_with(&source) || source.starts_with(&destination) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "Source and destination overlap",
        ));
    }
    if destination.symlink_metadata().is_ok() || backup.symlink_metadata().is_ok() {
        return Err(Error::new(
            ErrorKind::AlreadyExists,
            "Migration destination or backup already exists",
        ));
    }
    inventory(&source)?;
    copy_tree(&source, &backup)?;
    sync_dir(parent)?;
    let staging = destination.with_extension(format!("migration-{}", std::process::id()));
    copy_tree(&backup, &staging)?;
    if destination.symlink_metadata().is_ok() {
        return Err(Error::new(
            ErrorKind::AlreadyExists,
            "Migration destination appeared during offline copy",
        ));
    }
    std::fs::rename(&staging, &destination)?;
    sync_dir(parent)?;
    Ok(backup)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preserves_source_and_backup_refuses_conflict_and_links() {
        let root = std::env::temp_dir().join(format!("neutron-migration-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let source = root.join("old");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("database"), b"authentic fixture").unwrap();
        let dest = root.join("app-a").join("nucleus");
        let backup = migrate_legacy_storage(&source, &dest).unwrap();
        for dir in [&source, &backup, &dest] {
            assert_eq!(
                std::fs::read(dir.join("database")).unwrap(),
                b"authentic fixture"
            );
        }
        assert!(migrate_legacy_storage(&source, &dest).is_err());
        assert!(migrate_legacy_storage(&source, &source.join("nested")).is_err());
        #[cfg(unix)]
        {
            let root_link = root.join("root-link");
            std::os::unix::fs::symlink(&source, &root_link).unwrap();
            assert!(migrate_legacy_storage(&root_link, &root.join("root-target")).is_err());
            let ancestor_link = root.join("ancestor-link");
            std::os::unix::fs::symlink(&root, &ancestor_link).unwrap();
            assert!(
                migrate_legacy_storage(&ancestor_link.join("old"), &root.join("ancestor-target"))
                    .is_err()
            );
            assert!(migrate_legacy_storage(&source, &ancestor_link.join("dest")).is_err());
            std::os::unix::fs::symlink(source.join("database"), source.join("link")).unwrap();
            assert!(migrate_legacy_storage(&source, &root.join("app-b")).is_err());
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
