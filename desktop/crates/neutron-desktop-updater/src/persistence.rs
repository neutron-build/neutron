//! Private durable publication. Errors propagate, including unsupported directory sync.
use std::{fs, io::Write, path::Path};
pub fn private_directory(path: &Path) -> Result<(), String> {
    let mut prefix = std::path::PathBuf::new();
    for part in path.components() {
        if matches!(part, std::path::Component::ParentDir) {
            return Err("Parent path refused".into());
        }
        prefix.push(part.as_os_str());
        match fs::symlink_metadata(&prefix) {
            Ok(m) if m.file_type().is_symlink() || !m.is_dir() => {
                return Err("Stage ancestor is not a real directory".into());
            }
            Ok(_) => (),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut builder = fs::DirBuilder::new();
                #[cfg(unix)]
                {
                    use std::os::unix::fs::DirBuilderExt;
                    builder.mode(0o700);
                }
                builder.create(&prefix).map_err(|e| e.to_string())?;
                if let Some(parent) = prefix.parent() {
                    sync_directory(parent)?;
                }
            }
            Err(e) => return Err(e.to_string()),
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        unsafe extern "C" {
            fn getuid() -> u32;
        }
        let meta = fs::symlink_metadata(path).map_err(|e| e.to_string())?;
        if meta.uid() != unsafe { getuid() } {
            return Err("Stage directory belongs to another owner".into());
        }
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|e| e.to_string())?;
    }
    #[cfg(not(unix))]
    {
        return Err(
            "Private updater staging requires a supported owner-only directory backend".into(),
        );
    }
    #[allow(unreachable_code)]
    sync_directory(path)
}
pub fn sync_directory(path: &Path) -> Result<(), String> {
    #[cfg(unix)]
    {
        fs::File::open(path)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Err("Durable directory synchronization unsupported".into())
    }
}
pub fn publish(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = path.parent().ok_or("Missing stage parent")?;
    private_directory(parent)?;
    if fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_symlink() || !m.is_file()) {
        return Err("Stage destination is not a regular file".into());
    }
    let temp = path.with_extension(format!("{}.tmp", uuid::Uuid::new_v4()));
    let result = (|| {
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temp).map_err(|e| e.to_string())?;
        file.write_all(bytes)
            .and_then(|_| file.sync_all())
            .map_err(|e| e.to_string())?;
        fs::rename(&temp, path).map_err(|e| e.to_string())?;
        sync_directory(parent)
    })();
    if result.is_err() {
        let _ = fs::remove_file(temp);
    }
    result
}
pub fn remove(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => sync_directory(path.parent().ok_or("Missing parent")?),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.to_string()),
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    #[cfg(unix)]
    fn publication_is_private_and_refuses_links() {
        use std::os::unix::fs::MetadataExt;
        let root = std::env::temp_dir()
            .canonicalize()
            .unwrap()
            .join(format!("neutron-update-{}", uuid::Uuid::new_v4()));
        let file = root.join("stage");
        publish(&file, b"old").unwrap();
        publish(&file, b"new").unwrap();
        assert_eq!(fs::read(&file).unwrap(), b"new");
        assert_eq!(fs::metadata(&root).unwrap().mode() & 0o777, 0o700);
        assert_eq!(fs::metadata(&file).unwrap().mode() & 0o777, 0o600);
        let link = root.join("link");
        std::os::unix::fs::symlink(&file, &link).unwrap();
        assert!(publish(&link, b"bad").is_err());
        fs::remove_dir_all(root).unwrap();
    }
}
