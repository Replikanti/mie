//! The post-rebuild removal gate's format (#193 B10).

use super::*;
use crate::testutil::TempDir;

#[test]
fn gate_round_trips() {
    for len in [0, 18, 1_000, u64::MAX] {
        let text = render(len);
        assert_eq!(parse(text.as_bytes()).unwrap(), len, "{text:?}");
    }
    assert_eq!(render(1_000), "mie-rebuilt 1\nprefix 1000\n");

    let dir = TempDir::new("rebuilt-gate");
    assert_eq!(read(dir.path()).unwrap(), None);
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    assert_eq!(read(dir.path()).unwrap(), None);
    write(dir.path(), 1_000).unwrap();
    assert_eq!(read(dir.path()).unwrap(), Some(1_000));
    write(dir.path(), 2_000).unwrap();
    assert_eq!(read(dir.path()).unwrap(), Some(2_000));
    assert_eq!(
        fs::read_to_string(dir.path().join(".mie/rebuilt")).unwrap(),
        "mie-rebuilt 1\nprefix 2000\n"
    );
}

/// Anything `render` cannot produce is `Corrupt`, and `read` reports it.
#[test]
fn gate_parser_is_strict() {
    let bad: Vec<&[u8]> = vec![
        b"mie-rebuilt 1\nprefix 01000\n",
        b"mie-rebuilt 1\nprefix +1000\n",
        "mie-rebuilt 1\nprefix １０００\n".as_bytes(),
        b"mie-rebuilt 1\r\nprefix 1000\r\n",
        b"mie-rebuilt 1\nprefix 1000\r\n",
        b"mie-rebuilt 1\nprefix 1000\nx",
        b"mie-rebuilt 1\nprefix 1000\n\n",
        b"mie-rebuilt 1\nprefix 1000",
        b"mie-rebuilt 2\nprefix 1000\n",
        b"mie-rebuilt 1\nprefix  1000\n",
        b"mie-rebuilt 1\nprefix \n",
        b"mie-rebuilt 1\nprefix 18446744073709551616\n",
        b"mie-rebuilt 1\nprefix -1\n",
        b"mie-rebuilt 1\n",
        b"",
        b"mie-rebuilt 1\nprefix \xff\xfe\n",
    ];
    for bytes in bad {
        assert!(
            matches!(parse(bytes), Err(RawStoreError::Corrupt(_))),
            "{:?}",
            String::from_utf8_lossy(bytes)
        );
    }

    let dir = TempDir::new("rebuilt-strict");
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    fs::write(
        dir.path().join(".mie/rebuilt"),
        b"mie-rebuilt 1\nprefix 01000\n",
    )
    .unwrap();
    assert!(matches!(read(dir.path()), Err(RawStoreError::Corrupt(_))));
}
