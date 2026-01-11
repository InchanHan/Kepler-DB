use bytes::Bytes;
use kepler::Kepler;
use tempfile::tempdir;

#[test]
fn insert_and_get() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;

    db.insert(b"hello", b"world!")?;
    db.insert(b"blue", b"sky")?;
    db.insert(b"wtf", b"isthis")?;
    
    assert_eq!(db.get(b"hello")?, Some(Bytes::from("world!")));
    assert_eq!(db.get(b"wtf")?, Some(Bytes::from("isthis")));

    Ok(())
}

#[test]
fn overwrite_previous_val() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;

    db.insert(b"water", b"melon")?;
    db.insert(b"water", b"park")?;

    assert_eq!(db.get(b"water")?, Some(Bytes::from("park")));

    db.insert(b"water", b"jelly")?;

    assert_eq!(db.get(b"water")?, Some(Bytes::from("jelly")));

    Ok(())
}

#[test]
fn try_get_with_false_key() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;
    
    assert_eq!(db.get(b"black")?, None);

    Ok(())
}

#[test]
fn try_get_after_remove() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;

    db.insert(b"fizz", b"buzz")?;
    db.remove(b"fizz")?;
    
    assert_eq!(db.get(b"fizz")?, None);

    Ok(())
}

#[test]
fn try_remove_before_insert() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;

    db.remove(b"ambient")?;

    Ok(())
}

#[test]
fn try_get_after_flush() -> kepler::Result<()> {
    let dir = tempdir()?;
    let db = Kepler::new(dir.path())?;

    let big_val = vec![1u8; 32 * 1024 * 1024]; //ACTIVE_CAP_MAX = 32MB

    db.insert(b"bytes", &big_val)?;
    db.insert(b"space", b"monkey")?;

    assert_eq!(db.get(b"space")?, Some(Bytes::from("monkey")));
    assert_eq!(db.get(b"bytes")?, Some(Bytes::copy_from_slice(&big_val)));

    Ok(())
}

