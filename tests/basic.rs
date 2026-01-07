use bytes::Bytes;
use kepler::{Kepler, KeplerResult};
use tempfile::tempdir;

#[test]
fn insert_and_get() -> KeplerResult<()> {
    let temp = tempdir()?;
    let db = Kepler::new(temp.path())?;

    db.insert(b"hello", b"world!")?;
    db.insert(b"blue", b"sky")?;
    db.insert(b"wtf", b"isthis")?;
    let found_val_a = db.get(b"hello")?;
    let found_val_b = db.get(b"wtf")?;

    assert_eq!(found_val_a, Some(Bytes::from("world!")));
    assert_eq!(found_val_b, Some(Bytes::from("isthis")));

    Ok(())
}

#[test]
fn overwrite_previous_val() -> KeplerResult<()> {
    let temp = tempdir()?;
    let db = Kepler::new(temp.path())?;
    
    db.insert(b"water", b"melon")?;
    db.insert(b"water", b"park")?;
    let found_val = db.get(b"water")?;

    assert_eq!(found_val, Some(Bytes::from("park")));

    db.insert(b"water", b"jelly")?;
    let found_val = db.get(b"water")?;

    assert_eq!(found_val, Some(Bytes::from("jelly")));

    Ok(())
}

#[test]
fn try_get_with_false_key() -> KeplerResult<()> {
    let temp = tempdir()?;
    let db = Kepler::new(temp.path())?;
    
    let found_val = db.get(b"black")?;

    assert_eq!(found_val, None);

    Ok(())
}

#[test]
fn try_get_after_remove() -> KeplerResult<()> {
    let temp = tempdir()?;
    let db = Kepler::new(temp.path())?;

    db.insert(b"fizz", b"buzz")?;
    db.remove(b"fizz")?;
    let found_val = db.get(b"fizz")?;
    
    assert_eq!(found_val, None);

    Ok(())
}

#[test]
fn try_remove_before_insert() -> KeplerResult<()> {
    let temp = tempdir()?;
    let db = Kepler::new(temp.path())?;

    db.remove(b"ambient")?;
    
    Ok(())
}

