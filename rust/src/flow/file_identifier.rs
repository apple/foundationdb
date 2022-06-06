use super::Result;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use std::collections::HashMap;
// See the following files:
// flow/FileIdentifier.h
// flow/README.md (section on file identifiers)

#[derive(Debug)]
pub struct FileIdentifier {
    file_identifier: u32,
}

impl FileIdentifier {
    pub fn new(file_identifier: u32) -> Result<FileIdentifier> {
        if (file_identifier & 0xFF00_0000) != 0 {
            Err(format!("raw file identifier {} must be < 2^24", file_identifier).into())
        } else {
            Ok(FileIdentifier { file_identifier })
        }
    }
    pub fn new_from_wire(file_identifier: u32) -> Result<FileIdentifier> {
        let inner = (file_identifier >> 24) & 0xF;
        let outer = (file_identifier >> 28) & 0xF;
        if inner > 4 {
            Err(format!("Invalid inner wrapper type {:x}", file_identifier).into())
        } else if outer > 4 {
            Err(format!("Invalid inner wrapper type {:x}", file_identifier).into())
        } else {
            Ok(FileIdentifier { file_identifier })
        }
    }
    fn compose(&self, b: u8) -> Result<FileIdentifier> {
        if self.is_nested_composed() {
            Err("Attempt to double nest FileIdentifier".into())
        } else {
            Ok(FileIdentifier {
                file_identifier: if self.is_composed() {
                    self.file_identifier | ((b as u32) << 28)
                } else {
                    self.file_identifier | ((b as u32) << 24)
                },
            })
        }
    }
    fn is_composed(&self) -> bool {
        (self.file_identifier & 0xF << 24) != 0
    }
    fn is_nested_composed(&self) -> bool {
        (self.file_identifier & 0xF << 28) != 0
    }
    #[allow(dead_code)]
    pub fn to_reply_promise(&self) -> Result<FileIdentifier> {
        self.compose(IdentifierType::ReplyPromise.to_u8().unwrap())
    }
    pub fn to_error_or(&self) -> Result<FileIdentifier> {
        self.compose(IdentifierType::ErrorOr.to_u8().unwrap())
    }
    #[allow(dead_code)]
    pub fn to_vector_ref(&self) -> Result<FileIdentifier> {
        self.compose(IdentifierType::VectorRef.to_u8().unwrap())
    }
    #[allow(dead_code)]
    pub fn to_optional(&self) -> Result<FileIdentifier> {
        self.compose(IdentifierType::Optional.to_u8().unwrap())
    }

    // need to tell flatbuffers to put a file identifier in, but FDB flatbuffers
    // are not necessarily valid UTF-8.  Therefore they are not necessarily valid
    // flatbuffers.
    pub fn rewrite_flatbuf(&self, flatbuf: &mut [u8]) -> Result<FileIdentifier> {
        if flatbuf.len() < 8 {
            return Err("flatbuf is not long enough to contain a file identifier!".into());
        }
        let old_id = u32::from_le_bytes(flatbuf[4..8].try_into()?);
        flatbuf[4..8].copy_from_slice(&u32::to_le_bytes(self.file_identifier));
        Ok(FileIdentifier {
            file_identifier: old_id,
        })
    }
}
#[derive(Debug, FromPrimitive, ToPrimitive, PartialEq)]
pub enum IdentifierType {
    None = 0,
    ReplyPromise = 1,
    ErrorOr = 2,
    VectorRef = 3,
    Optional = 4,
}

#[derive(Debug, PartialEq)]
pub struct ParsedFileIdentifier {
    pub file_identifier: u32,
    pub inner_wrapper: IdentifierType,
    pub outer_wrapper: IdentifierType,
    pub file_identifier_name: Option<&'static str>,
}
pub struct FileIdentifierNames {
    name_to_id: HashMap<&'static str, u32>,
    id_to_name: HashMap<u32, &'static str>,
}

impl FileIdentifierNames {
    pub fn new() -> Result<FileIdentifierNames> {
        let name_to_id = super::file_identifier_table::file_identifier_table();
        let mut id_to_name: HashMap<u32, &'static str> = HashMap::new();

        for i in &name_to_id {
            id_to_name.insert(*i.1, *i.0);
        }
        let fin = FileIdentifierNames {
            name_to_id,
            id_to_name,
        };
        Ok(fin)
    }
    pub fn from_id(&self, id: FileIdentifier) -> Result<ParsedFileIdentifier> {
        let id = id.file_identifier;
        let file_identifier = id & 0x00FF_FFFF;
        let inner_wrapper = IdentifierType::from_u16((id >> 24) as u16 & 0xF)
            .ok_or::<super::Error>(format!("Invalid inner_wrapper: {:0x}", id).into())?;
        let outer_wrapper = IdentifierType::from_u16((id >> 28) as u16 & 0xF)
            .ok_or::<super::Error>(format!("Invalid outer_wrapper: {:0x}", id).into())?;
        let file_identifier_name = match self.id_to_name.get(&file_identifier) {
            Some(s) => Some(*s),
            None => None,
        };
        Ok(ParsedFileIdentifier {
            file_identifier,
            inner_wrapper,
            outer_wrapper,
            file_identifier_name,
        })
    }
    #[allow(dead_code)]
    pub fn name_to_raw(&self, name: &'static str) -> Result<FileIdentifier> {
        match self.name_to_id.get(name) {
            None => Err("Name not found!".into()),
            Some(&file_identifier) => Ok(FileIdentifier { file_identifier }),
        }
    }
}

#[test]
fn test_file_identifier() -> Result<()> {
    let fi = FileIdentifier::new(0x1ead4a)?;
    let err_or_fi = fi.to_error_or()?;
    assert_eq!(0x21ead4a, err_or_fi.file_identifier);
    assert_eq!(FileIdentifier::new(0x21ead4a).is_err(), true);

    let reply_err_or_fi = err_or_fi.to_reply_promise()?;
    assert_eq!(0x121ead4a, reply_err_or_fi.file_identifier);
    assert_eq!(reply_err_or_fi.to_optional().is_err(), true);

    Ok(())
}

#[test]
fn test_file_identifier_table() -> Result<()> {
    let table = super::file_identifier_table::file_identifier_table();
    let mut failed = false;
    for t in table {
        match FileIdentifier::new(t.1) {
            Ok(_) => (),
            Err(e) => {
                failed = true;
                println!("Invalid file_identifier {} ({:0x}): {}", t.0, t.1, e);
            }
        }
    }
    if failed {
        Err("test failed".into())
    } else {
        Ok(())
    }
}

#[test]
fn test_file_identifier_names() -> Result<()> {
    let names = FileIdentifierNames::new()?;
    let parsed = names.from_id(FileIdentifier {
        file_identifier: 0x121ead4a,
    })?;
    assert_eq!(parsed.file_identifier, 0x1ead4a);
    assert_eq!(parsed.inner_wrapper, IdentifierType::ErrorOr);
    assert_eq!(parsed.outer_wrapper, IdentifierType::ReplyPromise);
    assert_eq!(parsed.file_identifier_name, Some("Void"));
    if false {
        // handy for translating FDB traces about mismatched file identifiers
        // into human-readable form.
        assert_eq!(
            names
                .from_id(FileIdentifier {
                    file_identifier: 35564874
                })
                .unwrap(),
            names
                .from_id(FileIdentifier {
                    file_identifier: 48019806
                })
                .unwrap()
        );
    }
    Ok(())
}
