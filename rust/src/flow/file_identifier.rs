use super::Result;

// See the following files:
// flow/FileIdentifier.h
// flow/README.md (section on file identifiers)

pub struct FileIdentifier {
    file_identifier: u32,
}

impl FileIdentifier {
    pub fn new(file_identifier: u32) -> Result<FileIdentifier> {
        if (file_identifier & 0xFF00_0000) != 0 {
            Err("raw file identifier must be < 2^24".into())
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
    pub fn to_reply_promise(&self) -> Result<FileIdentifier> {
        self.compose(1)
    }
    pub fn to_error_or(&self) -> Result<FileIdentifier> {
        self.compose(2)
    }
    pub fn to_vector_ref(&self) -> Result<FileIdentifier> {
        self.compose(3)
    }
    pub fn to_optional(&self) -> Result<FileIdentifier> {
        self.compose(4)
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
