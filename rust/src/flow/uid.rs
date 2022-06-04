use super::Result;

use num_derive::FromPrimitive;
use num_traits::FromPrimitive;

#[derive(PartialEq, PartialOrd)]
pub struct UID {
    pub uid: [u64; 2],
}
// This needs to be kept in sync with FlowTransport.h, but these almost never change.
// Enum members are spelled like this in C++: WLTOKEN_ENDPOINT_NOT_FOUND
#[derive(FromPrimitive, Debug)]
pub enum WLTOKEN {
    EndpointNotFound = 0,
    PingPacket = 1,
    AuthTenant = 2,
    UnauthorizedEndpoint = 3,
    NetworkTest = 4, // XXX the C++ code isn't particularly typesafe...
}

impl std::fmt::Debug for UID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[UID: {:0>16x}, {:0>16x}]", self.uid[0], self.uid[1])
    }
}

impl UID {
    pub fn new(uid: [u8; 16]) -> Result<Self> {
        Ok(UID {
            uid: [
                u64::from_le_bytes(uid[0..8].try_into()?),
                u64::from_le_bytes(uid[8..].try_into()?),
            ],
        })
    }
    #[allow(dead_code)]
    pub fn well_known_token(id: u64) -> UID {
        UID {
            uid: [u64::MAX, id],
        }
    }
    #[allow(dead_code)]
    pub fn get_adjusted_token(&self, index: u32) -> UID {
        let new_index = self.uid[1] + (index as u64);
        UID {
            uid: [
                self.uid[0] + ((index as u64) << 32),
                (self.uid[1] & 0xffff_ffff_0000_0000) | new_index,
            ],
        }
    }

    #[allow(dead_code)]
    pub fn is_valid(&self) -> bool {
        self.uid[0] != 0 || self.uid[1] != 0
    }

    pub fn get_well_known_endpoint(&self) -> Option<WLTOKEN> {
        // TODO: Might need to strip out more bits.  get_adjusted_token stores the index
        // in the upper 32 bits of second (uid[1]), so maybe this is OK?
        WLTOKEN::from_u32(self.uid[1] as u32)
    }
}
