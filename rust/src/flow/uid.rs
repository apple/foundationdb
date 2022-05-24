use super::Result;

#[derive(PartialEq, PartialOrd)]
pub struct UID {
    pub uid: u128,
}
// This needs to be kept in sync with FlowTransport.h, but these almost never change.
// Enum members are spelled like this in C++: WLTOKEN_ENDPOINT_NOT_FOUND
enum WLTOKEN {
	EndpointNotFound = 0, 
	PingPacket,
	AuthTenant,
	UnauthorizedEndpoint,
	FirstAvailable,
}

impl std::fmt::Debug for UID {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "[UID: {:0>16x}]", self.uid)
    }
}


impl UID {
    pub fn new(uid : [u8; 16]) -> Result<Self> {
        Ok(UID {
            uid: u128::from_le_bytes(uid),
        })
    }
    pub fn to_u128 (&self) -> u128 {
        self.uid
    }
    pub fn is_valid(&self) -> bool {
        self.uid != 0
    }

    pub fn from_string(s : &str) -> Result<Self> {
        Ok(UID {
            uid: u128::from_str_radix(s, 16)?,
        })
    }
    pub fn to_string(&self) -> String {
        format!("{:0>32x}", self.uid)
    }
}
