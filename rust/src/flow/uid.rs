use super::{Error, Result};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};

#[derive(PartialEq, PartialOrd, Hash, Eq, Clone)]
pub struct UID {
    pub uid: [u64; 2],
}
// This needs to be kept in sync with FlowTransport.h, but these almost never change.
// Enum members are spelled like this in C++: WLTOKEN_ENDPOINT_NOT_FOUND
#[derive(PartialEq, FromPrimitive, ToPrimitive, Debug, Eq, Hash)]
pub enum WLTOKEN {
    EndpointNotFound = 0,
    PingPacket = 1,
    AuthTenant = 2,
    UnauthorizedEndpoint = 3,
    ClientLeaderRegGetLeader = 4,
    ClientLeaderRegOpenDatabase = 5,
    LeaderElectionRegCandidacy = 6,
    LeaderElectionRegElectionResult = 7,
    LeaderElectionRegLeaderHeartBeat = 8,
    LeaderElectionRegForward = 9,
    ProtocolInfo = 10,
    GenerationRegRead = 11,
    GenerationRegWrite = 12,
    ClientLeaderRegDescriptorMutable = 13,
    ConfigTxnGetGeneration = 14,
    ConfigTxnGet = 15,
    ConfigTxnGetClasses = 16,
    ConfigTxnGetKnobs = 17,
    ConfigTxnCommit = 18,
    ConfigFollowerGetSnapshotAndChanges = 19,
    ConfigFollowerGetChanges = 20,
    ConfigFollowerCompact = 21,
    ConfigFollowerRollForward = 22,
    ConfigFollowerGetCommittedVersion = 23,
    Process = 24,
    ReservedForTesting = 25,
    ReservedCount = 26,
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
    pub fn well_known_token(id: WLTOKEN) -> UID {
        UID {
            uid: [u64::MAX, id.to_u64().unwrap()],
        }
    }
    pub fn random_token() -> UID {
        UID {
            uid: [fastrand::u64(..), fastrand::u64(..)],
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

impl From<&crate::common_generated::UID> for UID {
    fn from(uid : &crate::common_generated::UID) -> UID {
        UID{ uid: [uid.first(), uid.second() ]}
    }
}

impl From<&UID> for crate::common_generated::UID {
    fn from(uid : &UID) -> crate::common_generated::UID {
        let mut arry = [0u8;16];
        arry[0..8].copy_from_slice(&uid.uid[0].to_le_bytes());
        arry[8..16].copy_from_slice(&uid.uid[1].to_le_bytes());
        crate::common_generated::UID(arry)
    }
}