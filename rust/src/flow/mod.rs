// Implementation of the flow network protocol.  See flow_transport.md for more information.
// TODO
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

pub mod connection;
pub mod file_identifier;
mod file_identifier_table;
pub mod frame;
pub mod uid;

// #[test]
// fn test_uid() -> Result<()> {
//     let s = "0123456789abcdeffedcba9876543210";
//     let uid = uid::UID::from_string(s)?;
//     let uid_s = uid.to_string();
//     assert_eq!(uid_s, s);
//     let uid2 = uid::UID::from_string(&uid_s)?;
//     assert_eq!(uid, uid2);
//     assert_eq!(uid.to_u128(), 0x0123456789abcdeffedcba9876543210);
//     Ok(())
// }
