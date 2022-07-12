use super::{Error, Result};
use regex::Regex;
use std::net::SocketAddr;
use std::str::FromStr;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum ClusterHost {
    IPAddr(SocketAddr, bool /*tls?*/),
    Hostname(String, u16, bool /*tls?*/),
}
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ClusterFile {
    pub description: String,
    pub id: String,
    pub hosts: Vec<ClusterHost>,
}

impl FromStr for ClusterFile {
    type Err = Error;

    fn from_str(input: &str) -> Result<Self> {
        let input = input.trim();
        // expect "key:desc@host:port[,host:port...]"
        let tokens: Vec<&str> = input.split('@').collect();
        if tokens.len() != 2 {
            return Err(format!("Expected exactly one @ sign in input.  Got: {}", input).into());
        }
        let key = tokens[0];
        let host_list = tokens[1];

        let desc_id_tokens: Vec<&str> = key.split(":").collect();
        if desc_id_tokens.len() != 2 {
            return Err(format!(
                "Expected key:desc before @ sign.  Got {:?} from {}",
                desc_id_tokens, input
            )
            .into());
        }
        let description = desc_id_tokens[0].to_string();
        let id = desc_id_tokens[1].to_string();

        if !description
            .chars()
            .all(|x| char::is_alphanumeric(x) || x == '_')
        {
            return Err(format!(
                "Description should only contain a-z, A-Z, 0-9, _  Got: {}",
                description
            )
            .into());
        }

        if !id.chars().all(char::is_alphanumeric) {
            return Err(format!("ID should only contain a-z, A-Z, 0-9  Got: {}", id).into());
        }

        let host_port_tokens = host_list.split(",");

        let mut hosts = Vec::<ClusterHost>::new();
        let mut uniq = std::collections::HashSet::<ClusterHost>::new();
        for host_port in host_port_tokens {
            let tokens: Vec<&str> = host_port.split(":").collect();
            let (tls_mode, host_port) = match tokens.len() {
                3 => {
                    if tokens[2] == "tls" {
                        (true, format!("{}:{}", tokens[0], tokens[1]))
                    } else {
                        return Err(format!("Expected host:port[:tls]  Got: {}", host_port).into());
                    }
                }
                2 => (false, host_port.to_string()),
                _ => {
                    return Err(format!("Expected host:port[:tls]  Got: {}", host_port).into());
                }
            };

            let host = match host_port.parse() {
                Ok(sockaddr) => ClusterHost::IPAddr(sockaddr, tls_mode),
                Err(_) => {
                    let host_regex = Regex::new(r"^([\w\-]+\.?)+$")?;
                    if host_regex.is_match(tokens[0]) {
                        ClusterHost::Hostname(tokens[0].to_string(), tokens[1].parse()?, tls_mode)
                    } else {
                        return Err(format!("Invalid hostname / IP: {}", tokens[0]).into());
                    }
                }
            };
            // TODO: This doesn't notice if the same IP:port is registered with + without TLS support...
            if !uniq.insert(host.clone()) {
                return Err(format!("Duplicate entry for {:?}", host).into());
            }
            hosts.push(host);
        }
        Ok(Self {
            description,
            id,
            hosts,
        })
    }
}

#[test]
fn test_cluster_file() -> Result<()> {
    ClusterFile::from_str("45FiQ41H:45FiQ41H@127.0.0.1:4000")?;
    ClusterFile::from_str("test:test@127.0.0.1:4501")?;
    ClusterFile::from_str("test:test@127.0.0.1:4501:tls,127.0.0.1:4502:tls,127.0.0.1:4503:tls")?;
    ClusterFile::from_str("test:test@127.0.0.1:4501:tls,127.0.0.1:4502:tls,127.0.0.1:4502:tls")
        .expect_err("Expected error!");
    ClusterFile::from_str("test:test@localhost:4501")?;
    ClusterFile::from_str("test:test@localhost:4501:tls")?;

    Ok(())
}
