use std::{
    io::{Error, Read},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

const MESSAGE_TAG_JOIN_CLUSTER_CALL: u8 = 0;

const ADDR_TAG_IPV4: u8 = 4;
const ADDR_TAG_IPV6: u8 = 6;

#[derive(Debug, Clone)]
pub enum Message {
    JoinClusterCall { seqno: u32, from: SocketAddr },
    //JoinClusterReply,
}

impl Message {
    // TODO: Use writer
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Message::JoinClusterCall { seqno, from } => {
                buf.extend(&seqno.to_be_bytes());
                buf.push(MESSAGE_TAG_JOIN_CLUSTER_CALL);

                match from {
                    SocketAddr::V4(addr) => {
                        buf.push(ADDR_TAG_IPV4);
                        buf.extend(&addr.ip().octets());
                        buf.extend(&addr.port().to_be_bytes());
                    }
                    SocketAddr::V6(addr) => {
                        buf.push(ADDR_TAG_IPV6);
                        buf.extend(&addr.ip().octets());
                        buf.extend(&addr.port().to_be_bytes());
                    }
                }
            }
        }
    }

    pub fn decode<R: Read>(mut reader: R) -> std::io::Result<Self> {
        let seqno = read_u32(&mut reader)?;
        let msg_tag = read_u8(&mut reader)?;
        match msg_tag {
            MESSAGE_TAG_JOIN_CLUSTER_CALL => {
                let addr_tag = read_u8(&mut reader)?;
                match addr_tag {
                    ADDR_TAG_IPV4 => {
                        let ip = read_u32(&mut reader)?;
                        let port = read_u16(&mut reader)?;
                        let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port);
                        Ok(Message::JoinClusterCall { seqno, from })
                    }
                    ADDR_TAG_IPV6 => {
                        let ip = read_u128(&mut reader)?;
                        let port = read_u16(&mut reader)?;
                        let from = SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port);
                        Ok(Message::JoinClusterCall { seqno, from })
                    }
                    _ => Err(Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Unknown address tag: {addr_tag}"),
                    )),
                }
            }
            _ => Err(Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown message tag: {msg_tag}"),
            )),
        }
    }
}

fn read_u8<R: Read>(reader: &mut R) -> std::io::Result<u8> {
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn read_u16<R: Read>(reader: &mut R) -> std::io::Result<u16> {
    let mut buf = [0; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf))
}

fn read_u32<R: Read>(reader: &mut R) -> std::io::Result<u32> {
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf))
}

// fn read_u64<R: Read>(reader: &mut R) -> std::io::Result<u64> {
//     let mut buf = [0; 8];
//     reader.read_exact(&mut buf)?;
//     Ok(u64::from_be_bytes(buf))
// }

fn read_u128<R: Read>(reader: &mut R) -> std::io::Result<u128> {
    let mut buf = [0; 16];
    reader.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}
