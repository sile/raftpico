use raftbare::{CommitPromise, LogIndex, LogPosition, NodeId, Term};
use std::{
    io::{Error, ErrorKind, Read, Write},
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
};

const MESSAGE_TAG_CLUSTER_CALL: u8 = 0;
const MESSAGE_TAG_CLUSTER_REPLY: u8 = 1;
const MESSAGE_TAG_RAFT_MESSAGE_CAST: u8 = 2;

const ADDR_TAG_IPV4: u8 = 4;
const ADDR_TAG_IPV6: u8 = 6;

#[derive(Debug, Clone)]
pub enum Message {
    JoinCall {
        seqno: u32,
        from: SocketAddr,
    },
    JoinReply {
        seqno: u32,
        node_id: NodeId,
        promise: CommitPromise,
    },
    RaftMessageCast {
        seqno: u32, // TODO(?): delete
        msg: raftbare::Message,
    },
}

impl Message {
    pub fn seqno(&self) -> u32 {
        match self {
            Self::JoinCall { seqno, .. } => *seqno,
            Self::JoinReply { seqno, .. } => *seqno,
            Self::RaftMessageCast { seqno, .. } => *seqno,
        }
    }

    pub fn is_reply(&self) -> bool {
        matches!(self, Self::JoinReply { .. })
    }

    // TODO: Use writer
    pub fn encode(&self, buf: &mut Vec<u8>) {
        match self {
            Self::JoinCall { seqno, from } => {
                buf.push(MESSAGE_TAG_CLUSTER_CALL);
                buf.extend(&seqno.to_be_bytes());

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
            Self::JoinReply {
                seqno,
                node_id,
                promise,
            } => {
                buf.push(MESSAGE_TAG_CLUSTER_REPLY);
                buf.extend(&seqno.to_be_bytes());
                buf.extend(&node_id.get().to_be_bytes());
                encode_commit_promise(buf, *promise).expect("TODO");
            }
            Self::RaftMessageCast { seqno, msg } => {
                buf.push(MESSAGE_TAG_RAFT_MESSAGE_CAST);
                buf.extend(&seqno.to_be_bytes());
                encode_raft_message(buf, msg).expect("TODO");
            }
        }
    }

    pub fn decode<R: Read>(mut reader: R) -> std::io::Result<Self> {
        let msg_tag = read_u8(&mut reader)?;
        let seqno = read_u32(&mut reader)?;
        match msg_tag {
            MESSAGE_TAG_CLUSTER_CALL => {
                let addr_tag = read_u8(&mut reader)?;
                match addr_tag {
                    ADDR_TAG_IPV4 => {
                        let ip = read_u32(&mut reader)?;
                        let port = read_u16(&mut reader)?;
                        let from = SocketAddr::new(IpAddr::V4(Ipv4Addr::from(ip)), port);
                        Ok(Self::JoinCall { seqno, from })
                    }
                    ADDR_TAG_IPV6 => {
                        let ip = read_u128(&mut reader)?;
                        let port = read_u16(&mut reader)?;
                        let from = SocketAddr::new(IpAddr::V6(Ipv6Addr::from(ip)), port);
                        Ok(Self::JoinCall { seqno, from })
                    }
                    _ => Err(Error::new(
                        ErrorKind::InvalidData,
                        format!("Unknown address tag: {addr_tag}"),
                    )),
                }
            }
            MESSAGE_TAG_CLUSTER_REPLY => {
                let node_id = read_u64(&mut reader)?;
                let promise = decode_commit_promise(&mut reader)?;
                Ok(Self::JoinReply {
                    seqno,
                    node_id: NodeId::new(node_id),
                    promise,
                })
            }
            MESSAGE_TAG_RAFT_MESSAGE_CAST => {
                let msg = decode_raft_message(&mut reader)?;
                Ok(Self::RaftMessageCast { seqno, msg })
            }
            _ => Err(Error::new(
                ErrorKind::InvalidData,
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

fn read_u64<R: Read>(reader: &mut R) -> std::io::Result<u64> {
    let mut buf = [0; 8];
    reader.read_exact(&mut buf)?;
    Ok(u64::from_be_bytes(buf))
}

fn read_u128<R: Read>(reader: &mut R) -> std::io::Result<u128> {
    let mut buf = [0; 16];
    reader.read_exact(&mut buf)?;
    Ok(u128::from_be_bytes(buf))
}

fn encode_commit_promise<W: Write>(writer: &mut W, promise: CommitPromise) -> std::io::Result<()> {
    let tag = match promise {
        CommitPromise::Pending(_) => 0,
        CommitPromise::Rejected(_) => 1,
        CommitPromise::Accepted(_) => 2,
    };
    writer.write_all(&[tag])?;

    let position = promise.log_position();
    writer.write_all(&position.term.get().to_be_bytes())?;
    writer.write_all(&position.index.get().to_be_bytes())?;

    Ok(())
}

fn decode_commit_promise<R: Read>(reader: &mut R) -> std::io::Result<CommitPromise> {
    let tag = read_u8(reader)?;
    let term = read_u64(reader)?;
    let index = read_u64(reader)?;

    let position = LogPosition {
        term: Term::new(term),
        index: LogIndex::new(index),
    };

    match tag {
        0 => Ok(CommitPromise::Pending(position)),
        1 => Ok(CommitPromise::Rejected(position)),
        2 => Ok(CommitPromise::Accepted(position)),
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            format!("Unknown commit promise tag: {tag}"),
        )),
    }
}

fn encode_raft_message<W: Write>(writer: &mut W, msg: &raftbare::Message) -> std::io::Result<()> {
    todo!()
}

fn decode_raft_message<R: Read>(reader: &mut R) -> std::io::Result<raftbare::Message> {
    todo!()
}
