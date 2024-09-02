use raftbare::{
    ClusterConfig, CommitPromise, LogEntries, LogEntry, LogIndex, LogPosition, MessageSeqNo,
    NodeId, Term,
};
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
pub enum SystemCommand {
    AddNode { node_id: NodeId, addr: SocketAddr },
}

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

    // TODO: Add CommandLog
    pub fn encode<W: Write>(&self, mut writer: W) -> std::io::Result<()> {
        match self {
            Self::JoinCall { seqno, from } => {
                writer.write_all(&[MESSAGE_TAG_CLUSTER_CALL])?;
                writer.write_all(&seqno.to_be_bytes())?;

                match from {
                    SocketAddr::V4(addr) => {
                        writer.write_all(&[ADDR_TAG_IPV4])?;
                        writer.write_all(&addr.ip().octets())?;
                        writer.write_all(&addr.port().to_be_bytes())?;
                    }
                    SocketAddr::V6(addr) => {
                        writer.write_all(&[ADDR_TAG_IPV6])?;
                        writer.write_all(&addr.ip().octets())?;
                        writer.write_all(&addr.port().to_be_bytes())?;
                    }
                }
            }
            Self::JoinReply {
                seqno,
                node_id,
                promise,
            } => {
                writer.write_all(&[MESSAGE_TAG_CLUSTER_REPLY])?;
                writer.write_all(&seqno.to_be_bytes())?;
                writer.write_all(&node_id.get().to_be_bytes())?;
                encode_commit_promise(&mut writer, *promise)?;
            }
            Self::RaftMessageCast { seqno, msg } => {
                writer.write_all(&[MESSAGE_TAG_RAFT_MESSAGE_CAST])?;
                writer.write_all(&seqno.to_be_bytes())?;
                encode_raft_message(&mut writer, msg)?;
            }
        }
        Ok(())
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

fn encode_raft_message_header<W: Write>(
    writer: &mut W,
    header: &raftbare::MessageHeader,
) -> std::io::Result<()> {
    writer.write_all(&header.from.get().to_be_bytes())?;
    writer.write_all(&header.term.get().to_be_bytes())?;
    writer.write_all(&header.seqno.get().to_be_bytes())?;
    Ok(())
}

fn decode_raft_message_header<R: Read>(reader: &mut R) -> std::io::Result<raftbare::MessageHeader> {
    let from = read_u64(reader)?;
    let term = read_u64(reader)?;
    let seqno = read_u64(reader)?;
    Ok(raftbare::MessageHeader {
        from: NodeId::new(from),
        term: Term::new(term),
        seqno: MessageSeqNo::new(seqno),
    })
}

fn encode_log_position<W: Write>(writer: &mut W, position: LogPosition) -> std::io::Result<()> {
    writer.write_all(&position.term.get().to_be_bytes())?;
    writer.write_all(&position.index.get().to_be_bytes())?;
    Ok(())
}

fn decode_log_position<R: Read>(reader: &mut R) -> std::io::Result<LogPosition> {
    let term = read_u64(reader)?;
    let index = read_u64(reader)?;
    Ok(LogPosition {
        term: Term::new(term),
        index: LogIndex::new(index),
    })
}

fn encode_log_entry<W: Write>(writer: &mut W, entry: LogEntry) -> std::io::Result<()> {
    match entry {
        LogEntry::Term(t) => {
            writer.write_all(&[0])?;
            writer.write_all(&t.get().to_be_bytes())?;
        }
        LogEntry::ClusterConfig(c) => {
            writer.write_all(&[1])?;
            encode_cluster_config(writer, &c)?;
        }
        LogEntry::Command => {
            todo!(); // TODO: mapping to actual command
        }
    }
    Ok(())
}

fn decode_log_entry<R: Read>(reader: &mut R) -> std::io::Result<LogEntry> {
    let tag = read_u8(reader)?;
    match tag {
        0 => {
            let term = read_u64(reader)?;
            Ok(LogEntry::Term(Term::new(term)))
        }
        1 => decode_cluster_config(reader).map(LogEntry::ClusterConfig),
        2 => {
            todo!()
        }
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            format!("Unknown log entry tag: {tag}"),
        )),
    }
}

fn encode_cluster_config<W: Write>(writer: &mut W, config: &ClusterConfig) -> std::io::Result<()> {
    writer.write_all(&(config.voters.len() as u16).to_be_bytes())?;
    for voter in &config.voters {
        writer.write_all(&voter.get().to_be_bytes())?;
    }

    writer.write_all(&(config.new_voters.len() as u16).to_be_bytes())?;
    for voter in &config.new_voters {
        writer.write_all(&voter.get().to_be_bytes())?;
    }

    writer.write_all(&(config.non_voters.len() as u16).to_be_bytes())?;
    for non_voter in &config.non_voters {
        writer.write_all(&non_voter.get().to_be_bytes())?;
    }

    Ok(())
}

fn decode_cluster_config<R: Read>(reader: &mut R) -> std::io::Result<ClusterConfig> {
    let voters_len = read_u16(reader)?;
    let voters = (0..voters_len)
        .map(|_| read_u64(reader).map(NodeId::new))
        .collect::<std::io::Result<_>>()?;

    let new_voters_len = read_u16(reader)?;
    let new_voters = (0..new_voters_len)
        .map(|_| read_u64(reader).map(NodeId::new))
        .collect::<std::io::Result<_>>()?;

    let non_voters_len = read_u16(reader)?;
    let non_voters = (0..non_voters_len)
        .map(|_| read_u64(reader).map(NodeId::new))
        .collect::<std::io::Result<_>>()?;

    Ok(ClusterConfig {
        voters,
        new_voters,
        non_voters,
    })
}

fn encode_raft_message<W: Write>(writer: &mut W, msg: &raftbare::Message) -> std::io::Result<()> {
    match msg {
        raftbare::Message::RequestVoteCall {
            header,
            last_position,
        } => {
            writer.write_all(&[0])?;
            encode_raft_message_header(writer, header)?;
            encode_log_position(writer, *last_position)?;
        }
        raftbare::Message::RequestVoteReply {
            header,
            vote_granted,
        } => {
            writer.write_all(&[1])?;
            encode_raft_message_header(writer, header)?;
            writer.write_all(&[*vote_granted as u8])?;
        }
        raftbare::Message::AppendEntriesCall {
            header,
            commit_index,
            entries,
        } => {
            writer.write_all(&[2])?;
            encode_raft_message_header(writer, header)?;
            writer.write_all(&commit_index.get().to_be_bytes())?;
            encode_log_position(writer, entries.prev_position())?;

            // TODO: limit the number of entries to not exceed the maximum message size
            writer.write_all(&(entries.len() as u32).to_be_bytes())?;
            for entry in entries.iter() {
                encode_log_entry(writer, entry)?;
            }
        }
        raftbare::Message::AppendEntriesReply {
            header,
            last_position,
        } => {
            writer.write_all(&[3])?;
            encode_raft_message_header(writer, header)?;
            encode_log_position(writer, *last_position)?;
        }
    }
    Ok(())
}

fn decode_raft_message<R: Read>(reader: &mut R) -> std::io::Result<raftbare::Message> {
    let tag = read_u8(reader)?;
    match tag {
        0 => todo!(),
        1 => todo!(),
        2 => {
            let header = decode_raft_message_header(reader)?;
            let commit_index = LogIndex::new(read_u64(reader)?);
            let prev_position = decode_log_position(reader)?;
            let entry_count = read_u32(reader)?;
            let mut entries = LogEntries::new(prev_position);
            for _ in 0..entry_count {
                entries.push(decode_log_entry(reader)?);
            }
            Ok(raftbare::Message::AppendEntriesCall {
                header,
                commit_index,
                entries,
            })
        }
        3 => todo!(),
        _ => Err(Error::new(
            ErrorKind::InvalidData,
            format!("Unknown raft message tag: {tag}"),
        )),
    }
}
