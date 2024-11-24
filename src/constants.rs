use mio::Token;

pub const CLIENT_TOKEN_MIN: Token = Token(0);
pub const CLIENT_TOKEN_MAX: Token = Token(1_000_000 - 1);

pub const SERVER_TOKEN_MIN: Token = Token(CLIENT_TOKEN_MAX.0 + 1);
pub const SERVER_TOKEN_MAX: Token = Token(usize::MAX);

pub const EVENTS_CAPACITY: usize = 1024;
