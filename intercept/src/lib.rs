//! An `LD_PRELOAD` shim that logs the order of
//! cross-process message traffic without touching the workload or the
//! existing recorder/replay/fault pipeline.
//!
//! Shadows `connect`/`send`/`recv`/`write`/`read` (see `MessageEvent` doc
//! for why both pairs are needed) via `dlsym(RTLD_NEXT, ...)`, so the real
//! libc behavior is unchanged — this only observes. Each successful call on
//! a socket fd appends one JSON line to `ANG_MESSAGE_LOG`, `flock`-guarded
//! the same way `recorder::append_line` serializes concurrent boundary-log
//! writers — duplicated here rather than depending on
//! `recorder`, since this crate builds as a `cdylib` loaded into arbitrary
//! target processes, not linked into ANG's own binaries.

use std::ffi::c_void;
use std::fs::OpenOptions;
use std::io::Write as _;
use std::os::raw::c_int;
use std::os::unix::io::AsRawFd;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// One logged call. `direction` is `"send"` or `"recv"` regardless of
/// whether the underlying syscall was `send`/`recv` or plain `write`/`read`
/// on a socket fd — Rust's `std::net::TcpStream` uses `write`/`read`, not
/// `send`/`recv`, so a shim that only wrapped `send`/`recv` would silently
/// log nothing for a Rust workload.
///
/// `job_id` is read from `SLURM_JOB_ID` the same way
/// `recorder::Event::new` already does for Run events — `None` if the shim
/// isn't running under Slurm.
///
/// `step_id`/`task_id`/`node` are read from `SLURM_STEP_ID`,
/// `SLURM_PROCID`, and `SLURMD_NODENAME` — the exact triple
/// `cli/src/job_event.rs`'s `task-started`/`task-exited` hooks already read
/// for `TaskStarted`/`TaskExited`'s own `(step_id, task_id, node)`
/// correlation key. `None` if the shim isn't running under a Slurm task
/// (e.g. a local test process with no Slurm context at all).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageEvent {
    pub pid: u32,
    pub fd: i32,
    pub direction: String,
    pub peer: Option<String>,
    pub bytes: i64,
    pub timestamp_ms: u128,
    pub job_id: Option<String>,
    pub step_id: Option<String>,
    pub task_id: Option<String>,
    pub node: Option<String>,
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis()
}

/// Same correlation source `recorder::Event::new` already reads for Run
/// events; `None` if the shim isn't running under Slurm.
fn slurm_job_id() -> Option<String> {
    std::env::var("SLURM_JOB_ID").ok()
}

/// The same `(step_id, task_id, node)` triple
/// `cli/src/job_event.rs`'s `task-started`/`task-exited` hooks already read
/// for `TaskStarted`/`TaskExited`'s correlation key. `None` per field if
/// the shim isn't running under a Slurm task.
fn slurm_task_attribution() -> (Option<String>, Option<String>, Option<String>) {
    (
        std::env::var("SLURM_STEP_ID").ok(),
        std::env::var("SLURM_PROCID").ok(),
        std::env::var("SLURMD_NODENAME").ok(),
    )
}

fn log_path() -> Option<std::path::PathBuf> {
    std::env::var_os("ANG_MESSAGE_LOG").map(std::path::PathBuf::from)
}

fn is_socket(fd: c_int) -> bool {
    unsafe {
        let mut st: libc::stat = std::mem::zeroed();
        libc::fstat(fd, &mut st) == 0 && (st.st_mode & libc::S_IFMT) == libc::S_IFSOCK
    }
}

fn peer_string(fd: c_int) -> Option<String> {
    unsafe {
        let mut addr: libc::sockaddr_storage = std::mem::zeroed();
        let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
        if libc::getpeername(fd, &mut addr as *mut _ as *mut libc::sockaddr, &mut len) != 0 {
            return None;
        }
        if addr.ss_family as c_int != libc::AF_INET {
            return None;
        }
        let addr_in = &*(&addr as *const _ as *const libc::sockaddr_in);
        let ip = std::net::Ipv4Addr::from(u32::from_be(addr_in.sin_addr.s_addr));
        let port = u16::from_be(addr_in.sin_port);
        Some(format!("{ip}:{port}"))
    }
}

/// Appends one event, `flock`-guarded (same convention as
/// `recorder::append_line`). Silently drops the event if `ANG_MESSAGE_LOG`
/// isn't set or the file can't be opened — a shim that can crash the
/// process it's injected into defeats the purpose of only observing.
fn append(event: &MessageEvent) {
    let Some(path) = log_path() else { return };
    let Ok(mut file) = OpenOptions::new().create(true).append(true).open(&path) else {
        return;
    };
    let Ok(line) = serde_json::to_string(event) else {
        return;
    };
    let fd = file.as_raw_fd();
    unsafe {
        if libc::flock(fd, libc::LOCK_EX) != 0 {
            return;
        }
    }
    let _ = writeln!(file, "{line}");
    unsafe {
        libc::flock(fd, libc::LOCK_UN);
    }
}

fn log_call(fd: c_int, direction: &str, bytes: isize) {
    if bytes <= 0 || !is_socket(fd) {
        return;
    }
    let (step_id, task_id, node) = slurm_task_attribution();
    append(&MessageEvent {
        pid: std::process::id(),
        fd,
        direction: direction.to_string(),
        peer: peer_string(fd),
        bytes: bytes as i64,
        timestamp_ms: now_ms(),
        job_id: slurm_job_id(),
        step_id,
        task_id,
        node,
    });
}

macro_rules! resolve_real {
    ($name:literal, $ty:ty) => {{
        static CELL: OnceLock<usize> = OnceLock::new();
        let ptr = *CELL.get_or_init(|| unsafe {
            libc::dlsym(libc::RTLD_NEXT, concat!($name, "\0").as_ptr() as *const _) as usize
        });
        assert!(
            ptr != 0,
            concat!("dlsym(RTLD_NEXT, \"", $name, "\") returned null")
        );
        unsafe { std::mem::transmute::<usize, $ty>(ptr) }
    }};
}

type ConnectFn = unsafe extern "C" fn(c_int, *const libc::sockaddr, libc::socklen_t) -> c_int;
type SendFn = unsafe extern "C" fn(c_int, *const c_void, usize, c_int) -> isize;
type RecvFn = unsafe extern "C" fn(c_int, *mut c_void, usize, c_int) -> isize;
type WriteFn = unsafe extern "C" fn(c_int, *const c_void, usize) -> isize;
type ReadFn = unsafe extern "C" fn(c_int, *mut c_void, usize) -> isize;

fn real_connect() -> ConnectFn {
    resolve_real!("connect", ConnectFn)
}
fn real_send() -> SendFn {
    resolve_real!("send", SendFn)
}
fn real_recv() -> RecvFn {
    resolve_real!("recv", RecvFn)
}
fn real_write() -> WriteFn {
    resolve_real!("write", WriteFn)
}
fn real_read() -> ReadFn {
    resolve_real!("read", ReadFn)
}

/// # Safety
/// Same contract as libc's `connect(2)`; this only wraps it.
#[no_mangle]
pub unsafe extern "C" fn connect(
    sockfd: c_int,
    addr: *const libc::sockaddr,
    addrlen: libc::socklen_t,
) -> c_int {
    let ret = (real_connect())(sockfd, addr, addrlen);
    if ret == 0 {
        let (step_id, task_id, node) = slurm_task_attribution();
        append(&MessageEvent {
            pid: std::process::id(),
            fd: sockfd,
            direction: "connect".to_string(),
            peer: peer_string(sockfd),
            bytes: 0,
            timestamp_ms: now_ms(),
            job_id: slurm_job_id(),
            step_id,
            task_id,
            node,
        });
    }
    ret
}

/// # Safety
/// Same contract as libc's `send(2)`; this only wraps it.
#[no_mangle]
pub unsafe extern "C" fn send(
    sockfd: c_int,
    buf: *const c_void,
    len: usize,
    flags: c_int,
) -> isize {
    let ret = (real_send())(sockfd, buf, len, flags);
    log_call(sockfd, "send", ret);
    ret
}

/// # Safety
/// Same contract as libc's `recv(2)`; this only wraps it.
#[no_mangle]
pub unsafe extern "C" fn recv(sockfd: c_int, buf: *mut c_void, len: usize, flags: c_int) -> isize {
    let ret = (real_recv())(sockfd, buf, len, flags);
    log_call(sockfd, "recv", ret);
    ret
}

/// # Safety
/// Same contract as libc's `write(2)`; this only wraps it. Logged as a
/// `"send"` event when `fd` is a socket, a no-op passthrough otherwise
/// (including writes this crate's own `append` makes to the log file).
#[no_mangle]
pub unsafe extern "C" fn write(fd: c_int, buf: *const c_void, count: usize) -> isize {
    let ret = (real_write())(fd, buf, count);
    log_call(fd, "send", ret);
    ret
}

/// # Safety
/// Same contract as libc's `read(2)`; this only wraps it. Logged as a
/// `"recv"` event when `fd` is a socket, a no-op passthrough otherwise.
#[no_mangle]
pub unsafe extern "C" fn read(fd: c_int, buf: *mut c_void, count: usize) -> isize {
    let ret = (real_read())(fd, buf, count);
    log_call(fd, "recv", ret);
    ret
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{TcpListener, TcpStream};

    #[test]
    fn message_event_round_trips_through_json() {
        let event = MessageEvent {
            pid: 123,
            fd: 4,
            direction: "send".to_string(),
            peer: Some("127.0.0.1:9000".to_string()),
            bytes: 8,
            timestamp_ms: 42,
            job_id: Some("77".to_string()),
            step_id: Some("0".to_string()),
            task_id: Some("1".to_string()),
            node: Some("n1".to_string()),
        };
        let line = serde_json::to_string(&event).unwrap();
        let back: MessageEvent = serde_json::from_str(&line).unwrap();
        assert_eq!(back.pid, 123);
        assert_eq!(back.direction, "send");
        assert_eq!(back.peer.as_deref(), Some("127.0.0.1:9000"));
        assert_eq!(back.job_id.as_deref(), Some("77"));
        assert_eq!(back.step_id.as_deref(), Some("0"));
        assert_eq!(back.task_id.as_deref(), Some("1"));
        assert_eq!(back.node.as_deref(), Some("n1"));
    }

    #[test]
    fn is_socket_distinguishes_sockets_from_regular_files() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        assert!(is_socket(client.as_raw_fd()));

        let file = tempfile_for_test();
        assert!(!is_socket(file.as_raw_fd()));
    }

    #[test]
    fn peer_string_reports_the_connected_socket_address() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let client = TcpStream::connect(addr).unwrap();
        let peer = peer_string(client.as_raw_fd());
        assert_eq!(peer, Some(addr.to_string()));
    }

    #[test]
    fn log_call_skips_zero_and_negative_byte_counts() {
        // A shim that logged failed/empty calls would produce noise no
        // downstream merge logic could distinguish from real messages —
        // log_call must only fire on ret > 0. No log path is exercised here
        // (ANG_MESSAGE_LOG unset), so this just confirms it doesn't panic.
        log_call(0, "send", 0);
        log_call(0, "send", -1);
    }

    fn tempfile_for_test() -> std::fs::File {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("intercept-test-{}", std::process::id()));
        std::fs::File::create(&path).unwrap()
    }
}
