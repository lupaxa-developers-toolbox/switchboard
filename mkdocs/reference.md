# Reference

`--json` prints a machine-readable result where the command has one.
`--quiet` hides informational text. `--verbose` raises the log level.
Unknown flags exit 2 and do not print a traceback.

## Commands

```text
switchboard
├── serve
├── status
├── reload
├── bridge
│   ├── list
│   ├── show
│   ├── start
│   ├── stop
│   └── restart
├── connections
│   ├── list
│   └── show
├── profile
│   ├── list
│   ├── show
│   ├── validate
│   ├── decode
│   └── encode
├── test
├── trace
├── metrics
├── config
│   ├── validate
│   ├── show
│   └── path
├── doctor
├── tui
└── version
```

`serve` listens until interrupted. `bridge list` and `config show`
read the file. `status` reads the running process when management is
enabled and the socket accepts a connection. Otherwise it describes
the file and says the source is `configuration`.

`bridge start`, `bridge stop`, `bridge restart`, and `reload` change
a running process. They require `--yes` when the terminal is not
interactive. A refused confirmation exits 1.

`trace NAME` follows diagnostic events. `--once` prints the current
buffer and exits. `--payloads` prints a hex preview only when one was
recorded. `metrics` prints counters from the running gateway.

`test NAME` does not open the data path end to end. `doctor` checks
Python, the configuration, whether management is reachable, and
whether Textual is installed. `tui` needs the `tui` extra and an
enabled management socket.

`profile encode` writes bytes to standard output. `profile decode`
writes text. Neither command starts listeners.

## Exit Codes

| Code | Meaning                                                      |
| ---- | ------------------------------------------------------------ |
| 0    | The command finished                                         |
| 1    | The gateway or the operation failed, or you cancelled it     |
| 2    | The configuration or the command line was rejected           |

## Configuration

The file is YAML. Only `version: 1` is valid. Built-in defaults sit
under a user file, then the file you pass with `--config`, then
`--log-level`. Lists such as `bridges` are replaced. Mappings are
merged.

A bridge name matches `^[A-Za-z0-9][A-Za-z0-9_-]*$` and is 1 to 128
characters. Two enabled bridges cannot listen on the same protocol
family, host, and port unless the port is 0. Port 0 asks the system
for a free port. The input and the output of one bridge cannot be the
same listener.

```yaml
version: 1
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 9000
      framing:
        type: newline
        max_frame_bytes: 65536
    output:
      protocol: tcp
      host: 127.0.0.1
      port: 9001
      framing:
        type: newline
```

Framing types are `newline`, `delimiter`, `fixed_length`,
`length_prefix`, and `raw_stream`. `max_frame_bytes` cannot exceed
16777216. A UDP datagram cannot exceed 65507 bytes. An oversized
datagram follows `udp.overflow`: `drop` or `reject`. Both discard it.
`reject` also logs a warning without the payload.

Router bridges set `outputs`, `rules`, and `transforms`. Gateway
bridges set `output`, and may set `forward` and `reverse`. Mixing
those sets is a validation error. A profile `extends` key is rejected.

## Limits

| Setting                         | Default                         |
| ------------------------------- | ------------------------------- |
| `server.max_connections`        | 1000                            |
| `limits.max_connections`        | 100                             |
| `limits.idle_timeout_seconds`   | 300                             |
| `limits.connect_timeout_seconds`| 10                              |
| `limits.write_timeout_seconds`  | 30                              |
| `limits.frame_timeout_seconds`  | 30                              |
| `limits.read_timeout_seconds`   | disabled                        |
| `max_hops`                      | 8                               |

`read_timeout_seconds` applies only while bytes are expected.
`frame_timeout_seconds` applies while a partial frame is buffered.
Idle time closes a quiet session. `disconnect.on_peer_close` is
`close_both` or `half_close`. `half_close` applies to TCP to TCP.

## Logging

`logging.format` is `text` or `json`. The JSON object has `level`,
`logger`, `message`, and sometimes `error`. Payloads, headers, tokens,
and private keys are not logged.
