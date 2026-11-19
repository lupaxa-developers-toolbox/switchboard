# Usage

`switchboard serve` stays in the foreground until Ctrl-C. `--config`
selects a YAML file. `--log-level` overrides the file for that process.
`--quiet` hides the listening lines. `--verbose` turns logging up.
`--quiet` and `--verbose` together exit 2.

Pass `--config` before the subcommand or on `serve`. Both of these read
the same file:

```bash
switchboard --config switchboard.yml serve
switchboard serve --config switchboard.yml
```

## Gateway

A gateway bridge has one `input` and one `output`. TCP needs a framing
block. WebSocket and HTTP already have message boundaries, so they do
not take one. `raw_stream` is valid only when both TCP ends use it.

```yaml
bridges:
  - name: line
    mode: gateway
    input:
      protocol: tcp
      host: 127.0.0.1
      port: 9000
      framing:
        type: newline
    output:
      protocol: tcp
      host: 127.0.0.1
      port: 9001
      framing:
        type: newline
```

Newline framing removes the delimiter from the message and writes it
again on the way out. A length prefix is rejected before Switchboard
allocates a body of that size.

`forward` runs on the way to the output. `reverse` runs on the way
back. Framing is applied after those steps. A field transform is
rejected when the file is loaded unless `json-parse` comes first in
that pipeline.

One inbound connection uses one outbound connection. A refused output
on one bridge does not stop the others. UDP keeps a peer association
only so a reply can go back to that source. It still does not promise
delivery, order, or duplicate suppression.

## HTTP

An HTTP input answers with `accepted`, `forwarded`, or `upstream`.
`accepted` responds after the body is read and does not mean the
destination stored it. `forwarded` waits until the local write
finishes. `upstream` returns the upstream status and body, and the
output must be HTTP.

`Authorization`, `Cookie`, and proxy credential headers are dropped
unless `http.forward_headers` names them. A method outside
`http.allowed_methods` returns 405.

## Router

`mode: router` uses `outputs` instead of `output`. With no rules, every
output receives the message. Rules run in order. `stop_on_match` ends
the list. An unmatched message is dropped and counted unless
`on_unmatched` says otherwise.

`delivery.success` is `all`, `any`, or `best_effort`. One failed
destination does not cancel the others, except that `all` admits none
of them when a queue cannot fit the message. Queues are in memory.
`reject`, `drop_newest`, and `drop_oldest` are the overflow choices.

Retries run when the payload was not sent, or when
`delivery.retry.replay` is set. A POST is not replayed just because it
is a POST. The header `X-Switchboard-Hops` stops a forward loop at
`max_hops`. A static cycle in the file is rejected before anything
listens.

## Profiles

A profile names a transport, framing, a character encoding, and an
optional flat structure. An endpoint uses `profile` instead of
repeating those settings. The structure becomes JSON on the way in and
bytes on the way out, once each. Byte fields are standard base64.
Fixed strings are padded with NUL bytes.

```bash
switchboard --config switchboard.yml profile list
switchboard --config switchboard.yml profile encode device message.json > device.bin
switchboard --config switchboard.yml profile decode device device.bin
```

`profile encode` and `profile decode` do not start the gateway.
`profile encode` writes the raw bytes to standard output.

## TLS

TLS is available on TCP, WebSocket, and HTTP. A listener needs
`certfile` and `keyfile`. Outbound certificate checks stay on unless
`tls.verify` is false, which logs a warning and does not log the key.
UDP cannot use TLS.

## Management

Set `management.enabled` to open a control socket that is not one of
the bridges. On macOS and Linux the default is a Unix socket. Loopback
TCP is available and requires a token of at least 16 characters. The
token is not written to the log.

`status`, `bridge start`, `bridge stop`, `bridge restart`,
`connections`, `metrics`, `trace`, and `reload` use that socket when
it is open. `start`, `stop`, `restart`, and `reload` ask for
confirmation unless you pass `--yes`.

Reload reads the file used at startup, checks it, and leaves an
unchanged bridge running. Replacing a listener is not atomic: the port
can be lost between the stop and the next bind.

`switchboard test NAME` reports configuration, a local profile
round trip, and a TCP connect as separate checks. It does not call a
successful connect an end-to-end delivery.

Traces hide payloads unless `diagnostics.trace_payloads` is true. The
preview is a short hex string. Names listed in
`diagnostics.secret_fields` are replaced before that preview is stored.

## Text Interface

```bash
pip install "lupaxa-switchboard[tui]"
switchboard tui
```

The interface only calls the management socket. Keys `b`, `c`, `t`,
`p`, `l`, and `v` switch between bridges, connections, traces,
profiles, log lines, and validation. `q` quits. If the socket closes,
the screen says it is not connected and clears the rows.
