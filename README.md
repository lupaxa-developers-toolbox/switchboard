<p align="center">
  <a href="https://github.com/lupaxa-developers-toolbox">
    <img src="https://raw.githubusercontent.com/the-lupaxa-project/brand-assets/master/logos/organisations/developers-toolbox/readme-logo.png" alt="Developers Toolbox" />
  </a>
</p>

<h1 align="center">Switchboard</h1>

Switchboard connects applications that use different protocols. It runs
locally and forwards between TCP, UDP, WebSocket, and HTTP endpoints. No
extra broker or database is required.

> **Note:** UDP does not guarantee delivery, ordering, or duplicate
> suppression. An HTTP `accepted` response means the request was admitted
> locally, not stored. A process restart drops anything still in flight.
> A successful local write is not proof that the remote application
> handled the message. A trace `send` means the local socket accepted the
> bytes.

The PyPI name is `lupaxa-switchboard`. The import path is
`lupaxa.switchboard`. The console script is `switchboard`. `lupaxa` is a
namespace package — there is no `lupaxa/__init__.py`.

## Install

```bash
pip install lupaxa-switchboard
```

Python 3.11 or newer is required.

## Example

```yaml
version: 1
bridges:
  - name: tcp-to-tcp
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

A gateway bridge can set `forward` and `reverse` pipelines. `forward` runs
on the way to the output. `reverse` runs on the way back. The endpoint
framing is still applied after those steps. A field transform is rejected
when the configuration is loaded unless `json-parse` comes first.

TCP endpoints need an explicit framing policy. `raw_stream` is valid only
when both TCP endpoints use it. A TCP stream and a UDP endpoint can share
a bridge when the TCP side is framed: each frame is one datagram, and each
datagram is written back with that framing.

## Protocols

WebSocket messages already have boundaries, so those endpoints do not take
a framing block. HTTP inputs answer with `accepted`, `forwarded`, or
`upstream`. `upstream` is only valid when the output is also HTTP.
`Authorization`, `Cookie`, and proxy credential headers are not forwarded
unless `http.forward_headers` names them.

TLS is available on TCP, WebSocket, and HTTP. Outbound certificate
verification stays on unless `tls.verify` is false, which logs a warning.
Outbound TCP and WebSocket connections can retry with `reconnect`. Payloads
that were already written are not sent again.

## Router

`mode: router` treats each framed message on its own. Rules choose named
outputs. With no rules, every output receives the message. One failed
destination does not stop the others. `delivery.success` is `all`, `any`,
or `best_effort`.

Transforms are declarative: JSON, fields, text, encoding, and `{{field}}`
templates. They do not run Python. A buffer limit drops or rejects messages
instead of storing them. Retries run only when the payload was not sent, or
when `delivery.retry.replay` is set. Hop count and a static cycle check stop
obvious forwarding loops.

## Profiles

A profile names a transport, a framing rule, a character encoding, and an
optional flat structure. An endpoint can use `profile` instead of repeating
those settings. The structure is decoded once when a message arrives and
encoded once when it leaves. Byte fields are base64 in the JSON form.
Fixed strings are padded with NUL bytes.

```bash
switchboard profile list
switchboard profile show device-protocol
switchboard profile validate profiles.yml
switchboard profile decode device-protocol sample.bin
switchboard profile encode device-protocol message.json
```

`profile decode` and `profile encode` do not start the gateway.

## Management

Set `management.enabled` to open a local control socket. Loopback TCP is
available and requires a token. Bridge listeners do not accept management
requests.

`switchboard status` uses that socket when the process is running. `bridge
start`, `bridge stop`, `bridge restart`, `connections`, `metrics`, `trace`,
and `reload` do too. Reload checks the file before it changes anything, and
leaves an unchanged bridge running. Replacing a listener is not atomic: the
port can be lost between the stop and the next bind.

`switchboard test` reports configuration, a local profile round trip, and a
TCP connect separately. It does not call that an end-to-end delivery.

Traces hide payloads unless `diagnostics.trace_payloads` is true. The
preview is a short hex string. Names in `diagnostics.secret_fields` are
replaced before that preview is recorded.

`switchboard tui` is optional. Install it with
`pip install lupaxa-switchboard[tui]`. It only uses the management socket.
If that socket closes, the screen says it is not connected and drops the
rows it was showing.

## Run

```bash
switchboard config validate switchboard.yml
switchboard serve --config switchboard.yml
```

`switchboard bridge list` describes the configuration file. `switchboard
status` reads the running process when management is enabled.

## Samples

The repository has three ready-to-run directories under `samples/`.
This is the smallest. Start the echo:

```bash
cd samples/line
python echo.py
```

From another terminal, in that same directory:

```bash
switchboard --config switchboard.yml serve
```

From a third terminal, still in `samples/line`:

```bash
python send.py
```

`samples/webhook` turns an HTTP POST into a TCP line. `samples/profile`
encodes and decodes a binary structure without listening.

## Documentation

The guide lives in `mkdocs/` and publishes to
<https://switchboard.thelupaxaproject.org/>.
It has install steps, gateway and router behaviour, a command
reference, and a walkthrough of each sample with commands you can paste.

```bash
pip install -r requirements.txt
mkdocs serve
```

<a href="https://github.com/the-lupaxa-project">
    <img src="https://raw.githubusercontent.com/the-lupaxa-project/brand-assets/master/logos/components/footer-for-child-orgs.svg" alt="The Lupaxa Project Footer" width="100%" />
</a>
