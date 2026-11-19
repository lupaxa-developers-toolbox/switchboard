# Switchboard

Switchboard connects programs that do not speak the same protocol. One
side can be a TCP line, a UDP datagram, a WebSocket, or an HTTP request.
Switchboard frames the bytes, can reshape them, and writes them to the
other side. Nothing else has to be installed.

A gateway keeps one conversation between one inbound connection and one
outbound connection. A router takes each framed message on its own and
can send it to more than one output. A profile describes framing and an
optional flat structure in YAML, so a custom layout does not need a new
adapter.

The package name is `lupaxa-switchboard`. The import path is
`lupaxa.switchboard`. The command is `switchboard`.

> **Note:** UDP does not guarantee delivery, ordering, or duplicate
> suppression. An HTTP `accepted` response means the request was admitted
> locally, not stored. A restart drops anything still in memory. A local
> write, and a trace `send`, mean the local socket accepted the bytes.

## What it Does

- Bridges TCP, UDP, WebSocket, and HTTP on one process
- Frames streams with a newline, a delimiter, a fixed length, or a length prefix
- Decodes a declared binary structure once on the way in and encodes it once on the way out
- Routes a message to named outputs, with rules and a hop limit
- Transforms JSON, text, and fields without running Python
- Exposes status, traces, and reload on a separate local socket

## Samples

The repository includes three directories you can run as they stand.
Each one has its own `switchboard.yml`.

| Sample              | What it Shows                         |
| ------------------- | ------------------------------------- |
| `samples/line`      | Forward one newline between TCP peers |
| `samples/webhook`   | Turn an HTTP POST into a TCP line     |
| `samples/profile`   | Encode and decode a binary structure  |

## The Guide

[Getting Started](getting-started.md) installs the package and runs
`samples/line`. [Usage](usage.md) covers bridges, the router, profiles,
and the text interface. [Reference](reference.md) lists commands, exit
codes, and configuration. [Examples](examples.md) walks through all
three samples with commands you can paste.
