# Getting Started

Switchboard needs Python 3.11 or newer.

## Install

```bash
pip install lupaxa-switchboard
```

The text interface is separate:

```bash
pip install "lupaxa-switchboard[tui]"
```

A normal install does not include that extra. `switchboard tui` without
it prints an install hint and exits 1.

Check the command:

```bash
switchboard --version
switchboard doctor
```

`switchboard doctor` loads the configuration. When management is
disabled, or nothing is listening on the control socket, that check is
skipped. A broken configuration exits 2.

## Run a Sample

From a clone of this repository, start the program that will receive
the line:

```bash
cd samples/line
python echo.py
```

`echo.py` prints `listening`, then repeats each line it is sent. In a
second terminal, from the same directory:

```bash
switchboard --config switchboard.yml serve
```

In a third terminal, still in `samples/line`:

```bash
python send.py
```

`send.py` writes `hello` to port 19000. Switchboard frames that as one
message and opens a connection to the echo on port 19001. The script
prints `hello`. Stop the gateway and the echo with Ctrl-C.

The ports in the sample are fixed. Run one sample at a time, or change
both the configuration and the scripts together. Listeners bind to
`127.0.0.1`.

## Check the Configuration

```bash
switchboard --config switchboard.yml config validate
switchboard --config switchboard.yml bridge list
```

`config validate` exits 0 when the file is usable. `bridge list`
describes that file. It does not require a running gateway.

## Read the Docs Locally

The guide in this site is the `mkdocs/` directory.

```bash
pip install -r requirements.txt
mkdocs serve
```

Open the URL MkDocs prints. The published site is
<https://switchboard.thelupaxaproject.org/>.
