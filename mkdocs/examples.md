# Examples

These three directories are in `samples/`. Each one is self-contained.
Start the helper process, then Switchboard, then the client script.
Stop each process with Ctrl-C.

The ports are local and fixed. Do not run two samples that bind the
same port at the same time.

## Line

`samples/line` copies a newline-framed TCP message to another TCP
port and copies the reply back.

```yaml
--8<-- "samples/line/switchboard.yml"
```

```python
--8<-- "samples/line/send.py"
```

```bash
cd samples/line
python echo.py
```

From another terminal:

```bash
cd samples/line
switchboard --config switchboard.yml serve
```

From a third terminal:

```bash
cd samples/line
python send.py
```

`echo.py` prints `listening`, then prints each line it receives and
sends that line back. `send.py` prints `hello`. The bytes on the wire
include the newline. The message Switchboard forwards is the text
before that newline, and the output framing adds the newline again.

`switchboard --config switchboard.yml config validate` checks the file
without listening.

## Webhook

`samples/webhook` accepts a POST on `/events` and writes the body to a
TCP line. The HTTP response is `forwarded`: Switchboard answers after
the line has been written, and that answer is not the TCP peer's
reply.

```yaml
--8<-- "samples/webhook/switchboard.yml"
```

```python
--8<-- "samples/webhook/post.py"
```

```bash
cd samples/webhook
python echo.py
```

From another terminal:

```bash
cd samples/webhook
switchboard --config switchboard.yml serve
```

From a third terminal:

```bash
cd samples/webhook
python post.py
```

`post.py` prints `202`. The echo prints the JSON body and a newline.
A response of 202 means the local write finished. It does not mean
another system stored the event. If the echo is not listening, the
POST fails and Switchboard returns 502.

The same call without the script:

```bash
curl --silent --show-error --output /dev/null --write-out "%{http_code}\n" \
  --request POST --data '{"event":"ping"}' \
  http://127.0.0.1:18081/events
```

## Profile

`samples/profile` describes a three-field structure and does not need
a running gateway. `kind` is one byte, `note` is four characters, and
`rest` is whatever remains, shown as base64.

```yaml
--8<-- "samples/profile/switchboard.yml"
```

```json
--8<-- "samples/profile/message.json"
```

```python
--8<-- "samples/profile/roundtrip.py"
```

```bash
cd samples/profile
python roundtrip.py
switchboard --config switchboard.yml profile encode device message.json > device.bin
switchboard --config switchboard.yml profile decode device device.bin
```

`roundtrip.py` prints JSON with the decoded object and the encoded
length. `profile encode` writes the raw bytes. `profile decode` prints
the same JSON object. Neither command opens a socket.
