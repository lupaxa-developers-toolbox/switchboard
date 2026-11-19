"""Command-line entry point."""

from __future__ import annotations

import asyncio
import contextlib
import json
import signal
import sys
import time
from pathlib import Path
from typing import Annotated

import typer
import yaml
from rich.console import Console
from rich.table import Table
from typer._click.exceptions import ClickException

from lupaxa.switchboard.config import (
    default_config_path,
    endpoint_label,
    load_config,
    load_profiles,
)
from lupaxa.switchboard.exceptions import SwitchboardError, UsageError
from lupaxa.switchboard.gateway import Gateway
from lupaxa.switchboard.log import configure_logging
from lupaxa.switchboard.manage import call, management_endpoint
from lupaxa.switchboard.profile import decode_profile, encode_profile
from lupaxa.switchboard.version import __version__

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Connect applications that use different protocols.",
)
config_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Inspect configuration.",
)
bridge_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Show configured bridges.",
)
profile_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Inspect and convert protocol profiles.",
)
connections_app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="Show live connections.",
)
app.add_typer(config_app, name="config")
app.add_typer(bridge_app, name="bridge")
app.add_typer(profile_app, name="profile")
app.add_typer(connections_app, name="connections")

_LEVELS = ["debug", "info", "warning", "error"]


def _show_version(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit


@app.callback()
def _root(
    ctx: typer.Context,
    _version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the package version and exit.",
            callback=_show_version,
            is_eager=True,
        ),
    ] = False,
    config: Annotated[
        Path | None,
        typer.Option("--config", help="Explicit configuration file."),
    ] = None,
    quiet: Annotated[bool, typer.Option("--quiet", help="Hide informational text.")] = False,
    verbose: Annotated[bool, typer.Option("--verbose", help="Log gateway detail.")] = False,
) -> None:
    """Connect applications that use different protocols."""
    if quiet and verbose:
        raise UsageError("--quiet and --verbose cannot be used together.")
    ctx.obj = {"config": config, "quiet": quiet, "verbose": verbose}


@app.command()
def serve(
    ctx: typer.Context,
    config_path: Annotated[
        Path | None,
        typer.Option("--config", help="Explicit configuration file."),
    ] = None,
    log_level: Annotated[
        str | None,
        typer.Option("--log-level", help="Override the configured log level."),
    ] = None,
) -> None:
    """Listen for TCP and UDP bridges until interrupted."""
    if log_level is not None and log_level not in _LEVELS:
        raise UsageError("log level must be debug, info, warning, or error.")
    if config_path is not None:
        if not isinstance(ctx.obj, dict):
            ctx.obj = {}
        ctx.obj["config"] = config_path
    config = _load(ctx, log_level)
    configure_logging(_level(ctx, config.logging.level, log_level), config.logging.format)
    try:
        asyncio.run(
            _serve(config, quiet=bool(_obj(ctx).get("quiet")), source=_source(ctx, config_path))
        )
    except KeyboardInterrupt:
        return


@app.command()
def version() -> None:
    """Show the package version."""
    typer.echo(__version__)


@app.command()
def status(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Show gateway status. A running management socket is preferred."""
    config = _load(ctx, None)
    payload = _live(config, "status")
    if not isinstance(payload, dict):
        rows = [_bridge_row(bridge) for bridge in config.bridges]
        payload = {"source": "configuration", "name": config.switchboard.name, "bridges": rows}
    if as_json:
        typer.echo(json.dumps(payload))
        return
    bridges = payload.get("bridges")
    if isinstance(bridges, list):
        _print_bridges([row for row in bridges if isinstance(row, dict)])
        return
    typer.echo("No bridges are configured.")


@config_app.command("validate")
def config_validate(
    ctx: typer.Context,
    path: Annotated[Path | None, typer.Argument(help="Configuration file.")] = None,
) -> None:
    """Read a configuration file and report whether it is valid."""
    _load(ctx, None, path)
    if not _obj(ctx).get("quiet"):
        typer.echo("Configuration is valid.")


@config_app.command("show")
def config_show(
    ctx: typer.Context,
    path: Annotated[Path | None, typer.Argument(help="Configuration file.")] = None,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Print the resolved configuration."""
    config = _load(ctx, None, path)
    data = config.model_dump(mode="json")
    if as_json:
        typer.echo(json.dumps(data))
        return
    typer.echo(yaml.safe_dump(data, sort_keys=False).rstrip())


@config_app.command("path")
def config_path(ctx: typer.Context) -> None:
    """Print the configuration file this process would read."""
    override = _obj(ctx).get("config")
    if isinstance(override, Path):
        typer.echo(str(override))
        return
    typer.echo(str(default_config_path()))


@bridge_app.command("list")
def bridge_list(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """List bridges in the configuration."""
    config = _load(ctx, None)
    rows = [_bridge_row(bridge) for bridge in config.bridges]
    if as_json:
        typer.echo(json.dumps({"source": "configuration", "bridges": rows}))
        return
    _print_bridges(rows)


@bridge_app.command("show")
def bridge_show(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Show one configured bridge."""
    config = _load(ctx, None)
    for bridge in config.bridges:
        if bridge.name == name:
            row = _bridge_row(bridge)
            if as_json:
                typer.echo(json.dumps(row))
                return
            typer.echo(f"{row['name']}  {row['mode']}  {row['input']} -> {row['output']}")
            return
    raise SwitchboardError(f"Bridge is not configured: {name}", exit_code=1)


@profile_app.command("list")
def profile_list(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """List protocol profiles in the configuration."""
    config = _load(ctx, None)
    rows = [_profile_row(name, profile) for name, profile in config.profiles.items()]
    if as_json:
        typer.echo(json.dumps({"source": "configuration", "profiles": rows}))
        return
    table = Table(title="Protocol Profiles")
    table.add_column("Name")
    table.add_column("Transport")
    table.add_column("Framing")
    table.add_column("Structure")
    for row in rows:
        table.add_row(
            str(row["name"]),
            str(row["transport"]),
            str(row["framing"]),
            str(row["structure"]),
        )
    Console().print(table)


@profile_app.command("show")
def profile_show(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Profile name.")],
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Show one protocol profile."""
    config = _load(ctx, None)
    profile = config.profiles.get(name)
    if profile is None:
        raise SwitchboardError(f"Profile is not configured: {name}", exit_code=1)
    data = profile.model_dump(mode="json")
    if as_json:
        typer.echo(json.dumps({"name": name, **data}))
        return
    typer.echo(yaml.safe_dump({name: data}, sort_keys=False).rstrip())


@profile_app.command("validate")
def profile_validate(
    ctx: typer.Context,
    path: Annotated[Path, typer.Argument(help="Profile or configuration file.")],
) -> None:
    """Read a profile file and report whether it is valid."""
    load_profiles(path)
    if not _obj(ctx).get("quiet"):
        typer.echo("Profile file is valid.")


@profile_app.command("decode")
def profile_decode(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Profile name.")],
    path: Annotated[Path, typer.Argument(help="Payload file.")],
) -> None:
    """Decode one payload without starting the gateway."""
    profile = _profile(_load(ctx, None), name)
    if not path.is_file():
        raise UsageError(f"Payload file not found: {path}")
    typer.echo(decode_profile(profile, path.read_bytes()))


@profile_app.command("encode")
def profile_encode(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Profile name.")],
    path: Annotated[Path, typer.Argument(help="JSON message file.")],
) -> None:
    """Encode one JSON message without starting the gateway."""
    profile = _profile(_load(ctx, None), name)
    if not path.is_file():
        raise UsageError(f"Message file not found: {path}")
    sys.stdout.buffer.write(encode_profile(profile, path.read_text(encoding="utf-8")))


@bridge_app.command("start")
def bridge_start(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation prompt.")] = False,
) -> None:
    """Start one bridge on the running gateway."""
    _confirm(yes, f"Start {name}")
    _require(_load(ctx, None), "bridge.start", {"name": name})
    if not _obj(ctx).get("quiet"):
        typer.echo(f"{name} started.")


@bridge_app.command("stop")
def bridge_stop(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation prompt.")] = False,
) -> None:
    """Stop one bridge. Other bridges keep running."""
    _confirm(yes, f"Stop {name}")
    _require(_load(ctx, None), "bridge.stop", {"name": name})
    if not _obj(ctx).get("quiet"):
        typer.echo(f"{name} stopped.")


@bridge_app.command("restart")
def bridge_restart(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    yes: Annotated[bool, typer.Option("--yes", help="Skip the confirmation prompt.")] = False,
) -> None:
    """Restart one bridge from the current configuration."""
    _confirm(yes, f"Restart {name}")
    _require(_load(ctx, None), "bridge.restart", {"name": name})
    if not _obj(ctx).get("quiet"):
        typer.echo(f"{name} restarted.")


@connections_app.command("list")
def connections_list(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """List connections on the running gateway."""
    payload = _require(_load(ctx, None), "connections.list")
    if as_json:
        typer.echo(json.dumps(payload))
        return
    rows = payload.get("connections") if isinstance(payload, dict) else None
    table = Table(title="Connections")
    table.add_column("Id")
    table.add_column("Bridge")
    table.add_column("State")
    table.add_column("Inbound")
    if isinstance(rows, list):
        for row in rows:
            if isinstance(row, dict):
                table.add_row(
                    str(row.get("id")),
                    str(row.get("bridge")),
                    str(row.get("state")),
                    str(row.get("inbound")),
                )
    Console().print(table)


@connections_app.command("show")
def connections_show(
    ctx: typer.Context,
    identity: Annotated[str, typer.Argument(help="Connection id.")],
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Show one live connection."""
    payload = _require(_load(ctx, None), "connections.show", {"id": identity})
    rendered = json.dumps(payload) if as_json else yaml.safe_dump(payload, sort_keys=False)
    typer.echo(rendered.rstrip())


@app.command()
def reload(
    ctx: typer.Context,
    yes: Annotated[bool, typer.Option("--yes", help="Skip confirmation.")] = False,
) -> None:
    """Reload the startup configuration. Unchanged bridges stay in place."""
    _confirm(yes, "Reload the configuration")
    payload = _require(_load(ctx, None), "reload")
    typer.echo(json.dumps(payload))


@app.command()
def metrics(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Show counters from the running gateway."""
    payload = _require(_load(ctx, None), "metrics")
    rendered = json.dumps(payload) if as_json else yaml.safe_dump(payload, sort_keys=False)
    typer.echo(rendered.rstrip())


@app.command()
def trace(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    once: Annotated[bool, typer.Option("--once", help="Print current events and exit.")] = False,
    payloads: Annotated[
        bool,
        typer.Option("--payloads", help="Show a recorded hex preview."),
    ] = False,
) -> None:
    """Follow diagnostic events. A send is local transmission, not remote handling."""
    config = _load(ctx, None)
    seen = 0
    while True:
        payload = _require(config, "events", {"bridge": name, "since": seen})
        events = payload.get("events") if isinstance(payload, dict) else None
        if isinstance(events, list):
            for event in events:
                if isinstance(event, dict):
                    _print_event(event, payloads=payloads)
                    seq = event.get("seq")
                    if isinstance(seq, int):
                        seen = max(seen, seq)
        if once:
            return
        time.sleep(0.5)


@app.command()
def test(
    ctx: typer.Context,
    name: Annotated[str, typer.Argument(help="Bridge name.")],
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Check configuration, a local profile round trip, and TCP connectivity."""
    from lupaxa.switchboard.config import BridgeSpec

    config = _load(ctx, None)
    bridge = next((item for item in config.bridges if item.name == name), None)
    if not isinstance(bridge, BridgeSpec):
        raise SwitchboardError(f"Bridge is not configured: {name}", exit_code=1)
    checks = [
        {"name": "configuration", "status": "pass", "detail": "bridge is configured"},
        _named_check("profile", *_profile_check(bridge)),
        _named_check("connectivity", *_connect_check(bridge)),
        {
            "name": "end_to_end",
            "status": "not_run",
            "detail": "configuration and a TCP connect are not an end-to-end delivery",
        },
    ]
    failed = any(item["status"] == "fail" for item in checks)
    payload = {"bridge": name, "ok": not failed, "checks": checks}
    if as_json:
        typer.echo(json.dumps(payload))
    else:
        for item in checks:
            typer.echo(f"{item['name']}: {item['status']}  {item['detail']}")
    if failed:
        raise SwitchboardError("Bridge test failed.", exit_code=1)


@app.command()
def doctor(
    ctx: typer.Context,
    as_json: Annotated[bool, typer.Option("--json", help="Print JSON.")] = False,
) -> None:
    """Report the local setup without changing it."""
    import importlib.util
    import platform

    checks = [{"name": "python", "status": "pass", "detail": platform.python_version()}]
    try:
        config = _load(ctx, None)
    except SwitchboardError as exc:
        checks.append({"name": "configuration", "status": "fail", "detail": str(exc)})
        _emit_checks(checks, as_json=as_json)
        raise
    checks.append({"name": "configuration", "status": "pass", "detail": "configuration loaded"})
    endpoint = management_endpoint(config)
    if endpoint is None:
        checks.append({"name": "management", "status": "skip", "detail": "management is disabled"})
    else:
        try:
            call(endpoint, "health")
            checks.append({"name": "management", "status": "pass", "detail": "socket is open"})
        except SwitchboardError:
            checks.append({"name": "management", "status": "skip", "detail": "not running"})
    textual = importlib.util.find_spec("textual") is not None
    checks.append(
        {
            "name": "tui",
            "status": "pass" if textual else "skip",
            "detail": "textual is installed" if textual else "pip install lupaxa-switchboard[tui]",
        }
    )
    _emit_checks(checks, as_json=as_json)


@app.command()
def tui(ctx: typer.Context) -> None:
    """Open the text interface through the management socket."""
    try:
        from lupaxa.switchboard.tui import run_tui
    except ImportError as exc:
        message = "Install the tui extra: pip install 'lupaxa-switchboard[tui]'"
        raise SwitchboardError(message) from exc
    config = _load(ctx, None)
    endpoint = management_endpoint(config)
    if endpoint is None:
        raise SwitchboardError("Management is not enabled.")
    run_tui(endpoint)


def main(argv: list[str] | None = None) -> int:
    """Run the Switchboard command line. Return a process exit code."""
    try:
        app(args=argv, standalone_mode=False)
    except typer.Exit as exc:
        return int(exc.exit_code or 0)
    except ClickException as exc:
        exc.show()
        return int(exc.exit_code)
    except SwitchboardError as exc:
        typer.echo(str(exc), err=True)
        return exc.exit_code
    return 0


async def _serve(config: object, *, quiet: bool, source: Path | None = None) -> None:
    from lupaxa.switchboard.config import SwitchboardConfig

    if not isinstance(config, SwitchboardConfig):
        raise UsageError("Configuration could not be loaded.")
    gateway = Gateway(config, source_path=source)
    await gateway.start()
    if not quiet:
        for runtime in gateway.bridges.values():
            if runtime.listening:
                output = _outputs_label(runtime.spec)
                typer.echo(f"{runtime.spec.name} listening on {runtime.label()} -> {output}")
            else:
                typer.echo(f"{runtime.spec.name} failed to start.", err=True)
        if gateway.management is not None:
            address = gateway.management
            if address.transport == "local":
                typer.echo(f"management listening on {address.path}")
            else:
                typer.echo(f"management listening on tcp://{address.host}:{address.port}")
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await gateway.stop()


def _source(ctx: typer.Context, path: Path | None) -> Path | None:
    chosen = path if path is not None else _obj(ctx).get("config")
    if isinstance(chosen, Path):
        return chosen
    candidate = default_config_path()
    if candidate.is_file():
        return candidate
    return None


def _live(config: object, method: str, params: dict[str, object] | None = None) -> object:
    from lupaxa.switchboard.config import SwitchboardConfig

    if not isinstance(config, SwitchboardConfig):
        return None
    endpoint = management_endpoint(config)
    if endpoint is None:
        return None
    try:
        return call(endpoint, method, params)
    except SwitchboardError:
        return None


def _require(
    config: object,
    method: str,
    params: dict[str, object] | None = None,
) -> dict[str, object]:
    from lupaxa.switchboard.config import SwitchboardConfig

    if not isinstance(config, SwitchboardConfig):
        raise UsageError("Configuration could not be loaded.")
    endpoint = management_endpoint(config)
    if endpoint is None:
        raise SwitchboardError("Management is not enabled.")
    result = call(endpoint, method, params)
    if not isinstance(result, dict):
        raise SwitchboardError("Management response is invalid.")
    return result


def _confirm(yes: bool, action: str) -> None:
    if yes:
        return
    if not sys.stdin.isatty():
        raise UsageError(f"Pass --yes to {action}.")
    answer = input(f"{action}? [y/N] ")
    if answer.strip().lower() not in {"y", "yes"}:
        raise SwitchboardError("Cancelled.", exit_code=1)


def _print_event(event: dict[str, object], *, payloads: bool) -> None:
    typer.echo(
        f"{event.get('time')}  {event.get('bridge')}  {event.get('stage')}  "
        f"{event.get('size')} bytes  {event.get('outcome')}"
    )
    preview = event.get("preview")
    if payloads and isinstance(preview, str) and preview:
        typer.echo(f"  preview {preview}")


def _named_check(name: str, status: str, detail: str) -> dict[str, str]:
    return {"name": name, "status": status, "detail": detail}


def _profile_check(bridge: object) -> tuple[str, str]:
    from lupaxa.switchboard.config import BridgeSpec
    from lupaxa.switchboard.exceptions import ProfileError
    from lupaxa.switchboard.profile import decode_structure, encode_structure

    if not isinstance(bridge, BridgeSpec):
        return "fail", "bridge record is invalid"
    structure = bridge.input.structure
    if structure is None and bridge.output is not None:
        structure = bridge.output.structure
    if structure is None:
        return "skip", "no structure"
    try:
        raw = encode_structure(structure, _sample(structure))
        decode_structure(structure, raw, max_bytes=16_777_216)
    except ProfileError:
        return "fail", "structure round trip failed"
    return "pass", "local round trip"


def _sample(structure: object) -> dict[str, object]:
    from lupaxa.switchboard.config import StructureSpec

    if not isinstance(structure, StructureSpec):
        return {}
    values: dict[str, object] = {}
    for item in structure.fields:
        if item.type == "bool":
            values[item.name] = False
        elif item.type == "string":
            values[item.name] = ""
        elif item.type == "bytes":
            values[item.name] = _zeros(item.length or 0)
        elif item.type == "remaining_bytes":
            values[item.name] = ""
        elif item.type == "array":
            values[item.name] = [_sample_item(item) for _ in range(item.count or 0)]
        else:
            values[item.name] = 0
    named = structure.message_type
    if named is not None and named.values:
        values[named.field] = next(iter(named.values))
    return values


def _sample_item(field: object) -> object:
    from lupaxa.switchboard.config import FieldSpec

    if not isinstance(field, FieldSpec):
        return 0
    if field.item == "bool":
        return False
    if field.item == "string":
        return ""
    if field.item == "bytes":
        return _zeros(field.length or 0)
    return 0


def _zeros(length: int) -> str:
    import base64

    return base64.b64encode(b"\x00" * length).decode("ascii")


def _connect_check(bridge: object) -> tuple[str, str]:
    import socket

    from lupaxa.switchboard.config import BridgeSpec

    if not isinstance(bridge, BridgeSpec) or bridge.output is None:
        return "skip", "router outputs are not probed"
    output = bridge.output
    if output.protocol == "udp":
        return "skip", "udp does not confirm a listener"
    if output.port in {None, 0}:
        return "skip", "output port is dynamic"
    try:
        with socket.create_connection((output.host, output.port), 1):
            return "pass", "tcp connect succeeded"
    except OSError:
        return "fail", "tcp connect failed"


def _emit_checks(checks: list[dict[str, str]], *, as_json: bool) -> None:
    if as_json:
        typer.echo(json.dumps({"checks": checks}))
        return
    for item in checks:
        typer.echo(f"{item['name']}: {item['status']}  {item['detail']}")


def _load(ctx: typer.Context, log_level: str | None, path: Path | None = None):
    chosen = path if path is not None else _obj(ctx).get("config")
    config_path = chosen if isinstance(chosen, Path) else None
    return load_config(config_path, log_level=log_level)


def _level(ctx: typer.Context, configured: str, explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    obj = _obj(ctx)
    if obj.get("verbose"):
        return "debug"
    if obj.get("quiet"):
        return "warning"
    return configured


def _obj(ctx: typer.Context) -> dict[str, object]:
    obj = ctx.obj
    if isinstance(obj, dict):
        return {str(key): value for key, value in obj.items()}
    return {}


def _outputs_label(bridge: object) -> str:
    from lupaxa.switchboard.config import BridgeSpec

    if not isinstance(bridge, BridgeSpec):
        raise UsageError("Bridge record is invalid.")
    if bridge.output is not None:
        return endpoint_label(bridge.output)
    return ", ".join(endpoint_label(item) for item in bridge.outputs)


def _bridge_row(bridge: object) -> dict[str, object]:
    from lupaxa.switchboard.config import BridgeSpec

    if not isinstance(bridge, BridgeSpec):
        raise UsageError("Bridge record is invalid.")
    return {
        "name": bridge.name,
        "mode": bridge.mode,
        "enabled": bridge.enabled,
        "input": endpoint_label(bridge.input),
        "output": _outputs_label(bridge),
    }


def _profile(config: object, name: str):
    from lupaxa.switchboard.config import ProfileSpec, SwitchboardConfig

    if not isinstance(config, SwitchboardConfig):
        raise UsageError("Configuration could not be loaded.")
    profile = config.profiles.get(name)
    if not isinstance(profile, ProfileSpec):
        raise SwitchboardError(f"Profile is not configured: {name}", exit_code=1)
    return profile


def _profile_row(name: str, profile: object) -> dict[str, object]:
    from lupaxa.switchboard.config import ProfileSpec

    if not isinstance(profile, ProfileSpec):
        raise UsageError("Profile record is invalid.")
    framing = profile.framing.type if profile.framing is not None else ""
    structure = "yes" if profile.structure is not None else ""
    return {
        "name": name,
        "transport": profile.transport,
        "framing": framing,
        "structure": structure,
    }


def _print_bridges(rows: list[dict[str, object]]) -> None:
    table = Table(title="Configured Bridges")
    table.add_column("Name")
    table.add_column("Mode")
    table.add_column("Input")
    table.add_column("Output")
    for row in rows:
        table.add_row(str(row["name"]), str(row["mode"]), str(row["input"]), str(row["output"]))
    Console().print(table)
