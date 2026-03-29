import subprocess


def expand_command(params: dict) -> str:
    """Expand $VAR references in a factory's command using bash with its env vars.

    Uses an isolated env (no workstation vars) — exactly how AWS Batch runs it.
    """
    cmd = params["ContainerOverrides"]["Command"][2]
    env_dict = {e["Name"]: e["Value"] for e in params["ContainerOverrides"]["Environment"]}
    result = subprocess.run(
        ["bash", "-c", f"echo {cmd}"],
        env={**env_dict, "PATH": "/usr/bin"},
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def get_env_dict(params: dict) -> dict[str, str]:
    """Extract {Name: Value} dict from ContainerOverrides.Environment."""
    return {e["Name"]: e["Value"] for e in params["ContainerOverrides"]["Environment"]}
