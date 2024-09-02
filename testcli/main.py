import typer
from typing import Optional

app = typer.Typer()


@app.command()
def hello(name: str):
    print(f"Hello {name}")


@app.command()
def goodbye(
    first_name: str,
    formal: bool = False,
    last_name: str = None,
    intro: Optional[str] = None,
):
    full_name = f"{first_name} {last_name}" if last_name else f"{first_name}"
    print(full_name)
    if formal:
        print(f"Goodbye Ms. {full_name}. Have a good day.")
    else:
        print(f"Bye {full_name}")


if __name__ == "__main__":
    app()
