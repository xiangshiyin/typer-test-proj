import typer
from typing import Optional
from typing_extensions import Annotated

from .copy_files_from_a_to_b import copy_a2b

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


@app.command(
    help="Copy files from one bucket to another and distribute files into equally sized slices in destination, if needed"
)
def copy(
    source_bucket: Annotated[str, typer.Argument(help="Source bucket name")],
    source_dir_prefix: Annotated[str, typer.Argument(help="Source directory prefix")],
    destination_bucket: Annotated[str, typer.Argument(help="Destination bucket name")],
    destination_dir: Annotated[str, typer.Argument(help="Destination directory name")],
    destination_slices: Annotated[
        int, typer.Argument(help="Number of slices to split in the destination folder")
    ] = 2,
    max_workers: Annotated[
        int, typer.Argument(help="Maximum number of workers to run concurrently")
    ] = 2,
):
    copy_a2b(
        source_bucket_name=source_bucket,
        source_directory_prefix=source_dir_prefix,
        destination_bucket_name=destination_bucket,
        destination_directory=destination_dir,
        destination_slices=destination_slices,
        max_workers=max_workers,
    )


if __name__ == "__main__":
    app()
