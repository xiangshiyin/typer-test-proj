# typer-test-proj
Play with Tyler for cli tool building

## Package installation
- For local test, run `poetry install`
- For gcloud connection and auth, follow the steps below
  - Install `gcloud` cli [[steps](https://cloud.google.com/sdk)]
  - Run `gcloud init` to initialize `gcloud` cli and pick the default GCP project [[steps](https://cloud.google.com/sdk/docs/install-sdk#initializing_the)]
  - Run `gcloud auth application-default login` to set up the connection to GCP

## Commands to test
For local test, run `poetry shell` to activate the virtual env, then run the following commands
### `testcli hello`
### `testcli goodbye`
### `testcli copy`
Try running `testcli copy --help`

## Reference
- `Typer` documentations [[link](https://typer.tiangolo.com/#run-the-code)]
- Python types intro [[link](https://fastapi.tiangolo.com/python-types/)]
