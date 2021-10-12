import click

from observatory.api.docker import build_docker_file
from observatory.api.server.openapi_renderer import OpenApiRenderer


@click.group()
def cli():
    """The Observatory API command line tool.

    COMMAND: the commands to run include:\n
      - generate-open-api: generate an OpenAPI specification for the Observatory API.\n
    """

    pass


@cli.command()
@click.argument("template-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("output-file", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--cloud-endpoints", is_flag=True, default=False, help="Generate OpenAPI config for Cloud Endpoints.")
@click.option(
    "--api-client", is_flag=True, default=False, help="Generate OpenAPI config for OpenAPI client generation."
)
def generate_openapi_spec(template_file, output_file, cloud_endpoints, api_client):
    """Generate an OpenAPI specification for the Observatory API.\n

    TEMPLATE_FILE: the type of config file to generate.
    OUTPUT_FILE: the type of config file to generate.
    """

    # Render file
    renderer = OpenApiRenderer(template_file, cloud_endpoints=cloud_endpoints, api_client=api_client)
    render = renderer.render()

    # Save file
    with open(output_file, mode="w") as f:
        f.write(render)


@cli.command()
@click.argument("observatory-api-path", type=click.Path(exists=True, file_okay=False, dir_okay=True))
def build_image(observatory_api_path: str, tag: str):
    """Build an Observatory API Docker image.\n
    OBSERVATORY_API_PATH: the path to the observatory api package.
    TAG: the Docker tag name.
    """

    build_docker_file(observatory_api_path, tag)


if __name__ == "__main__":
    cli()
