import click

from observatory.api.cli.openapi_renderer import OpenApiRenderer


@click.group()
def cli():
    """The Observatory API command line tool.

    COMMAND: the commands to run include:\n
      - generate-openapi-spec: generate an OpenAPI specification for the Observatory API.\n
    """

    pass


@cli.command()
@click.argument("template-file", type=click.Path(exists=True, file_okay=True, dir_okay=False))
@click.argument("output-file", type=click.Path(exists=False, file_okay=True, dir_okay=False))
@click.option("--usage-type", type=click.Choice(["cloud_endpoints", "backend", "openapi_generator"]), required=True)
def generate_openapi_spec(template_file, output_file, usage_type):
    """Generate an OpenAPI specification for the Observatory API.\n

    TEMPLATE_FILE: the type of config file to generate.
    OUTPUT_FILE: the type of config file to generate.
    """

    # Render file
    renderer = OpenApiRenderer(template_file, usage_type=usage_type)
    render = renderer.render()

    # Save file
    with open(output_file, mode="w") as f:
        f.write(render)


if __name__ == "__main__":
    cli()
