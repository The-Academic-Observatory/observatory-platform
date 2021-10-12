
class VmImageBuilder:

    def __init__(self):
        pass

    def build_image(self) -> Tuple[str, str, int]:
        """Build the Observatory Platform Google Compute image with Packer.
        :return: output and error stream results and proc return code.
        """

        # Make Terraform files
        self.build_terraform()

        # Load template
        template_vars = {
            "credentials_file": self.config.google_cloud.credentials,
            "project_id": self.config.google_cloud.project_id,
            "zone": self.config.google_cloud.zone,
            "environment": self.config.backend.environment.value,
        }
        variables = []
        for key, val in template_vars.items():
            variables.append("-var")
            variables.append(f"{key}={val}")

        # Build the containers first
        args = ["packer", "build"] + variables + ["-force", "observatory-image.json"]

        if self.debug:
            print("Executing subprocess:")
            print(indent(f"Command: {subprocess.list2cmdline(args)}", INDENT1))
            print(indent(f"Cwd: {self.terraform_build_path}", INDENT1))

        proc: Popen = subprocess.Popen(
            args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=self.terraform_build_path
        )

        # Wait for results
        # Debug always true here because otherwise nothing gets printed and you don't know what the state of the
        # image building is
        output, error = stream_process(proc, True)
        return output, error, proc.returncode