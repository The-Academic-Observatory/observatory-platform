# @patch("subprocess.Popen")
# @patch("observatory.platform.terraform_builder.stream_process")
# def test_build_image(self, mock_stream_process, mock_subprocess):
#     """Test building of the observatory platform"""
#
#     # Check that the environment variables are set properly for the default config
#     with CliRunner().isolated_filesystem() as t:
#         mock_subprocess.return_value = Popen()
#         mock_stream_process.return_value = ("", "")
#
#         # Save default config file
#         config_path = self.save_terraform_config(t)
#
#         # Make observatory files
#         cmd = TerraformBuilder(config_path=config_path)
#
#         # Build the image
#         output, error, return_code = cmd.build_image()
#
#         # Assert that the image built
#         expected_return_code = 0
#         self.assertEqual(expected_return_code, return_code)
#
#
# @patch("subprocess.Popen")
# @patch("observatory.platform.terraform_builder.stream_process")
# def test_gcloud_activate_service_account(self, mock_stream_process, mock_subprocess):
#     """Test activating the gcloud service account"""
#
#     # Check that the environment variables are set properly for the default config
#     with CliRunner().isolated_filesystem() as t:
#         mock_subprocess.return_value = Popen()
#         mock_stream_process.return_value = ("", "")
#
#         # Save default config file
#         config_path = self.save_terraform_config(t)
#
#         # Make observatory files
#         cmd = TerraformBuilder(config_path=config_path)
#
#         # Activate the service account
#         output, error, return_code = cmd.gcloud_activate_service_account()
#
#         # Assert that account was activated
#         expected_return_code = 0
#         self.assertEqual(expected_return_code, return_code)
#
#
# @patch("subprocess.Popen")
# @patch("observatory.platform.terraform_builder.stream_process")
# def test_gcloud_builds_submit(self, mock_stream_process, mock_subprocess):
#     """Test gcloud builds submit command"""
#
#     # Check that the environment variables are set properly for the default config
#     with CliRunner().isolated_filesystem() as t:
#         mock_subprocess.return_value = Popen()
#         mock_stream_process.return_value = ("", "")
#
#         # Save default config file
#         config_path = self.save_terraform_config(t)
#
#         # Make observatory files
#         cmd = TerraformBuilder(config_path=config_path)
#
#         # Build the image
#         output, error, return_code = cmd.gcloud_builds_submit()
#
#         # Assert that the image built
#         expected_return_code = 0
#         self.assertEqual(expected_return_code, return_code)
#
#
# def test_build_api_image(self):
#     """Test building API image using Docker"""
#
#     # Check that the environment variables are set properly for the default config
#     with CliRunner().isolated_filesystem() as t:
#         # Save default config file
#         config_path = self.save_terraform_config(t)
#
#         # Make observatory files
#         cmd = TerraformBuilder(config_path=config_path)
#
#         args = ["docker", "build", "."]
#         print("Executing subprocess:")
#
#         proc: Popen = subprocess.Popen(
#             args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cmd.api_package_path
#         )
#         output, error = stream_process(proc, True)
#
#         # Assert that the image built
#         expected_return_code = 0
#         self.assertEqual(expected_return_code, proc.returncode)
