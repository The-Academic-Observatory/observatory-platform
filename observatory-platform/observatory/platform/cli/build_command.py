import os

from observatory.platform.observatory_config import BuildConfig
from observatory.platform.docker.platform_builder import PlatformBuilder


class BuildCommand:
    def __init__(self, config_path: str):
        # Load config
        config_exists = os.path.exists(config_path)
        if not config_exists:
            raise FileExistsError(f"Observatory config file does not exist: {config_path}")
        else:
            self.config_is_valid = False
            self.config: BuildConfig = BuildConfig.load(config_path)
            self.config_is_valid = self.config.is_valid
            print(self.config.errors)

    def build_observatory_image(self, tag: str):
        print(self.config_is_valid)
        pb = PlatformBuilder(config=self.config, tag=tag)
        pb.build()

    def build_vm_image(self):
        pass

    def build_api_image(self):
        pass
