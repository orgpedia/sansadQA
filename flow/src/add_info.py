import json
from pathlib import Path
from typing import Any, Dict

from docint.ppln import Component, Pipeline


@Pipeline.register_component(
    assigns="info",
    depends=[],
    requires=[],
)
class AddInfo(Component):
    infos_dict:Dict[str, Any] = {}
    class Config:
        info_file: str = ''
        
    def load_infos(self, infos_file):
        if not self.infos_dict:
            infos = json.loads(Path(infos_file).read_text())
            self.infos_dict = {i['name']: i for i in infos}

    
    def __call__(self, video, cfg):
        print(f"Processing {video.file_name}")
        self.load_infos(cfg.info_file)
        


        if video.file_name in self.infos_dict:
            video.info = self.infos_dict[video.file_name]
        else:
            video.info = None

        return video
