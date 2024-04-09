import json

from collections import Counter
from itertools import groupby
from operator import itemgetter
from typing import Any, Dict
from pathlib import Path

from PIL import Image

from docint.ppln import Component, Pipeline

def get_time_str(time_secs):
    hr = 0 if time_secs < 3600 else int(time_secs / 3600)
    time_secs = time_secs - (hr * 3600)

    mn = 0 if time_secs < 60 else int(time_secs / 60)
    time_secs = time_secs - (mn * 60)

    ss = int(time_secs)
    return f"{hr:02d}:{mn:02d}:{ss:02d}"


def get_frame_text(frame_img, bbox):
    import pytesseract
    cropped_img = frame_img.crop(bbox)
    return pytesseract.image_to_string(cropped_img, lang="eng+hin")

ImagehashCutoff = 3
def match_image_hash(frame_img, hash_bbox, match_hash):
    import imagehash
    return match_hash - imagehash.colorhash(frame_img.crop(hash_bbox)) > ImagehashCutoff



def get_scene_text_two_pass(video, start_ss, end_ss, bbox, gap, hash_bbox, match_hash):
    gap1 = 10 * gap
    gap2 = gap

    assert gap1 > gap2, f"incorrect values of gaps {gap1} {gap2}"

    def iter_pass1(start_ss, end_ss, gap1):
        t_idx = start_ss
        while t_idx < end_ss:
            yield t_idx
            t_idx += gap1

    def iter_pass2(start_ss, end_ss, gap1, gap2, first_pass_timeidxs):
        t_idx = start_ss
        for f_idx in first_pass_timeidxs:
            f_start_idx, f_end_idx = f_idx - gap1, f_idx + gap2

            t_idx = max(t_idx, f_start_idx)
            while t_idx < min(end_ss, f_end_idx):
                yield t_idx
                t_idx += gap2

    second_pass_timeidxs, time_idx_texts = [], {}
    for time_idx in iter_pass1(start_ss, end_ss, gap1):
        frame_img = video.get_frame(time_idx)
        
        if not match_image_hash(frame_img, hash_bbox, match_hash):
            continue
        
        frame_text = get_frame_text(frame_img, bbox)
        if frame_text:
            frame_text_clean = frame_text.replace("\n", "")
            print(f'\t{get_time_str(time_idx)}: {frame_text_clean}')
            second_pass_timeidxs.append(time_idx)
            time_idx_texts[time_idx] = frame_text
        else:
            print(f'\t{get_time_str(time_idx)}')            

    if not second_pass_timeidxs and (end_ss - start_ss) < 10:
        print('\tEmpty- Adding start')
        second_pass_timeidxs.append(start_ss)

    print('\tPhase II')
    cropped_texts = []
    for time_idx in iter_pass2(start_ss, end_ss, gap1, gap2, second_pass_timeidxs):
        #print(f'\t{time_idx}')
        frame_text = time_idx_texts.get(time_idx, None)
        if not frame_text:
            frame_img = video.get_frame(time_idx)
            frame_text = get_frame_text(frame_img, bbox)

            
        if frame_text:
            frame_text_clean = frame_text.replace('\n', '')
            print(f'\t{get_time_str(time_idx)}: {frame_text_clean}')                        
            cropped_texts.append(frame_text)
        else:
            print(f'\t{get_time_str(time_idx)}')
    cropped_texts.sort(key=len, reverse=True) # prefer longer text over shorter
    return max(c := Counter(cropped_texts), key=c.get) if cropped_texts else "NO_NAME"



def convert_bbox(bbox, image_size):
    (w, h) = image_size
    return [round(bbox[0] * w), round(bbox[1] * h), round(bbox[2] * w), round(bbox[3] * h)]

TalkerConfig = { 'pre-2024': { 'bbox': [0.08, 0.775, 0.92, 0.835],
                               'color_hash': '31000e00008',
                               'hash_bbox': [0.0703125, 0, 0.234375, 1.0],
                               'gap': 1
                             },
                '2024': { 'bbox': [0.08, 0.775, 0.92, 0.835],
                          'color_hash': '31000e00008',
                          'hash_bbox': [0.0703125, 0, 0.234375, 1.0],                          
                          'gap': 1,
                         }
                }

@Pipeline.register_component(
    assigns="scene_talkers",
    depends=[],
    requires=[],
)
class FindTalker(Component):
    class Config:
        text_configs = [Dict[str, Any]]

    def __call__(self, video, cfg):
        print(f"Processing {video.file_name}")
        json_path = Path("output") / f"{video.file_name}.scene_talkers.json"
        if json_path.exists():
            print(f'scene_talkers {video.file_name} exists')            
            video.scene_talkers = json.loads(json_path.read_text())
            return video
        
        video.scene_talkers = {}
        bbox, gap = TalkerConfig['pre-2024']['bbox'], TalkerConfig['pre-2024']['gap']
        img_bbox = convert_bbox(bbox, video.size)

        hash_bbox = TalkerConfig['pre-2024']['hash_bbox']
        hash_bbox = convert_bbox(hash_bbox, video.size)

        import imagehash
        image_hash = TalkerConfig['pre-2024']['color_hash']
        image_hash = imagehash.hex_to_hash(image_hash)        
        
        
        scene_texts, merged_scene_texts = [], []
        for s, e in video.scene_intervals:
            scene_text = get_scene_text_two_pass(video, s, e, img_bbox, gap, hash_bbox, image_hash)
            scene_text = scene_text.replace('\n', '')
            print(f"{get_time_str(s)}->{get_time_str(e)} {scene_text}")
            scene_texts.append(((s, e), scene_text))

        print("Merging Scenes")
        for text, group in groupby(scene_texts, key=itemgetter(1)):
            group = list(group)
            s, e = (group[0][0][0], group[-1][0][1])
            merged_scene_texts.append(((s, e), text))
            print(f"{get_time_str(s)}->{get_time_str(e)} {text}")

        video.scene_talkers = merged_scene_texts
            
        json_path.write_text(json.dumps(video.scene_talkers))
        return video
