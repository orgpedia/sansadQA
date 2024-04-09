import json
from collections import Counter
from pathlib import Path
from typing import Any, Dict

#from PIL import Image
from docint.ppln import Component, Pipeline


def get_time_str(time_secs):
    hr = 0 if time_secs < 3600 else int(time_secs / 3600)
    time_secs = time_secs - (hr * 3600)

    mn = 0 if time_secs < 60 else int(time_secs / 60)
    time_secs = time_secs - (mn * 60)

    ss = int(time_secs)
    return f"{hr:02d}:{mn:02d}:{ss:02d}"

def convert_bbox(bbox, image_size):
    (w, h) = image_size
    return [round(bbox[0] * w), round(bbox[1] * h), round(bbox[2] * w), round(bbox[3] * h)]

def get_frame_image(video, time_idx):
    return  video.get_frame(time_idx)

def get_frame_text(frame_img, bbox):
    import pytesseract
    cropped_img = frame_img.crop(bbox)
    #cropped_img.save('/tmp/crop_date.png')
    return pytesseract.image_to_string(cropped_img, lang="eng")

ImagehashCutoff = 4
def match_image_hash(frame_img, hash_bbox, match_hash):
    import imagehash
    print(match_hash - imagehash.colorhash(frame_img.crop(hash_bbox), binbits=3))
    return (match_hash - imagehash.colorhash(frame_img.crop(hash_bbox), binbits=3)) <= ImagehashCutoff


DateConfig = { 'pre-2024': { 'bbox': [0.816406250, 0.9125, 0.93359375, 0.9611],
                             'color_hash': '01000000006',
                             'hash_bbox': [0.816406250, 0.9125, 0.93359375, 0.9611],
                             'gap': 1
                            },
               '2024': { 'bbox': [0.809375, 0.908333, 0.95703125, 0.9513888],
                         'color_hash': '32002e00000',
                         'hash_bbox': [0.809375, 0.908333, 0.95703125, 0.9513888],
                         'gap': 1,
                        }
              }

def extract_date_str(date_str):
    print(date_str)
    for split_char in '|(){}[] ':
        split_list = date_str.split(split_char, 1)
        if len(split_list) > 1:
            return split_list[1].strip()
        
    return None

DateCountCutoff = 3
@Pipeline.register_component(
    assigns="video_date",
    depends=[],
    requires=[],
)
class FindDate(Component):
    class Config:
        text_configs = [Dict[str, Any]]

    def __call__(self, video, cfg):
        print(f"Processing {video.file_name}")
        json_path = Path("output") / f"{video.file_name}.video_date.json"
        
        if json_path.exists():
            print(f'video_date {video.file_name} exists')            
            video.date_str = json.loads(json_path.read_text())
            return video
        
        if video.info['publish_date'][:4] == '2024':
            bbox, gap = DateConfig['2024']['bbox'], DateConfig['2024']['gap'] # noqa
            hash_bbox = DateConfig['2024']['hash_bbox']
            image_hash = DateConfig['2024']['color_hash']
        else:
            bbox, gap = DateConfig['pre-2024']['bbox'], DateConfig['pre-2024']['gap'] # noqa
            hash_bbox = DateConfig['pre-2024']['hash_bbox']
            image_hash = DateConfig['pre-2024']['color_hash']
            
        import imagehash        
        img_bbox = convert_bbox(bbox, video.size)
        print(f'{video.size} -> {img_bbox}')

        hash_bbox = convert_bbox(hash_bbox, video.size)
        #image_hash = imagehash.hex_to_hash(image_hash)        
        image_hash = imagehash.hex_to_flathash(image_hash, hashsize=3)

        video.date_str = ''
        date_counter = Counter()

        if hasattr(video, 'scene_intervals'):
            intervals = video.scene_intervals[1:]
        else:
            start_list = range(30, int(video.duration), 30)
            end_list = range(60, int(video.duration), 30)
            intervals = zip(start_list, end_list)
        
        for s, e in intervals:
            frame_img = get_frame_image(video, s)
            #crop_img = frame_img.crop(hash_bbox)
            #crop_img.save('/tmp/date_hash.png')
            
            if match_image_hash(frame_img, hash_bbox, image_hash):
                frame_text = get_frame_text(frame_img, img_bbox)
                date_str = extract_date_str(frame_text)
                
                if date_str:
                    date_counter[date_str] += 1
                    max_date_str = max(date_counter, key=date_counter.get)
                    max_count = date_counter[max_date_str]
                    print(f'{int(s)} >{frame_text.strip()}< -> >{date_str}< # {max_count}')
                    if max_count > DateCountCutoff:
                        video.date_str = max_date_str
                        break
                
        json_path.write_text(json.dumps(video.date_str))
        return video
