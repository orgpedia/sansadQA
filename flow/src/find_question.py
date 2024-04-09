import io
import json
import os
import time
import re

from collections import Counter
from itertools import groupby
from operator import itemgetter
from typing import Any, Dict
from pathlib import Path

from PIL import Image



from docint.ppln import Component, Pipeline
from docint.video import Video

def run_sync_gdai_image(page_image, line_height):
    """ Performs OCR on a PIL image using Google Document AI and returns the text line by line. """

    from google.cloud import documentai_v1 as documentai
    
    # Convert PIL Image to bytes
    img_byte_arr = io.BytesIO()
    page_image.save(img_byte_arr, format='PNG')
    image_bytes = img_byte_arr.getvalue()

    # Specify the project and location (region) to use
    project_id = os.environ['GCP_PROJECT_ID']
    location = os.environ['GCP_PROCESSOR_REGION']
    processor_id = os.environ['GCP_DAI_PROCESSOR_ID']

    # Initialize the Document AI client
    client = documentai.DocumentProcessorServiceClient()

    # The full resource name of the processor, e.g.:
    # projects/project-id/locations/location/processors/processor-id
    # You must create a new processor in the Document AI console
    name = f"projects/{project_id}/locations/{location}/processors/{processor_id}"

    # Document AI request
    request = {
        "name": name,
        "raw_document": {
            "content": image_bytes,
            "mime_type": "image/png"
        }
    }

    # Perform OCR
    result = client.process_document(request=request)

    num_lines = int((page_image.height + 1) / line_height)
    document_text, text_lines = result.document.text, [''] * num_lines
    
    for page in result.document.pages:
        for token in page.tokens:
            segs = token.layout.text_anchor.text_segments
            text_spans = [(s.start_index, s.end_index) for s in segs]
            text = ' '.join(document_text[s:e] for (s, e) in text_spans)
            text = text.strip(': \n-.')

            if not (text.isdigit() and text.isascii()):
                print(f'Skipping: >{text.strip()}<')
                continue

            x_coords = [v.x for v in token.layout.bounding_poly.vertices]
            y_coords = [v.y for v in token.layout.bounding_poly.vertices]

            # skip words that are as part of the date
            if min(x_coords) > 1028:
                continue
            
            y_mid = int((max(y_coords) + min(y_coords))/2.0)
            line_num = int(y_mid / line_height)
            
            print(f'*** {min(x_coords)}: [{line_num}] >{text.strip()}<')
            text_lines[line_num] += text + ' '
    return text_lines

def process_image(img):
    import cv2
    import numpy as np
    
    _, height = img.size
    
    #img = img.crop((9, 0, 1028, height-1))

    cv_img = np.array(img.convert('RGB'))
    cv_img = cv_img[:, :, ::-1].copy()
    cv_img = cv2.cvtColor(cv_img, cv2.COLOR_BGR2GRAY)
    (thresh, cv_img) = cv2.threshold(cv_img, 128, 255, cv2.THRESH_BINARY | cv2.THRESH_OTSU)
    #cv_img = cv2.bitwise_not(cv_img)

    img = Image.fromarray(cv_img)
    return img
def merge_images(image_list):
    page_height = sum(i.height for i in image_list)
    page_width = max(i.width for i in image_list)

    page_image = Image.new('RGB', (page_width, page_height))
    y_pos = 0
    for img in image_list:
        start_y = y_pos
        page_image.paste(img, (0, y_pos))
        y_pos += img.height
    return page_image

def get_page_lines(image_list):
    def word2texts(words):
        return ' '.join(''.join(c['text'] for c in w['symbols']) for w in words)

    image_list = [process_image(i) for i in image_list]
    image_height = image_list[0].height
    
    page_image = merge_images(image_list)
    page_lines = run_sync_gdai_image(page_image, image_height)
    print(f'# images: {len(image_list)} # lines: {len(page_lines)}')
    return page_lines


def get_time_str(time_secs):
    hr = 0 if time_secs < 3600 else int(time_secs / 3600)
    time_secs = time_secs - (hr * 3600)

    mn = 0 if time_secs < 60 else int(time_secs / 60)
    time_secs = time_secs - (mn * 60)

    ss = int(time_secs)
    return f"{hr:02d}:{mn:02d}:{ss:02d}"

#@record_timing
def get_frame_text(frame_img):
    import pytesseract
    return pytesseract.image_to_string(frame_img, lang="eng+hin")

    #return pytesseract.image_to_string(frame_bw_img, lang='eng', 
    #                                   config='--psm 10 --oem 3 -c tessedit_char_whitelist=0123456789')


#@record_timing
def get_frame_image(video, time_idx):
    return  video.get_frame(time_idx)

ImagehashCutoff = 4
#@record_timing
def match_image_hash(frame_img, hash_bbox, match_hash, use_color_hash):
    import imagehash
    frame_img.save('/tmp/ticker.jpg')
    frame_img.crop(hash_bbox).save('/tmp/quest_hash.jpg')
    if use_color_hash:
        diff = match_hash - imagehash.colorhash(frame_img.crop(hash_bbox), binbits=3)
        pass
    else:
        diff = match_hash - imagehash.colorhash(frame_img.crop(hash_bbox))
    return diff < ImagehashCutoff        




def convert_bbox(bbox, image_size):
    (w, h) = image_size
    return [round(bbox[0] * w), round(bbox[1] * h), round(bbox[2] * w), round(bbox[3] * h)]

def get_question_num(frame_text, en_num_res, hi_num_res):
    for (en_num_re, hi_num_re) in zip(en_num_res, hi_num_res):
        en_match, hi_match = en_num_re.search(frame_text), hi_num_re.search(frame_text)
        
        q_num = en_match.group(1) if en_match else ''
        q_num = hi_match.group(1) if (not q_num) and hi_match else q_num
        if q_num.isdigit():
            return q_num
    return ''

def is_valid_qnum(q_num, prev_num):
    if not q_num.isdigit():
        return False

    if int(q_num) > 1000:
        return False

    if prev_num == -1:
        return True
    
    q_num = int(q_num)
    
    if q_num < prev_num:
        return False
    elif (q_num - prev_num) > 5:
        return False
    else:
        return True
PageLines = 25

QuestionConfig = { 'pre-2024': { 'bbox': [0, 0.9125, 0.81640625, 0.9611],
                                 'color_hash': '38000e00008',
                                 'hash_bbox': [0.0703125, 0, 0.234375, 1.0],
                                 'gap': 1
                                },
                   '2024': { 'bbox': [0.1859375, 0.908333, 0.809375, 0.9513888],
                             'color_hash': '07000000000',
                             'hash_bbox': [0.0703125, 0, 0.234375, 1.0],
                             'gap': 1,
                            }
                  }
@Pipeline.register_component(
    assigns="scene_questions",
    depends=[],
    requires=[],
)
class FindQuestion(Component):
    class Config:
        question_times: Dict[int, int] = {}
    
    def __call__(self, video, cfg):
        import imagehash        
        print(f"Processing {video.file_name}")
        json_path = Path("output") / f"{video.file_name}.scene_questions.json"

        if json_path.exists():
            print(f'extractquestion {video.file_name} exists')            
            video.question_times = json.loads(json_path.read_text())
            return video

        if cfg.question_times:
            video.question_times = [f'{k}: {v}' for (k, v) in cfg.question_times.items()]
            json_path.write_text(json.dumps(video.question_times))
            return video

        import imagehash
        if video.info['publish_date'][:4] == '2024':
            bbox, gap = QuestionConfig['2024']['bbox'], QuestionConfig['2024']['gap'] # noqa
            hash_bbox = QuestionConfig['2024']['hash_bbox']
            image_hash = QuestionConfig['2024']['color_hash']
            match_hash = imagehash.hex_to_flathash(image_hash, hashsize=3)
            use_color_hash = True
        else:
            bbox, gap = QuestionConfig['pre-2024']['bbox'], QuestionConfig['pre-2024']['gap'] # noqa
            hash_bbox = QuestionConfig['pre-2024']['hash_bbox']
            image_hash = QuestionConfig['pre-2024']['color_hash']
            match_hash = imagehash.hex_to_hash(image_hash)
            use_color_hash = False
            

        bbox = convert_bbox(bbox, video.size)        

        print(f'{video.size} -> {bbox}')
        ticker_gap = 5        

        video.question_time_dict = {}
        en_num_res = [re.compile(r'No\.\s*(\d+)'), re.compile(r'Ques[\w:]*\s*(\d+)')]
        hi_num_res = [re.compile(r'संख्या\:\s*(\d+)'), re.compile(r'प्रश्न:\s*(\d+)')]

        image_list, time_idx_list, text_lines = [], [], []
        for time_idx in range(0, int(video.duration), ticker_gap):
             frame_img = get_frame_image(video, time_idx)
             crop_img = frame_img.crop(bbox)
             new_hash_bbox = convert_bbox(hash_bbox, crop_img.size)
             if match_image_hash(crop_img, new_hash_bbox, match_hash, use_color_hash):
                 image_list.append(crop_img), time_idx_list.append(time_idx)
                 if len(image_list) == PageLines:
                     page_text_lines = get_page_lines(image_list)
                     print(f'[{time_idx}] {"|".join(page_text_lines)}')
                     text_lines += page_text_lines
                     image_list.clear()
                     assert len(text_lines) == len(time_idx_list)
             else:
                 print(f'[{time_idx}: Hash Unmatched')
                     
        if image_list:
            text_lines += get_page_lines(image_list)
            image_list.clear()        



        video.question_times = [f'{t}: {num.strip()}' for (t, num) in zip(time_idx_list, text_lines)]
        #
        json_path.write_text(json.dumps(video.question_times))
        return video



"""
1. Convert the b/w and sharpen
2. Remove the right hand side date/time
3. 


mport cv2
import numpy as np
import pytesseract

pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"

# Load image, grayscale, apply sharpening filter, Otsu's threshold 
image = cv2.imread('1.png')
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
sharpen_kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
sharpen = cv2.filter2D(gray, -1, sharpen_kernel)
thresh = cv2.threshold(sharpen, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)[1]

# OCR
data = pytesseract.image_to_string(thresh, lang='eng', config='--psm 6')
print(data)

cv2.imshow('sharpen', sharpen)
cv2.imshow('thresh', thresh)
cv2.waitKey()


"""


        # Tesseract
        # start_idx, prev_num = None, -1
        # for time_idx in range(0, int(video.duration), ticker_gap):
        #     frame_img = get_frame_image(video, time_idx)
        #     crop_img = frame_img.crop(bbox)
            
        #     if match_image_hash(crop_img, hash_bbox, match_hash):
        #        frame_text = get_frame_text(crop_img)
        #         frame_text = frame_text.strip().replace('\n', ' ')
        #         q_num = get_question_num(frame_text, en_num_res, hi_num_res)
        #         #print(f'\t{get_time_str(time_idx)}: [{q_num}] {frame_text}')
        #         res_num = q_num
        #         if is_valid_qnum(q_num, prev_num):
        #             q_num = int(q_num)
        #             if prev_num != q_num:
        #                 if prev_num != -1:
        #                     import pdb
        #                     pdb.set_trace()
        #                     print(f'Saving {prev_num} {start_idx}')
        #                     video.question_time_dict[prev_num] = start_idx
        #                 start_idx, prev_num = time_idx, q_num
        #         else:
        #             print(f'{video.file_name} {get_time_str(time_idx)}[{time_idx}]: >{res_num}< {frame_text}')

        # if prev_num != -1:
        #     video.question_time_dict[prev_num] = start_idx
        # video.scene_questions = video.question_time_dict        

