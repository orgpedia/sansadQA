import csv
import json
import sys

from pathlib import Path
import collections
from more_itertools import flatten, first

def tee(iterable, n=2):
    it = iter(iterable)
    deques = [collections.deque() for i in range(n)]
    def gen(mydeque):
        while True:
            if not mydeque:             # when the local deque is empty
                try:
                    newval = next(it)   # fetch a new value and
                except StopIteration:
                    return
                for d in deques:        # load it to all the deques
                    d.append(newval)
            yield mydeque.popleft()
    return tuple(gen(d) for d in deques)


def pairwise(iterable):
    # pairwise('ABCDEFG') --> AB BC CD DE EF FG
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

ignore_strs = ['-', '|', '/', '।', '\.\.', '»', '\\', '॥', '=', '>', '<', '«', 'SS', '——', ']', '—', '2', '6', '9', '0', '7', 'eee' , 'ii', ' ae']

def get_time_str(time_secs):
    if time_secs is None:
        return ''
    
    hr = 0 if time_secs < 3600 else int(time_secs / 3600)
    time_secs = time_secs - (hr * 3600)

    mn = 0 if time_secs < 60 else int(time_secs / 60)
    time_secs = time_secs - (mn * 60)

    ss = int(time_secs)
    return f"{hr:02d}:{mn:02d}:{ss:02d}"

def infer_questions(number_times):
    def num_digits(num):
        return len(str(num))
    
    idx_nums = []
    for line in number_times:
        idx, nums = line.split(':', 1)
        if not nums.strip():
            continue
        idx, nums = int(idx), [int(n) for n in nums.strip().split()]
        idx_nums.append((idx, nums))
        
    if not idx_nums:
        return []

    numbers = flatten([nums for (_, nums) in idx_nums])
    numbers_cntr = collections.Counter(numbers)

    CountCutoff = 3 if len(idx_nums) > 10 else 0
    
    # only select question nums that occur more than CountCutoff
    question_nums = [k for (k,v) in numbers_cntr.items() if v > CountCutoff]

    question_nums = sorted(question_nums)
    
    # Identify most common digits and only select those numbers
    num_digits_ctr = collections.Counter([num_digits(n) for n in question_nums])
    #print(question_nums)
    #print(num_digits_ctr)
    
    max_num_digits = max(num_digits_ctr, key=num_digits_ctr.get)

    question_nums = [n for n in question_nums if num_digits(n) == max_num_digits]


    question_times_list = []
    for q_num in question_nums:
        time_idx = first(idx for (idx, nums) in idx_nums if q_num in nums)
        question_times_list.append((q_num, time_idx))
    return question_times_list
        


def get_question_num(q_time_pairs, start, end):
    for (q_num, (q_s, q_e)) in q_time_pairs.items():
        if q_s <= start <= q_e:
            return q_num, q_s
    return None, None

def export_scenes(doc_dir, video_infos, export_dir):
    def get_url(info, s, e):
        if s is None or e is None:
            return ''
        
        s = max(int(s)-2, 0)
        e = int(e) + 2
        return f"https://www.youtube.com/embed/{info['video_id']}?start={s}&end={e}"

    def has_ignored(m):
        return any(i in m for i in ignore_strs)

    def get_file_num(file_path):
        (stub, num) = file_path.stem.replace('.mp4.video', '').split('-')
        return int(num)

    def get_question(s):
        (idx, q) = s.split(':')
        idx = int(idx.strip())
        q = int(q) if q.strip() else None
        return (q, idx)
    
    
    doc_infos = []
    
    doc_files = doc_dir.glob('*.video.json')
    doc_files = sorted(doc_files, key=get_file_num)
    
    
    for doc_file in doc_files:
        if doc_file.name.replace('.video.json', '') in ['sansadQA-114.mp4', 'sansadQA-115.mp4', 'sansadQA-116.mp4', 'sansadQA-117.mp4', 'sansadQA-118.mp4', 'sansadQA-119.mp4', 'sansadQA-125.mp4', 'sansadQA-136.mp4']:
            continue

        
        doc = json.loads(doc_file.read_text())
        video_file_name = doc_file.name.replace('.video.json', '')
        info = [i for i in video_infos if i['name'] == video_file_name][0]

        q_times = infer_questions(doc['question_times'])

        if q_times and q_times[0] == ('-1', None):
            q_times.pop()

        # to ensure that last question is captured
        q_times.append((0, doc['scene_intervals'][-1][1]))
        
        q_time_pairs = dict((q1[0], (q1[1], q2[1])) for (q1, q2) in pairwise(q_times) if q2)

        if len(q_times) > 1:
            print(f'{video_file_name} Building q_time_pairs {q_times}')
            last_q, last_s = q_times[-1][0], q_times[-1][1]
            last_e = doc['scene_talkers'][-1][0][1] + 1
            q_time_pairs[last_q] = (last_s, last_e)


        video_date = doc['date_str']
        d, m, y = video_date.split('.')
        video_date_str = f'{d}-{m}-20{y}'
        
        
        for scene in doc['scene_talkers']:
            ((s, e), m) = scene
            q_num, q_start = get_question_num(q_time_pairs, s, e)
            m = m.strip('=+7:; %*\"')

            if has_ignored(m):
                m = 'UNREADABLE_NAME'
            
            doc_info = {'Name': video_file_name, 'Title': info['title'], 'House': info['house'],
                        'Start': get_time_str(s), 'End': get_time_str(e), 'Duration': f'{int(e-s)}s',
                        'Question': q_num, 'QuestionStart': get_time_str(q_start), 'Member': m,
                        'URL': get_url(info, s, e), 'Date': video_date_str}
            
            doc_infos.append(doc_info)

    fields = ['Name', 'Title', 'Date', 'House', 'Start', 'End', 'Duration', 'Question', 'QuestionStart', 'Member', 'URL']
    with open((export_dir / 'snippets.csv'), 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fields)
        csv_writer.writeheader()
        csv_writer.writerows(doc_infos)

    def sortable(tup):
        vals = [int(v) for v  in tup[0].split('-')]
        vals.reverse()
        vals.append(int(tup[1]))
        return vals

    loksabha_keys = [(i['Date'], i['Question']) for i in doc_infos if i['Question'] and i['House'] == 'LokSabha']
    loksabha_keys.sort(key=lambda tup: sortable(tup))
    loksabha_keys = {k:None for k in loksabha_keys}.keys()

    (export_dir/'orgpedia_key.txt').write_text('\n'.join(f'{d}| {q}' for (d, q) in loksabha_keys))


def export_metadata(video_infos, export_dir):
    print(video_infos[0].keys())

    fields = ['file_name', 'house', 'hour', 'date', 'url', 'title', 'video_id', 'publish_date', 'views', 'length_seconds', 'description', 'thumbnail_url', 'author', 'captions', 'keywords', 'age_restricted', 'list_file', 'referrer_page', 'name', 'question_type', 'session' ]

    with open((export_dir / 'meta_data.csv'), 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fields)
        csv_writer.writeheader()
        csv_writer.writerows(video_infos)

def main():
    doc_dir = Path(sys.argv[1])
    infos_dir = Path(sys.argv[2])
    export_dir = Path(sys.argv[3])

    video_infos = json.loads((infos_dir / 'videos.json').read_text())

    export_scenes(doc_dir, video_infos, export_dir)
    export_metadata(video_infos, export_dir)
main()


"""
  "url": "https://www.youtube.com/embed/RVyfwUSSDvk",
    "title": "Question Hour | 29 November, 2021 | 12:33 pm -12:37 pm",
    "video_id": "RVyfwUSSDvk",
    "publish_date": "2021-11-28 00:00:00",
    "views": 5893,
    "length_seconds": 340,
    "description": "#255RajyaSabhaSession #WinterSession #WinterSession2021\n\nFollow us on:\n-Twitter: https://twitter.com/sansad_tv\n-Insta: https://www.instagram.com/sansad.tv\n-FB: https://www.facebook.com/SansadTelevision/\n-Koo: https://www.kooapp.com/profile/Sansad_TV",
    "thumbnail_url": "https://i.ytimg.com/vi/RVyfwUSSDvk/sddefault.jpg?sqp=-oaymwEmCIAFEOAD8quKqQMa8AEB-AHUBoAC4AOKAgwIABABGH8gRygTMA8=&rs=AOn4CLA5evqW8xuW11xU3axdTVJhxUVSmg",
    "author": "Sansad TV",
    "captions": [
      "a.hi"
    ],
    "keywords": [
      "255th session of Rajya Sabha",
      "Rajya Sabha Session 255",
      "255th session of RS",
      "255th Parliament session",
      "255th RS session",
      "Bills & Acts",
      "Parliament",
      "Rajya Sabha Chairman",
      "RS Chairman",
      "zero hour",
      "question hour",
      "pending bills",
      "Rajya Sabha",
      "Rajya Sabha 255th session",
      "Lower House",
      "Parliament Winter Session",
      "Winter Session of Parliament",
      "Opposition party",
      "Ministry of Parliamentary Affairs",
      "Lok Sabha",
      "Rajya Sabha MP",
      "Live proceedings",
      "Sansad TV Lok Sabha",
      "LSTV",
      "Om Birla"
    ],
    "age_restricted": false,
    "house": "RajyaSabha",
    "question_type": "question_hour",
    "referrer_page": "https://sansadtv.nic.in/proceedings_type/rajya-sabha-question-hour/page/5",
    "session": 255,
    "name": "sansadQA-1.mp4",
    "file_name": "Question Hour  29 November 2021  1233 pm -1237 pm.mp4"
"""
