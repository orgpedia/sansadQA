import csv
import datetime
import json
import sys
from datetime import datetime


from pathlib import Path
import collections
from operator import itemgetter
from itertools import groupby
from more_itertools import flatten, first

IgnoreDocs = ['sansadQA-114.mp4', 'sansadQA-115.mp4', 'sansadQA-116.mp4', 'sansadQA-117.mp4', 'sansadQA-118.mp4',
              'sansadQA-119.mp4', 'sansadQA-125.mp4', 'sansadQA-136.mp4']
IgnoreDocs = []

KnownDocs = {"sansadQA-153.mp4.video.json", "sansadQA-166.mp4.video.json", "sansadQA-47.mp4.video.json",
             "sansadQA-52.mp4.video.json", "sansadQA-75.mp4.video.json", "sansadQA-82.mp4.video.json",
             "sansadQA-54.mp4.video.json", "sansadQA-52.mp4.video.json", "sansadQA-149.mp4.video.json",
             "sansadQA-73.mp4.video.json", "sansadQA-162.mp4.video.json", "sansadQA-84.mp4.video.json",
             "sansadQA-46.mp4.video.json", "sansadQA-41.mp4.video.json", "sansadQA-135.mp4.video.json",
             "sansadQA-51.mp4.video.json", "sansadQA-48.mp4.video.json", "sansadQA-23.mp4.video.json",
             "sansadQA-144.mp4.video.json", "sansadQA-60.mp4.video.json", "sansadQA-42.mp4.video.json",
             "sansadQA-35.mp4.video.json", "sansadQA-91.mp4.video.json"}

IgnoreDocs = []

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


# Returns the longest question hour pair
def infer_questions2(doc_name, number_times):
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
        return {}

    print(f'{doc_name} [{len(idx_nums)}] {idx_nums}')

    numbers = list(flatten([nums for (_, nums) in idx_nums]))
    numbers_cntr = collections.Counter(numbers)

    CountCutoff = 3 if len(idx_nums) > 10 else 0
    
    # only select question nums that occur more than CountCutoff
    frequent_question_nums = set(k for (k,v) in numbers_cntr.items() if v > CountCutoff)
    print(f'\t{doc_name} [{len(frequent_question_nums)}] {frequent_question_nums}')    

    #frequent_question_nums = sorted(frequent_question_nums)
    
    # Identify most common digits and only select those numbers
    
    num_digits_ctr = collections.Counter([num_digits(n) for n in numbers if n in frequent_question_nums])
    #print(frequent_question_nums)
    #print(num_digits_ctr)
    
    max_num_digits = max(num_digits_ctr, key=num_digits_ctr.get)
    print(f'\t{doc_name} max_num_digits: {max_num_digits}')        

    question_nums_set = set(n for n in frequent_question_nums if num_digits(n) == max_num_digits)

    question_times_list = []
    for idx, nums in idx_nums:
        q_num = first((n for n in nums if n in question_nums_set), default=None)
        if q_num:
            question_times_list.append((q_num, idx))

    question_time_pairs1 = {}
    for q_num, g in groupby(question_times_list, itemgetter(0)):
        g = list(g)
        start,end  = g[0][1], g[-1][1]
        question_time_pairs1.setdefault(q_num, []).append((start, end, len(g)) )
        #question_times_list2.append(q_num, start, end)
    #return question_times_list2

    question_time_pairs2 = {}
    for q_num, pair_lengths in question_time_pairs1.items():
        pair_lengths.sort(key=itemgetter(2), reverse=True)
        question_time_pairs2[q_num] = (pair_lengths[0][0], pair_lengths[0][1])

    print(f'{doc_name} [{len(question_time_pairs2)}] {question_time_pairs2.keys()}')
    return question_time_pairs2

def infer_questions3(doc_name, number_times):
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
        return {}

    print(f'{doc_name} [{len(idx_nums)}] {idx_nums}')

    numbers = list(flatten([nums for (_, nums) in idx_nums]))
    numbers_cntr = collections.Counter(numbers)

    CountCutoff = 3 if len(idx_nums) > 10 else 0
    
    # only select question nums that occur more than CountCutoff
    frequent_question_nums = set(k for (k,v) in numbers_cntr.items() if v > CountCutoff)
    print(f'\t{doc_name} [{len(frequent_question_nums)}] {frequent_question_nums}')    

    # Identify most common digits and only select those numbers
    num_digits_ctr = collections.Counter([num_digits(n) for n in numbers if n in frequent_question_nums])
    #print(frequent_question_nums)
    #print(num_digits_ctr)
    
    max_num_digits = max(num_digits_ctr, key=num_digits_ctr.get)
    print(f'\t{doc_name} max_num_digits: {max_num_digits}')        

    question_nums_set = set(n for n in frequent_question_nums if num_digits(n) == max_num_digits)

    question_times_list = []
    for idx, nums in idx_nums:
        q_num = first((n for n in nums if n in question_nums_set), default=None)
        if q_num:
            question_times_list.append((q_num, idx))

    #
    question_times_dict = {}
    [question_times_dict.setdefault(q_num, []).append(idx) for (q_num, idx) in question_times_list]

    question_times_starts = []
    for q_num, idxs in sorted(question_times_dict.items()):
        start = idxs.sort()[0]
        question_times_starts.append(q_num, start)

    question_time_pairs, last_q, last_start = {}
    for ((q1, s1), (q2, s2)) in pairwise(question_times_starts):
        question_time_pairs[q1] = (s1, s2)
        last_q, last_start = q2, s2

    question_time_pairs[last_q] = (last_start, -1)
    return question_time_pairs

def export_todo(doc_dir, video_infos, prs_ls, prs_rs, export_dir):
    def get_url(info, s, e):
        if s is None or e is None:
            return ''
        
        s = max(int(s)-2, 0)
        e = int(e) + 2
        return f"https://www.youtube.com/embed/{info['video_id']}?start={s}&end={e}"
    
    def get_file_num(file_path):
        (stub, num) = file_path.stem.replace('.mp4.video', '').split('-')
        return int(num)

    def get_question(s):
        (idx, q) = s.split(':')
        idx = int(idx.strip())
        q = int(q) if q.strip() else None
        return (q, idx)

    def get_date_str(old_str):
        parsed_date = datetime.strptime(old_str, '%d-%m-%Y')
        return parsed_date.strftime('%d-%b-%Y')
    
    
    doc_infos = []
    
    doc_files = doc_dir.glob('*.video.json')
    doc_files = sorted(doc_files, key=get_file_num)

    for doc_file in doc_files:
        print('##' + doc_file.name.replace('.video.json', ''))
        doc = json.loads(doc_file.read_text())
        video_file_name = doc_file.name.replace('.video.json', '')
        info = [i for i in video_infos if i['name'] == video_file_name][0]

        q_time_pairs = infer_questions2(doc_file.name, doc['question_times'])

        # if q_times and q_times[0] == ('-1', None):
        #     q_times.pop()

        # # to ensure that last question is captured
        # if 'scene_intervals' in doc:
        #     q_times.append((0, doc['scene_intervals'][-1][1]))
        # else:
        #     q_times.append((0, info['length_seconds']))
        
        # q_time_pairs = dict((q1[0], (q1[1], q2[1])) for (q1, q2) in pairwise(q_times) if q2)

        # if len(q_times) > 1:
        #     print(f'{video_file_name} Building q_time_pairs {q_times}')
        #     last_q, last_s = q_times[-1][0], q_times[-1][1]
        #     if 'scene_talkers' in doc:
        #         last_e = doc['scene_talkers'][-1][0][1] + 1
        #     else:
        #         last_e = int(doc['info']['length_seconds'])
        #     q_time_pairs[last_q] = (last_s, last_e)


        video_date = doc['date_str']
        video_date = video_date.replace('/', '.')
        
        if '.' in video_date:
            d, m, y = video_date.split('.')
        else:
            print(f'Date not found {video_file_name}')
            d, m, y = '', '', ''
        video_date_str = f'{d}-{m}-20{y}'

        
        # if doc_file.name == 'sansadQA-52.mp4.video.json':
        #     import pdb
        #     pdb.set_trace()
        
        prs_house = prs_ls if info['house'] == 'LokSabha' else prs_rs

        prs_questions = [int(q) for q in prs_house.get(video_date_str, [])]
        org_questions = [int(q) for q in q_time_pairs.keys()]

        all_questions = sorted(set(prs_questions + org_questions))

        only_doc_infos, num_errors, prev_idx = [], 0, 0
        for q_num in all_questions:
            if q_num == 0:
                continue
            
            if q_num in prs_questions and q_num in org_questions:
                status = 'matched'
                s, e = q_time_pairs[q_num]
            elif q_num in org_questions:
                num_errors += 1
                status = 'extra'
                s, e = q_time_pairs[q_num]
            elif q_num in prs_questions:
                num_errors += 1                
                status = 'missing'
                s, e = None, None
            else:
                assert False

            doc_info = {'Name': video_file_name, 'Title': info['title'], 'House': info['house'],
                        'Question': q_num, 'QuestionStart': get_time_str(s),
                        'QuestionEnd': get_time_str(e), 'URL': get_url(info, s, e),
                        'Date': get_date_str(video_date_str), 'Status': status, 'ErrorPercent': 0,
                        'StartIdx': s if s is not None else prev_idx}

            if s is not None:
                prev_idx = s
                
            only_doc_infos.append(doc_info)
            only_doc_infos.sort(key=itemgetter('StartIdx'))


        if num_errors > 0:
            err_percent = int((num_errors * 100)/len(only_doc_infos))
            for doc_info in only_doc_infos:
                doc_info['ErrorPercent'] = int((num_errors * 100)/len(only_doc_infos))
        else:
            err_percent = 0

        known = '*' if doc_file.name in KnownDocs else ''
        print(f'{doc_file.name}{known}: {video_date_str} {info["house"]} errors: {num_errors} of {len(only_doc_infos)} {err_percent}%')
        doc_infos.extend(only_doc_infos)
        
    #end

    fields = ['Name', 'Title', 'Date', 'House',  'ErrorPercent', 'Question', 'Status', 'QuestionStart', 'QuestionEnd', 'StartIdx', 'URL']
    with open((export_dir / 'question_hour_todo.csv'), 'w') as csv_file:
        csv_writer = csv.DictWriter(csv_file, fieldnames=fields)
        csv_writer.writeheader()
        csv_writer.writerows(doc_infos)
        #csv_writer.writerows([i for i in doc_infos if i['House'] == 'LokSabha'])


    # with open((export_dir / 'question_hour_rajyasabha_todo.csv'), 'w') as csv_file:
    #     csv_writer = csv.DictWriter(csv_file, fieldnames=fields)
    #     csv_writer.writeheader()
    #     csv_writer.writerows([i for i in doc_infos if i['House'] == 'RajyaSabha'])        
            


def parse_prs(export_dir):
    def dt(dt_str):
        y, m, d = dt_str.split('-')
        return f'{d}-{m}-{y}'
    
    ls_dict, rs_dict = {}, {}
    ls_file = export_dir / 'ls.csv'
    if ls_file.exists():
        with open(ls_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader) # remove header            
            for row in csv_reader:
                swapped_date = dt(row[0])
                ls_dict.setdefault(swapped_date,[]).append(row[1].strip('"'))
        #ls_questions = [ln.strip().split('| ') for ln in ls_file.read_text().split('\n') if ln]
        #ls_dict = {k:[t[1] for t in g] for (k, g) in groupby(ls_questions, key=itemgetter(0)) }
        
    rs_file = export_dir / 'rs.csv'
    if rs_file.exists():
        with open(rs_file) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            next(csv_reader) # remove header            
            for row in csv_reader:
                swapped_date = dt(row[0])
                rs_dict.setdefault(swapped_date,[]).append(row[1].strip('"'))
    return ls_dict, rs_dict

def main():
    doc_dir = Path(sys.argv[1])
    infos_dir = Path(sys.argv[2])
    export_dir = Path(sys.argv[3])


    video_infos = json.loads((infos_dir / 'videos.json').read_text())
    prs_ls, prs_rs = parse_prs(export_dir)
    export_todo(doc_dir, video_infos, prs_ls, prs_rs, export_dir)


main()
