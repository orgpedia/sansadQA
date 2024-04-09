import sys
from pathlib import Path

from docint.ppln import Pipeline

# import extract_question
# import extract_question2
# import infer_question

import find_talker
import find_question
import find_date
import add_info

def order_num(file_path):
    org_code, num = file_path.stem.rsplit("-", 1)
    return int(num)


if __name__ == "__main__":
    input_path = Path(sys.argv[1])
    output_path = Path(sys.argv[2])

    ppln = Pipeline.from_file("src/extractQuestion.yml")

    if input_path.is_dir():
        assert output_path.is_dir()
        input_files = sorted(input_path.glob("*.mp4"), key=order_num)
        print(len(input_files))

        # docs = viz.pipe_all(input_files[:3])
        docs = ppln(input_files)

        for doc in docs:
            output_doc_path = output_path / (doc.get_file_name() + ".video.json")
            doc.to_disk(output_doc_path)
    elif input_path.suffix.lower() in (".mp4", ".webm"):
        doc = ppln(input_path)
        doc.to_disk(output_path)        
        # if doc:
        #     doc.to_disk(output_path)

    elif input_path.suffix.lower() in (".list", ".lst"):
        print("processing list")
        
        input_file_names = input_path.read_text().split("\n")

        input_files = [Path("input") / f for f in input_file_names if f and f[0] != "#"]
        input_files = [p for p in input_files if p.exists()]

        docs = ppln(input_files)
        for doc in docs:
            output_doc_path = output_path / (doc.get_file_name() + ".video.json")
            doc.to_disk(output_doc_path)

