# introduced in make3.81
ROOT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
ROOT_DIR := $(strip $(ROOT_DIR))

export GOOGLE_APPLICATION_CREDENTIALS = $(ROOT_DIR)/../../.secrets/google.token
export PYTHONPATH := $(ROOT_DIR)/../src:$(PYTHONPATH)

.PHONY: all

all: 
	poetry run python src/extractQuestion.py input output | tee  logs/info.log

fuzzy:
	poetry run python -u src/extractQuestion.py input/fuzzy.list output/ | tee logs/fuzzy.log


%:
	poetry run python src/extractQuestion.py input/sansadQA-$@.mp4 output/sansadQA-$@.mp4.video.json



