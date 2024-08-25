PROJECT_NAME = ascii_hydra

ASCII_SOPS_AGE_PUBLIC_KEY=age1fnvypsjn6gm4kavr8hk6f6ejxsmulvyw3jm0wjzqrwj7hkhd0dsqhw3e6y

CWD := $(shell pwd)

ifeq (,$(shell which conda))
HAS_CONDA=False
else
HAS_CONDA=True
endif

# Need to specify bash in order for conda activate to work.
SHELL=/bin/zsh
# Note that the extra activate is needed to ensure that the activate floats env to the front of PATH
CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate
# https://stackoverflow.com/questions/53382383/makefile-cant-use-conda-activate

## Set up python interpreter environment
create_environment: ## create conda environment for analysis
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
		mamba env create --name $(PROJECT_NAME) -f ./src/environment.yml

		$(MAKE) setup_packages

		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
		
endif


## Set up local packages and DBT
setup_packages: 
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; npm install -g mapshaper)

## Set up python interpreter environment OSX
create_environment_osx: ## create conda environment for analysis
ifeq (True,$(HAS_CONDA))
		@echo ">>> Detected conda, creating conda environment."
		CONDA_SUBDIR=osx-64 mamba env create --name $(PROJECT_NAME) -f ./src/environment.yml

		$(MAKE) setup_packages

		@echo ">>> New conda env created. Activate with:\nsource activate $(PROJECT_NAME)"
endif

delete_environment: ## delete conda environment for analysis
	conda remove --name $(PROJECT_NAME) --all -y

.PHONY: fmt
fmt:
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; black src && isort --profile black src && yamllint -c yamllintconfig.yaml .)


.PHONY: lint lint-fix lint-fix-unsafe
lint:
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; ruff check src && mypy --explicit-package-bases src/pipeline_example)
lint-fix:
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; ruff check src --fix && mypy --explicit-package-bases src/pipeline_example)
lint-fix-unsafe:
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; ruff check src --fix --unsafe-fixes && mypy --explicit-package-bases src/pipeline_example)

start-dagster-webserver:
	($(CONDA_ACTIVATE) "${PROJECT_NAME}" ; cd src/pipeline_example && dagster dev -m pipeline_example -d .)

#################################################################################
# PROJECT RULES                                                                 #
#################################################################################
#################################################################################
# Self Documenting Commands                                                     #
#################################################################################
.DEFAULT_GOAL := help
# Inspired by <http://marmelab.com/blog/2016/02/29/auto-documented-makefile.html>
# sed script explained:
# /^##/:
# 	* save line in hold space
# 	* purge line
# 	* Loop:
# 		* append newline + line to hold space
# 		* go to next line
# 		* if line starts with doc comment, strip comment character off and loop
# 	* remove target prerequisites
# 	* append hold space (+ newline) to line
# 	* replace newline plus comments by `---`
# 	* print line
# Separate expressions are necessary because labels cannot be delimited by
# semicolon; see <http://stackoverflow.com/a/11799865/1968>
.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
