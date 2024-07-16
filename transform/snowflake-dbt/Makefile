
# General Variables
DBT_REFERENCE= "curl https://dbt.gitlabdata.com/manifest.json -o reference_state/manifest.json"
GIT_BRANCH_BASE = $$(git merge-base --fork-point origin/master)
DBT_DEV_STATE=--state reference_state

## Command Variables
DBT_DEV_CHANGES_DOWNSTREAM=$$({ git diff --name-only $(GIT_BRANCH_BASE) ; git ls-files --other --exclude-standard ; } | xargs -n1 basename | grep '\.sql\|\.csv' | sed -E "s/(.sql|.csv)/+/g" | tr '\n' ',')
DBT_DEV_CHANGES_UPSTREAM=$$({ git diff --name-only $(GIT_BRANCH_BASE) ; git ls-files --other --exclude-standard ; } | xargs -n1 basename | grep '\.sql\|\.csv' | sed "s/^/+/g" | sed -E "s/(.sql|.csv)//g" | tr '\n' ',')
DBT_CONTIG_VARS=$$(echo "\"TEST_LIST\":\"$(DBT_DEV_CHANGES_DOWNSTREAM)\",\"TEST_LIST_A\":\"$(DBT_DEV_CHANGES_UPSTREAM)\"")
DBT_DEV_CHANGES=$$({ git diff --name-only $(GIT_BRANCH_BASE) ; git ls-files --other --exclude-standard ; } | xargs -n1 basename | grep '\.sql\|\.csv'  | sed -E "s/(.sql|.csv)/$(DOWNSTREAM)/g")

DBT_DEV_CONTIG=$$(if [ -z "$(CONTIGUOUS)" ]; then echo "True"; fi)
DBT_DEV_SELECTION=$$(if [ ! -z "$(SELECTION)" ] ; then echo "--select $(SELECTION)"; elif [ ! -z "$(CONTIGUOUS)" ] ; then echo "--select $(DBT_DEV_CHANGES)"; else echo "--selector contiguous_list"; fi)
DBT_DEV_FAIL_FAST= $$(if [ -z "$(FAIL_FAST)" ]; then echo "--fail-fast"; fi)
DBT_DEV_EXCLUDE= $$(if [ ! -z "$(EXCLUDE)" ]; then echo "--exclude $(EXCLUDE)"; fi)
DBT_DEV_FULL_REFRESH=$$(if [ ! -z "$(FULL_REFRESH)" ]; then echo "--full-refresh"; fi)
DBT_DEV_VARS= $$(if [ ! -z "$(VARS)" ] && [ -z "$(CONTIGUOUS)" ]; then echo "--vars {$(VARS),$(DBT_CONTIG_VARS)}" ; elif [ -z "$(CONTIGUOUS)" ]; then echo "--vars {$(DBT_CONTIG_VARS)}"; elif [ ! -z "$(VARS)" ]; then echo "--vars {$(VARS)}"; fi)
DBT_DEV_TARGET= $$(if [ ! -z "$(PROFILE_TARGET)" ] ; then echo "$(PROFILE_TARGET)"; elif [ ! -z "$(TARGET)" ] ; then echo "--target $(TARGET)"; fi )

SQLFLUFF_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | cut -d'/' -f3- | grep '\.sql\b')
SQLFLUFF_DEV_MODEL_PATH=$$(find . -name "$(MODEL).sql")
SQLFLUFF_DEV_MODELS=$$(if [ -z "$(MODEL)" ]; then echo $(SQLFLUFF_DEV_CHANGES); else echo $(SQLFLUFF_DEV_MODEL_PATH); fi)
SQLFLUFF_DEV_COMMAND=$$(if [ -z "$(FIX)" ]; then echo "lint"; else echo "fix"; fi)

## Commands
DBT_DEV_BUILD_CHANGES_CLONE_COMMAND= dbt $(DBT_DEV_FAIL_FAST) clone $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) $(DBT_DEV_STATE)
DBT_DEV_BUILD_CHANGES_BUILD_COMMAND= dbt $(DBT_DEV_FAIL_FAST) build $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) --defer $(DBT_DEV_STATE)  $(DBT_DEV_VARS)

#DBT_CONTIG_VARS=$$(echo "--vars {\"TEST_LIST\":\"$(DBT_DEV_CHANGES_DOWNSTREAM)\",\"TEST_LIST_A\":\"$(DBT_DEV_CHANGES_UPSTREAM)\"}")
DBT_DEV_BUILD_CONTIG_CHANGES_BUILD_COMMAND=dbt list --selector refresh_list --resource-type model $(DBT_CONTIG_VARS)

DBT_DEV_SAFE_CHECK_COMMAND= dbt --quiet list --selector safe_models --resource-type model --output json > safe_models.json 

SQLFLUFF_COMMAND=sqlfluff $(SQLFLUFF_DEV_COMMAND) $(SQLFLUFF_DEV_MODELS)



help:
	@echo "\n \
	------------------------------------------------------------ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
	++ dbt Related ++ \n \
	++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ \n \
    - build-changes: will perform a clone and build of all of the changes sql and csv files accepts the following variables: \n \
		 - DOWNSTREAM - dbt graph operator for including downstream models to the changes \n \
		 - SELECTION - overrides the list of changed models for the provided dbt selection \n \
		 - FAIL_FAST - defaults to true, providing any value will set to false \n \
		 - EXCLUDE - a dbt selection of models to not clone or build \n \
		 - FULL_REFRESH - defaults to false, providing any value will set to true \n \
		 - VARS - list of key value pairs to pass to dbt, must be in the format \"key\":\"value\" \n \
		 - TARGET - a dbt target to use that is not the default \n \
		 - OVERRIDE_REFERENCE - defaults to false, providing any value will cause the reference state to not be downloaded \n \
		 - PROFILE_TARGET - a combination of profile director and target, used in the CI jobs \n \
    - lint-models: will lint or fix all of the changed sql files or the provided model \n \
     - MODEL: overrides the list of changed files for the provided file name \n \
     - FIX: defaults to false, providing any value will set to true and make possible changes to files \n \
	 \n \
	------------------------------------------------------------"

build-changes:
	@if [ -z "$(OVERRIDE_REFERENCE)" ]; then (echo "\n***** Fetching Reference *******\n"; "$(DBT_REFERENCE)";) fi;

	@echo "\n***** User Input *******\n"
	@echo User Selection: $(SELECTION)
	@echo User Fail Fast: $(FAIL_FAST)
	@echo User Exclude: $(EXCLUDE)
	@echo User Full Refresh: $(FULL_REFRESH)
	@echo User Vars: $(VARS)
	@echo User Target: $(TARGET)
	@echo User Downstream: $(DOWNSTREAM)

	@echo "\n***** Resultant Values *******\n"
	@echo Selection: $(DBT_DEV_SELECTION)
	@echo Fail Fast: $(DBT_DEV_FAIL_FAST)
	@echo Exclude: $(DBT_DEV_EXCLUDE)
	@echo Full Refresh: $(DBT_DEV_FULL_REFRESH)
	@echo Vars: $(DBT_DEV_VARS)
	@echo Target: $(DBT_DEV_TARGET)

	@echo "\n***** Cloning Selection *******\n"
	@echo $(DBT_DEV_BUILD_CHANGES_CLONE_COMMAND)
	@$(DBT_DEV_BUILD_CHANGES_CLONE_COMMAND) || (echo "Error in Clone"; exit 1)
	@echo "\n***** Building Selection *******\n"
	@echo $(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) 
	@$(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) || (echo "Error in Build"; exit 1)

lint-models:
	@echo $(SQLFLUFF_COMMAND)
	@$(SQLFLUFF_COMMAND)

test-safe:
	@echo $(DBT_DEV_SAFE_CHECK_COMMAND)
	$(DBT_DEV_SAFE_CHECK_COMMAND)
	@python3 safe_model_check.py

build-contig:
	@echo $(DBT_DEV_CHANGES_DOWNSTREAM)
	@echo $(DBT_DEV_CHANGES_UPSTREAM)
	@echo $(DBT_DEV_BUILD_CONTIG_CHANGES_BUILD_COMMAND)
	@$(DBT_DEV_BUILD_CONTIG_CHANGES_BUILD_COMMAND)