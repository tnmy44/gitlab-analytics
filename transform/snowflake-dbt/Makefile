
# General Variables
DBT_REFERENCE= "curl https://dbt.gitlabdata.com/manifest.json -o reference_state/manifest.json"
GIT_BRANCH_BASE = $$(git merge-base --fork-point origin/master)
DBT_DEV_STATE=--state reference_state

## Command Varialbs
DBT_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | xargs -n1 basename | grep '\.sql\|\.csv'  | sed -E "s/(.sql|.csv)/$(DOWNSTREAM)/g")
DBT_DEV_SELECTION=$$(if [ -z "$(SELECTION)" ]; then echo $(DBT_DEV_CHANGES); else echo $(SELECTION); fi)
DBT_DEV_FAIL_FAST= $$(if [ -z "$(FAIL_FAST)" ]; then echo "--fail-fast"; fi)
DBT_DEV_EXCLUDE= $$(if [ ! -z "$(EXCLUDE)" ]; then echo "--exclude $(EXCLUDE)"; fi)
DBT_DEV_FULL_REFRESH=$$(if [ ! -z "$(FULL_REFRESH)" ]; then echo "--full-refresh"; fi)
DBT_DEV_VARS= $$(if [ ! -z "$(VARS)" ]; then echo "--vars {$(VARS)}"; fi)
DBT_DEV_TARGET= $$(if [ ! -z "$(PROFILE_TARGET)" ] ; then echo "$(PROFILE_TARGET)"; elif [ ! -z "$(TARGET)" ] ; then echo "--target $(TARGET)"; fi )

SQLFLUFF_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | cut -d'/' -f3- | grep '\.sql')
SQLFLUFF_DEV_MODEL_PATH=$$(find . -name "$(MODEL).sql")

## Commands
DBT_DEV_BUILD_CHANGES_CLONE_COMMAND= dbt $(DBT_DEV_FAIL_FAST) clone --select $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) $(DBT_DEV_STATE)
DBT_DEV_BUILD_CHANGES_BUILD_COMMAND= dbt $(DBT_DEV_FAIL_FAST) build --select $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) --defer $(DBT_DEV_STATE)  $(DBT_DEV_VARS)

DBT_DEV_BUILD_CHANGES_BUILD_SELECTOR_COMMAND= dbt $(DBT_DEV_FAIL_FAST) list --selector in_between_list # $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) --defer $(DBT_DEV_STATE)  $(DBT_DEV_VARS)


SQLFLUFF_LINT_CHANGES_COMMAND=sqlfluff lint $(SQLFLUFF_DEV_CHANGES)
SQLFLUFF_FIX_CHANGES_COMMAND=sqlfluff fix $(SQLFLUFF_DEV_CHANGES)
SQLFLUFF_LINT_MODEL_COMMAND=sqlfluff lint $(SQLFLUFF_DEV_MODEL_PATH)
SQLFLUFF_FIX_MODEL_COMMAND=sqlfluff fix $(SQLFLUFF_DEV_MODEL_PATH)


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
    - lint-changes: will lint all of the changed sql files \n \
    - lint-fix-changes: will correct available lint errors on all changes sql files \n \
    - lint-model: will lint the dbt model provided with the MODEL variable \n \
    - lint-fix-model: will correct available lint errors in the dbt model provided with the MODEL variable \n \
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
	@$(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) 

lint-changes:
	@echo $(SQLFLUFF_LINT_CHANGES_COMMAND)
	@$(SQLFLUFF_LINT_CHANGES_COMMAND)

lint-fix-changes:
	@echo $(SQLFLUFF_FIX_CHANGES_COMMAND)
	@$(SQLFLUFF_FIX_CHANGES_COMMAND)

lint-model:
	@echo $(SQLFLUFF_LINT_MODEL_COMMAND)
	@$(SQLFLUFF_LINT_MODEL_COMMAND)

lint-fix-model:
	@echo $(SQLFLUFF_FIX_MODEL_COMMAND)
	@$(SQLFLUFF_FIX_MODEL_COMMAND)

test-selector:
	@echo python -c "import development_commands; development_commands.space_separated_to_yaml($(DBT_DEV_SELECTION))"
	@python -c "import development_commands; development_commands.space_separated_to_yaml('$(DBT_DEV_SELECTION)')" 
	@$(DBT_DEV_BUILD_CHANGES_BUILD_SELECTOR_COMMAND)