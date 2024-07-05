
# General Variables
DBT_REFERENCE= "curl https://dbt.gitlabdata.com/manifest.json -o reference_state/manifest.json"
GIT_BRANCH_BASE = $$(git merge-base --fork-point origin/master)
DBT_DEV_STATE=--state reference_state

## Input Variables
SELECTION=""
FAIL_FAST=""
EXCLUDE=""
FULL_REFRESH=""
VARS=""
TARGET=""
DOWNSTREAM=""

## Command Varialbs
DBT_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | xargs -n1 basename | grep '\.sql\|\.csv'  | sed -E "s/(.sql|.csv)/$(DOWNSTREAM)/g")
DBT_DEV_SELECTION=$$(if [ -z "$(SELECTION)" ]; then echo $(DBT_DEV_CHANGES); else echo $(SELECTION); fi)
DBT_DEV_FAIL_FAST= $$(if [ -z "$(FAIL_FAST)" ]; then echo "--fail-fast"; fi)
DBT_DEV_EXCLUDE= $$(if [ ! -z "$(EXCLUDE)" ]; then echo "--exclude $(EXCLUDE)"; fi)
DBT_DEV_FULL_REFRESH=$$(if [ ! -z "$(FULL_REFRESH)" ]; then echo "--full-refresh"; fi)
DBT_DEV_VARS= $$(if [ ! -z "$(VARS)" ]; then echo "--vars {$(VARS)}"; fi)
DBT_DEV_TARGET= $$(if [ ! -z "$(TARGET)" ]; then echo "--target $(TARGET)"; fi)

SQLFLUFF_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | cut -d'/' -f3- | grep '\.sql')

## Commands
DBT_DEV_BUILD_CHANGES_CLONE_COMMAND= dbt $(DBT_DEV_FAIL_FAST) clone --select $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) $(DBT_DEV_STATE)
DBT_DEV_BUILD_CHANGES_BUILD_COMMAND= dbt $(DBT_DEV_FAIL_FAST) build --select $(DBT_DEV_SELECTION) $(DBT_DEV_EXCLUDE) $(DBT_DEV_TARGET) $(DBT_DEV_FULL_REFRESH) --defer $(DBT_DEV_STATE)  $(DBT_DEV_VARS)

SQLFLUFF_LINT_CHANGES_COMMAND=sqlfluff lint $(SQLFLUFF_DEV_CHANGES)
SQLFLUFF_FIX_CHANGES_COMMAND=sqlfluff fix $(SQLFLUFF_DEV_CHANGES)

build-changes-make:
	@echo "\n***** Fetching Reference *******\n"
	@"$(DBT_REFERENCE)"

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
  #@$(DBT_DEV_BUILD_CHANGES_CLONE_COMMAND)
	@echo "\n***** Building Selection *******\n"
	@echo $(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) 
  #@$(DBT_DEV_BUILD_CHANGES_BUILD_COMMAND) 

build-changes:
	@. ./development_commands.zsh; dbt_build_changes $(SELECTION) $(FAIL_FAST) $(EXCLUDE) $(FULL_REFRESH) $(VARS) $(TARGET) $(DOWNSTREAM)

lint-changes:
  #@. ./development_commands.zsh; sqlfluff_lint_changes
  #SQLFLUFF_DEV_CHANGES=$$(git diff --name-only $(GIT_BRANCH_BASE) | cut -d'/' -f3- | grep '\.sql')
  #SQLFLUFF_LINT_COMMAND=sqlfluff lint $(SQLFLUFF_DEV_CHANGES)
	@echo $(SQLFLUFF_LINT_CHANGES_COMMAND)
  #@$(SQLFLUFF_LINT_CHANGES_COMMAND)

lint-fix-changes:
  #@. ./development_commands.zsh; sqlfluff_lint_changes
	@echo $(SQLFLUFF_FIX_CHANGES_COMMAND)
  #@$(SQLFLUFF_FIX_CHANGES_COMMAND)
