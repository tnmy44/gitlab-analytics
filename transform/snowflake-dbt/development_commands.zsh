export DBT_REFERENCE='curl https://dbt.gitlabdata.com/manifest.json -o reference_state/manifest.json'
export GIT_BRANCH_BASE=$(git merge-base --fork-point origin/master)
export DBT_DEV_STATE='--state reference_state'

dbt_build_changes() {
  ## Input
  # "" "False" "test_model" "True" "key":"value" "dev_xl" "+"
  # SELECTION=$1
  # FAIL_FAST=$2
  # EXCLUDE=$3
  # FULL_REFRESH=$4
  # VARS=$5
  # TARGET=$6
  # DOWNSTREAM=$7

  ## Command Varialbs
  export DBT_DEV_CHANGES=$(git diff --name-only $GIT_BRANCH_BASE | xargs -n1 basename | grep '\.sql\|\.csv'  | sed -E "s/(.sql|.csv)/$7/g")
  export DBT_DEV_SELECTION=$(if [ -z "$1" ]; then echo $DBT_DEV_CHANGES; else echo $1; fi)
  export DBT_DEV_FAIL_FAST=$(if [ -z "$2" ]; then echo "--fail-fast"; fi)
  export DBT_DEV_EXCLUDE=$(if [ ! -z "$3" ]; then echo "--exclude $3"; fi)
  export DBT_DEV_FULL_REFRESH=$(if [ ! -z "$4" ]; then echo "--full-refresh"; fi)
  export DBT_DEV_VARS=$(if [ ! -z "$5" ]; then echo "--vars {$5}"; fi)
  export DBT_DEV_TARGET=$(if [ ! -z "$6" ]; then echo "--target $6"; fi)

  ## Commands
  export DBT_DEV_BUILD_CHANGES_CLONE_COMMAND="dbt $DBT_DEV_FAIL_FAST clone --select $DBT_DEV_SELECTION $DBT_DEV_EXCLUDE $DBT_DEV_TARGET $DBT_DEV_FULL_REFRESH $DBT_DEV_STATE"
  export DBT_DEV_BUILD_CHANGES_BUILD_COMMAND="dbt $DBT_DEV_FAIL_FAST build --select $DBT_DEV_SELECTION $DBT_DEV_EXCLUDE $DBT_DEV_TARGET $DBT_DEV_FULL_REFRESH --defer $DBT_DEV_STATE  $DBT_DEV_VARS"


  echo "\n***** Fetching Reference *******\n"
  #eval $DBT_REFERENCE

  echo "\n***** User Input *******\n"
  echo User Selection: $SELECTION
  echo User Fail Fast: $FAIL_FAST
  echo User Exclude: $EXCLUDE
  echo User Full Refresh: $FULL_REFRESH
  echo User Vars: $VARS
  echo User Target: $TARGET
  echo User Downstream: $DOWNSTREAM

  echo "\n***** Resultant Values *******\n"
  echo Selection: $DBT_DEV_SELECTION
  echo Fail Fast: $DBT_DEV_FAIL_FAST
  echo Exclude: $DBT_DEV_EXCLUDE
  echo Full Refresh: $DBT_DEV_FULL_REFRESH
  echo Vars: $DBT_DEV_VARS
  echo Target: $DBT_DEV_TARGET

  echo "\n***** Cloning Selection *******\n"
  echo $DBT_DEV_BUILD_CHANGES_CLONE_COMMAND
  #eval $DBT_DEV_BUILD_CHANGES_CLONE_COMMAND
  echo "\n***** Building Selection *******\n"
  echo $DBT_DEV_BUILD_CHANGES_BUILD_COMMAND 
  #eval $DBT_DEV_BUILD_CHANGES_BUILD_COMMAND

  ## Clear Inputs
  #export SELECTION=""
  #export FAIL_FAST=""
  #export EXCLUDE=""
  #export FULL_REFRESH=""
  #export VARS=""
  #export TARGET=""
  #export DOWNSTREAM=""
}

sqlfluff_lint_changes() {
   export SQLFLUFF_DEV_CHANGES=$(git diff --name-only $GIT_BRANCH_BASE | cut -d'/' -f3- | grep '\.sql') # | xargs -n1 basename | grep '\.sql' | sed -E "s/(.sql)//g"
   echo $SQLFLUFF_DEV_CHANGES
   sqlfluff lint $SQLFLUFF_DEV_CHANGES
}