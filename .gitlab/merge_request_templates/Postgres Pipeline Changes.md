Closes

#### List and Describe Code Changes <!-- focus on why the changes are being made-->

* `change & why it was made`

#### Steps Taken to Test

* _action items_

## Submitter Checklist

* [ ] Does `Extract` -> [pgp_test](https://about.gitlab.com/handbook/business-technology/data-team/platform/ci-jobs/#pgp_test) pipeline passed properly?
* [ ] Any `huge` table is modified? If yes, please refer to the page [Large table backfilling](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/#large-tables-backfilling) and follow the steps
* [ ] Check is any `RED` data in your changes, they shouldn't be extracted into Data Warehouse. For more details about Data Classification refer to [Data Classification Standard](https://about.gitlab.com/handbook/security/data-classification-standard.html) page

## All MRs Checklist
- [ ] [Label hygiene](https://about.gitlab.com/handbook/business-ops/data-team/how-we-work/#issue-labeling) on issue.
- [ ] Branch set to delete. (Leave option `Squash commits when merge request is accepted.` unchecked)
- [ ] This MR is ready for final review and merge.
- [ ] All threads are resolved.
- [ ] Remove the `Draft:` prefix in the MR title before assigning to reviewer (or type command `/ready` in the comment)
- [ ] Assigned to reviewer.

## Reviewer Checklist
- [ ]  Check before setting to merge


## Further changes requested
* [ ] **AUTHOR**: Uncheck all boxes before taking further action.
* [ ] If any of `huge` table is modified? If yes, please refer to the page [Large table backfilling](https://about.gitlab.com/handbook/business-technology/data-team/platform/pipelines/#large-tables-backfilling) and follow the steps to deploy MR avoiding work days

