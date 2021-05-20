# ONIX workflow

The ONIX workflow uses the ONIX table created by the ONIX telescope to do the following:
  1. Aggregate book product records into works records. Works are equivalence classes of products, where each product in the class is a manifestation of each other. For example, a PDF and a paperback of the same work.
  2. Aggregate work records into work family records. A work family is an equivalence class of works where each work in the class is just a different edition.
  3. Produce intermediate lookup tables mapping ISBN13 -> WorkID and ISBN13 -> WorkFamilyID.
  4. Produce oaebu_intermediate tables that append work_id and work_family_id columns to different data tables with ISBN keys.

## Dependencies
The ONIX workflow is dependent on the ONIX telescope.  It waits for the ONIX telescope to finish before it starts executing.  This requires an ONIX telescope to be present and scheduled.

## Work ID
The Work ID will be an arbitrary ISBN representative from a product in the equivalence class.

## Work Family ID
The Work Family ID will be an arbitrary Work ID (ISBN) representative from a work in the equivalence class.

## Create OAEBU intermediate tables
For each data partner's tables containing ISBN, create new "matched" tables which extend the original data with new "work_id" and "work_family_id" columns.

## Create QA tables
For each data source, including the intermediate tables, we can produce some basic quality assurance checks on the data, and output these to tables for easy export. For example we can check to see if ISBNs provided are valid, or if there are unmatched ISBN indicating missing ONIX product records.