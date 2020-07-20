# Unpaywall
Unpaywall is an “open database of free scholarly articles.” It includes “data from open indexes like Crossref 
and DOAJ where it exists.” Data comes from “monitoring over 50,000 unique online content hosting locations, 
including Gold OA journals, Hybrid journals, institutional repositories, and disciplinary repositories.” 
“Unpaywall assigns an OA Status to every article”. “There are five possible values: closed, green, gold, 
hybrid, and bronze.
” _- source: [Unpaywall](https://unpaywall.org/)_ 
and [data details](https://unpaywall.org/data-format)

---

**Schema**

+ **journal_is_in_doaj** [*Boolean*]
+ **published_date** [*Date*]
+ **journal_issns** [*String*]
+ **journal_issn_l** [*String*]
+ **issn_l** [*String*]
+ **data_standard** [*Integer*]
+ **x_reported_noncompliant_copies** [*Record*]
    + **blank** [*String*]
+ **oa_locations** [*Record*]
    + **pmh_id** [*String*]
    + **url** [*String*]
    + **license** [*String*]
    + **url_for_landing_page** [*String*]
    + **version** [*String*]
    + **url_for_pdf** [*String*]
    + **is_best** [*Boolean*]
    + **updated** [*Timestamp*]
    + **evidence** [*String*]
    + **host_type** [*String*]
    + **repository_institution** [*String*]
    + **endpoint_id** [*String*]
    + **id** [*String*]
+ **doi** [*String*]
+ **best_oa_location** [*Record*]
    + **pmh_id** [*String*]
    + **url** [*String*]
    + **license** [*String*]
    + **url_for_landing_page** [*String*]
    + **version** [*String*]
    + **url_for_pdf** [*String*]
    + **is_best** [*Boolean*]
    + **updated** [*Timestamp*]
    + **evidence** [*String*]
    + **host_type** [*String*]
    + **repository_institution** [*String*]
    + **endpoint_id** [*String*]
    + **id** [*String*]
+ **updated** [*Timestamp*]
+ **oa_status** [*String*]
+ **title** [*String*]
+ **genre** [*String*]
+ **year** [*Integer*]
+ **is_oa** [*Boolean*]
+ **publisher** [*String*]
+ **doi_url** [*String*]
+ **z_authors** [*Record*]
    + **sequence** [*String*]
    + **authenticated_orcid** [*Boolean*]
    + **suffix** [*String*]
    + **affiliation** [*Record*]
        + **name** [*String*]
    + **name** [*String*]
    + **ORCID** [*String*]
    + **family** [*String*]
    + **given** [*String*]
+ **journal_is_oa** [*Boolean*]
+ **has_repository_copy** [*Boolean*]
+ **x_error** [*Boolean*]
+ **journal_name** [*String*]
+ **is_paratext** [*Boolean*]

